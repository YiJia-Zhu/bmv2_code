#!/usr/bin/env python3
"""Download the existing SWH origin list as resumable local Git mirrors.

No filename, P4 dialect, or quality filtering is repeated here. All records
remain auditable, including unavailable repositories and non-Git sources.
"""

import argparse
import collections
import concurrent.futures
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
from pathlib import Path

from swh_history_api import API
from swh_history_common import State, atomic_json, duration, git_source, utc_now


class Paused(Exception):
    pass


class GitError(Exception):
    pass


def tree_bytes(path):
    total = 0
    for parent, directories, files in os.walk(str(path)):
        for name in files:
            try:
                total += os.stat(os.path.join(parent, name), follow_symlinks=False).st_size
            except FileNotFoundError:
                pass
    return total


def tail(path, size=8192):
    if not path.exists():
        return ""
    with path.open("rb") as stream:
        stream.seek(max(0, path.stat().st_size - size))
        return stream.read().decode("utf-8", "replace")


class Collector:
    def __init__(self, args):
        self.args = args
        self.root = args.output.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.process_lock = (self.root / "collector.lock").open("a")
        fcntl.flock(self.process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.process_lock.write("{} {}\n".format(os.getpid(), utc_now()))
        self.process_lock.flush()
        self.state = State(self.root)
        self.state.import_input(args.input, args.shard_index, args.shard_count)
        self.state.recover()
        self.stop = threading.Event()
        self.pause_reason = None
        self.start = time.time()
        previous_start = self.state.rows("SELECT value FROM metadata WHERE key='first_started_epoch'")
        if previous_start:
            self.first_start = float(previous_start[0]["value"])
        else:
            self.first_start = self.start
            previous_config = self.root / "run_config.json"
            if previous_config.exists():
                import datetime
                previous_value = json.loads(previous_config.read_text()).get("first_started_at") or json.loads(previous_config.read_text()).get("started_at")
                if previous_value:
                    self.first_start = datetime.datetime.fromisoformat(previous_value).timestamp()
            self.state.execute("INSERT INTO metadata VALUES ('first_started_epoch', ?)", (str(self.first_start),))
        self.api = API(self.state, args.proxy, args.env_file)
        # Tracker discovery does not require a successful Git clone first.
        with self.state.batch():
            for repo in self.state.rows("SELECT * FROM repos"):
                self.api.seed(repo)
        self.host_limits = collections.defaultdict(lambda: 2)
        self.host_limits["github.com"] = args.github_start
        self.host_limits["gitlab.com"] = 4
        self.successes = collections.Counter()
        self.transient_errors = collections.Counter()
        self.host_pause = {}
        self.active = {}
        self.active_lock = threading.Lock()
        self.git_env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GIT_ASKPASS="/bin/false",
                            GCM_INTERACTIVE="never", GIT_LFS_SKIP_SMUDGE="1", LC_ALL="C")
        helper_path = getattr(args, "git_exec_path", None) or self.root / "toolchain/git-core"
        if (helper_path / "git-remote-http").exists():
            self.git_env["GIT_EXEC_PATH"] = str(helper_path.resolve())
        self.git_base = ["git", "-c", "credential.helper=", "-c", "core.askPass=",
                         "-c", "pack.threads=2", "-c", "http.version=HTTP/1.1",
                         "-c", "http.lowSpeedLimit=1024",
                         "-c", "http.lowSpeedTime=300"]
        if args.proxy:
            self.git_base += ["-c", "http.proxy=" + args.proxy]
        self.state.event("run_started", pid=os.getpid(), options={
            key: str(value) if isinstance(value, Path) else value
            for key, value in vars(args).items()})
        atomic_json(self.root / "run_config.json", {
            "started_at": utc_now(), "first_started_at": datetime_from_epoch(self.first_start), "pid": os.getpid(),
            "input": str(args.input.resolve()), "proxy": args.proxy,
            "global_git_max": args.git_max, "github_start": args.github_start,
            "github_max": args.github_max, "min_free_gib": args.min_free_gib,
            "shard_index": args.shard_index, "shard_count": args.shard_count,
            "github_authenticated": bool(self.api.token("GITHUB_TOKEN", "GH_TOKEN")),
            "git_exec_path": self.git_env.get("GIT_EXEC_PATH"),
            "completion_scope": "mirror, index, LFS, submodule dependencies, API endpoints tracked independently",
        })

    def check_space(self):
        free = shutil.disk_usage(str(self.root)).free
        if free < self.args.min_free_gib * 1024 ** 3:
            self.pause_reason = "disk_low"
            self.stop.set()
        if self.stop.is_set():
            raise Paused(self.pause_reason or "user_stop")

    def command(self, command, log, output=None, activity_timeout=1800):
        self.check_space()
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open("ab", buffering=0) as errors:
            errors.write(("\n[{}] {}\n".format(utc_now(), json.dumps(command))).encode())
            destination = output.open("wb") if output else errors
            try:
                with subprocess.Popen(command, stdout=destination, stderr=errors,
                                      env=self.git_env, start_new_session=True) as process:
                    last_activity = time.monotonic()
                    previous = None
                    try:
                        while process.poll() is None:
                            self.check_space()
                            measure = (log.stat().st_size, output.stat().st_size if output else 0)
                            if measure != previous:
                                last_activity, previous = time.monotonic(), measure
                            elif time.monotonic() - last_activity > activity_timeout:
                                raise GitError("command made no progress for {} seconds".format(activity_timeout))
                            self.stop.wait(0.25)
                    except BaseException:
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                            process.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                        except ProcessLookupError:
                            pass
                        raise
                    if process.returncode:
                        raise GitError("command exit {}: {}".format(process.returncode, tail(log)[-1600:]))
            finally:
                if output:
                    destination.close()

    def git(self, repository, arguments, log, output=None):
        self.command(self.git_base + ["-C", str(repository)] + arguments, log, output)

    def mirror(self, repo):
        repo_id = repo["repo_id"]
        destination = self.root / "repos" / repo_id
        destination.mkdir(parents=True, exist_ok=True)
        repository = destination / "mirror.git"
        log = destination / "fetch.log"
        started = time.time()
        with self.active_lock:
            self.active[repo_id] = {"url": repo["canonical_url"], "host": repo["host"],
                                    "started_at": utc_now(), "phase": "refs", "log": str(log)}
        self.state.event("repo_started", repo_id=repo_id, url=repo["canonical_url"], attempt=repo["attempts"])
        try:
            # Store HEAD and remote refs as direct evidence, including redirect warnings.
            remote_refs = destination / "remote_refs.txt"
            self.command(self.git_base + ["ls-remote", "--symref", repo["clone_url"]], log, remote_refs)
            if not repository.exists():
                self.command(self.git_base + ["init", "--bare", str(repository)], log)
            elif not (repository / "config").exists():
                raise GitError("existing mirror directory is not an initialized repository")
            for key, value in (("remote.origin.url", repo["clone_url"]),
                               ("remote.origin.fetch", "+refs/*:refs/*"),
                               ("remote.origin.mirror", "true")):
                self.git(repository, ["config", key, value], log)
            self.phase(repo_id, "fetch")
            self.git(repository, ["fetch", "--force", "--progress", "origin", "+refs/*:refs/*"], log)
            symbolic = None
            for line in remote_refs.read_text().splitlines():
                if line.startswith("ref: ") and line.endswith("\tHEAD"):
                    symbolic = line[5:].split("\t")[0]
            if symbolic:
                self.git(repository, ["symbolic-ref", "HEAD", symbolic], log)
            else:
                head = [line.split("\t")[0] for line in remote_refs.read_text().splitlines()
                        if line.endswith("\tHEAD") and not line.startswith("ref:")]
                if head:
                    self.git(repository, ["update-ref", "HEAD", head[0]], log)
            self.phase(repo_id, "verify")
            self.git(repository, ["fsck", "--connectivity-only", "--no-reflogs"], log,
                     destination / "fsck.txt")
            self.git(repository, ["for-each-ref", "--format=%(refname)\t%(objectname)\t%(objecttype)"], log,
                     destination / "refs.tsv")
            self.git(repository, ["rev-list", "--all", "--count"], log, destination / "commit_count.txt")
            self.phase(repo_id, "commit_index")
            exporter = Path(__file__).with_name("export_git_commit_index.py")
            self.command([sys.executable, str(exporter), str(repository),
                          str(destination / "commits.jsonl")], log)
            commits = int((destination / "commit_count.txt").read_text().strip())
            with (destination / "commits.jsonl").open("rb") as stream:
                index_count = sum(1 for _ in stream)
            if commits != index_count:
                raise GitError("commit count mismatch {} != {}".format(commits, index_count))
            ref_count = len((destination / "refs.tsv").read_text().splitlines())
            byte_count = tree_bytes(repository)
            redirect = re.findall(r"warning: redirecting to (https?://[^\s]+)", tail(log, 65536))
            resolved_url = redirect[-1] if redirect else repo["clone_url"]
            atomic_json(destination / "mirror_complete.json", {
                "repo_id": repo_id, "origin_url": repo["canonical_url"],
                "transport_url": repo["clone_url"], "redirect_observed": redirect,
                "refs": ref_count, "reachable_commits": commits, "index_rows": index_count,
                "mirror_bytes": byte_count, "finished_at": utc_now(),
                "scope": "all fetched visible refs and their reachable Git objects; external assets separate",
                "fsck_connectivity_only": "passed", "source_content_rescreened": False,
            })
            self.state.execute("""UPDATE repos SET status='complete',finished_at=?,elapsed_s=?,
                bytes=?,commits=?,error=NULL,resolved_url=? WHERE repo_id=?""",
                               (utc_now(), time.time() - started, byte_count, commits, resolved_url, repo_id))
            # Auxiliary work never turns an already verified mirror into a failed clone.
            self.api.seed(repo)
            try:
                self.auxiliary(repo, repository, destination, log)
            except Paused:
                raise
            except Exception as exc:
                self.state.execute("UPDATE repos SET lfs_status=CASE WHEN lfs_status='pending' THEN 'error' ELSE lfs_status END, error=? WHERE repo_id=?",
                                   ("auxiliary: " + str(exc)[-800:], repo_id))
                self.state.event("auxiliary_error", repo_id=repo_id, reason=str(exc)[-800:])
            self.state.event("repo_complete", repo_id=repo_id, url=repo["canonical_url"],
                             elapsed_s=round(time.time() - started, 2), bytes=byte_count, commits=commits)
            return "success"
        except Paused as exc:
            self.state.execute("UPDATE repos SET status=CASE WHEN status='complete' THEN status ELSE 'retry_wait' END,attempts=MAX(0,attempts-1),next_at=0,error=? WHERE repo_id=?",
                               (str(exc), repo_id))
            return "paused"
        except Exception as exc:
            message = str(exc)[-1800:]
            inaccessible = any(text in message.lower() for text in (
                "repository not found", "could not read username", "authentication failed",
                "requested url returned error: 404", "repository does not exist"))
            rate_limited = any(text in message.lower() for text in ("429", "too many requests", "rate limit", "error: 403"))
            status = "inaccessible" if inaccessible else "failed" if repo["attempts"] >= self.args.retries else "retry_wait"
            delay = max(600 if rate_limited else 30, 30 * 2 ** min(repo["attempts"], 6))
            self.state.execute("UPDATE repos SET status=?,next_at=?,error=?,elapsed_s=? WHERE repo_id=?",
                               (status, time.time() + delay, message, time.time() - started, repo_id))
            self.state.event("repo_error", repo_id=repo_id, url=repo["canonical_url"],
                             status=status, error=message, retry_delay_s=delay)
            return "rate_limited" if rate_limited else "inaccessible" if inaccessible else "transient"
        finally:
            with self.active_lock:
                self.active.pop(repo_id, None)

    def phase(self, repo_id, name):
        with self.active_lock:
            if repo_id in self.active:
                self.active[repo_id]["phase"] = name

    def auxiliary(self, repo, repository, destination, log):
        self.phase(repo["repo_id"], "external_assets")
        # Preserve every distinct root .gitmodules blob without copying every tree.
        self.git(repository, ["rev-list", "--objects", "--all", "--", ".gitmodules"], log,
                 destination / "gitmodule_objects.txt")
        module_oids = sorted({line.split(" ")[0] for line in (destination / "gitmodule_objects.txt").read_text().splitlines()
                              if line.endswith(" .gitmodules")})
        modules = []
        for oid in module_oids:
            output = destination / ("gitmodules-" + oid + ".txt")
            self.git(repository, ["cat-file", "blob", oid], log, output)
            modules.append({"blob_oid": oid, "file": output.name})
        atomic_json(destination / "submodules.json", {
            "gitmodules_versions": modules,
            "status": "dependency_fetch_pending" if modules else "none_detected",
            "note": "Git links remain in mirror; external submodule repository histories require separate tasks.",
        })
        self.state.execute("UPDATE repos SET submodule_status=? WHERE repo_id=?",
                           ("dependency_fetch_pending" if modules else "none_detected", repo["repo_id"]))
        if not shutil.which("git-lfs"):
            self.state.execute("UPDATE repos SET lfs_status='tool_missing' WHERE repo_id=?", (repo["repo_id"],))
            return
        self.git(repository, ["lfs", "ls-files", "--all", "--long"], log, destination / "lfs_files.txt")
        if (destination / "lfs_files.txt").stat().st_size:
            self.git(repository, ["lfs", "fetch", "--all", "origin"], log)
            lfs_status = "fetched_all_referenced"
        else:
            lfs_status = "none_detected"
        self.state.execute("UPDATE repos SET lfs_status=?,bytes=? WHERE repo_id=?",
                           (lfs_status, tree_bytes(repository), repo["repo_id"]))

    def report(self, final=False):
        now = time.time()
        statuses = {row["status"]: row["n"] for row in self.state.rows("SELECT status,COUNT(*) n FROM repos GROUP BY status")}
        api_status = {row["status"]: row["n"] for row in self.state.rows("SELECT status,COUNT(*) n FROM api_tasks GROUP BY status")}
        totals = self.state.rows("SELECT COUNT(*) total,COALESCE(SUM(bytes),0) bytes,COALESCE(SUM(commits),0) commits FROM repos")[0]
        done = statuses.get("complete", 0)
        inaccessible = statuses.get("inaccessible", 0)
        failed = statuses.get("failed", 0)
        terminal = done + inaccessible + failed
        process_elapsed = now - self.start
        elapsed = now - self.first_start
        baseline = getattr(self, "baseline_terminal", terminal)
        self.baseline_terminal = baseline
        rate = (terminal - baseline) / process_elapsed if process_elapsed else 0
        remaining = totals["total"] - terminal
        github = self.state.rows("""SELECT COUNT(*) total,
            SUM(CASE WHEN status IN ('complete','inaccessible','failed') THEN 1 ELSE 0 END) terminal,
            SUM(CASE WHEN status='complete' THEN 1 ELSE 0 END) verified
            FROM repos WHERE host='github.com'""")[0]
        # GitHub dominates the queue. Use its observed rate, including restarts,
        # instead of letting the first few small non-GitHub sites skew the ETA.
        gh_rate = (github["terminal"] or 0) / elapsed if elapsed else 0
        eta = ((github["total"] - (github["terminal"] or 0)) / gh_rate
               if (github["terminal"] or 0) >= 30 and elapsed >= 600 and gh_rate else None)
        with self.active_lock:
            active = {key: dict(value) for key, value in self.active.items()}
        active_bytes = 0
        for item in active.values():
            progress = tail(Path(item["log"]), 2048).replace("\r", "\n").splitlines()
            item["last_progress"] = progress[-1][-400:] if progress else None
        for repo_id, item in active.items():
            item["current_object_bytes"] = tree_bytes(self.root / "repos" / repo_id / "mirror.git" / "objects")
            if item["phase"] in ("refs", "fetch", "verify", "commit_index"):
                stored = self.state.rows("SELECT COALESCE(bytes,0) bytes FROM repos WHERE repo_id=?", (repo_id,))[0]["bytes"]
                active_bytes += max(0, item["current_object_bytes"] - stored)
        archive_bytes = self.state.rows("SELECT COALESCE(SUM(bytes),0) bytes FROM assets WHERE status='complete'")[0]["bytes"]
        current_bytes = totals["bytes"] + active_bytes + archive_bytes
        previous_bytes, previous_time = getattr(self, "previous_storage", (current_bytes, now))
        growth_rate = max(0, current_bytes - previous_bytes) / max(0.001, now - previous_time)
        if process_elapsed < 15:
            growth_rate = 0
        self.previous_storage = (current_bytes, now)
        hosts = self.state.rows("SELECT host,status,COUNT(*) n FROM repos GROUP BY host,status")
        record = {
            "updated_at": utc_now(), "run_started_at": datetime_from_epoch(self.start),
            "first_started_at": datetime_from_epoch(self.first_start),
            "process_elapsed_s": round(process_elapsed, 1),
            "elapsed_s": round(elapsed, 1), "git_repositories": totals["total"],
            "git_status": statuses, "mirror_complete": done, "git_terminal": terminal,
            "git_progress_percent": round(100 * terminal / totals["total"], 2) if totals["total"] else 100,
            "progress_note": "terminal includes explicitly inaccessible/failed; not successful full collection",
            "mirror_bytes": totals["bytes"], "reachable_commits": totals["commits"],
            "active_object_bytes": active_bytes, "completed_archive_bytes": archive_bytes,
            "local_payload_growth_mib_s": round(growth_rate / 1024 ** 2, 3),
            "throughput_note": "local object/archive growth, not measured wire traffic; per-repo fetch.log contains Git transfer rates",
            "api_status": api_status, "api_eta_s": None,
            "api_eta_note": "total pages expand during discovery; quota waits separate from Git ETA",
            "git_eta_s": round(eta) if eta is not None else None,
            "git_eta_note": "rough GitHub observed processed rate since first start; excludes tracker and long tail; may change greatly",
            "git_repos_per_min": round(rate * 60, 2),
            "github_total": github["total"], "github_terminal": github["terminal"] or 0,
            "github_verified": github["verified"] or 0,
            "github_repos_per_min": round(gh_rate * 60, 2),
            "free_gib": round(shutil.disk_usage(str(self.root)).free / 1024 ** 3, 2),
            "active_repositories": active, "host_limits": dict(self.host_limits),
            "hosts": hosts, "pause_reason": self.pause_reason, "final": final,
            "lfs_status": self.state.rows("SELECT lfs_status status,COUNT(*) n FROM repos GROUP BY lfs_status"),
            "submodule_status": self.state.rows("SELECT submodule_status status,COUNT(*) n FROM repos GROUP BY submodule_status"),
            "non_git_status": self.state.rows("SELECT status,COUNT(*) n FROM packages GROUP BY status"),
            "asset_status": self.state.rows("SELECT status,COUNT(*) n FROM assets GROUP BY status"),
        }
        with self.api.lock:
            record["api_quota_resume_at"] = dict(self.api.next_at)
        atomic_json(self.root / "status.json", record)
        text = ("[{at}] Git {terminal}/{total} ({pct:.2f}%) verified={done} inaccessible={unavailable} failed={failed} "
                "active={active} mirror={gib:.3f}GiB disk_growth={speed:.2f}MiB/s commits={commits} elapsed={elapsed} Git_ETA~{eta} "
                "API={api} free={free}GiB limits={limits}").format(
                    at=record["updated_at"], terminal=terminal, total=totals["total"], pct=record["git_progress_percent"],
                    done=done, unavailable=inaccessible, failed=failed, active=len(active),
                    gib=totals["bytes"] / 1024 ** 3, commits=totals["commits"],
                    speed=growth_rate / 1024 ** 2,
                    elapsed=duration(elapsed), eta=duration(eta), api=api_status,
                    free=record["free_gib"], limits=dict(self.host_limits))
        print(text, flush=True)
        self.state.event("progress", **{key: value for key, value in record.items() if key not in ("hosts", "active_repositories")})

    def run(self):
        from swh_history_packages import Packages
        packages = Packages(self)
        # Four independent lanes: Git, API, metadata resolvers, archive assets.
        pools = {"git": concurrent.futures.ThreadPoolExecutor(self.args.git_max),
                 "api": concurrent.futures.ThreadPoolExecutor(8),
                 "package": concurrent.futures.ThreadPoolExecutor(2),
                 "asset": concurrent.futures.ThreadPoolExecutor(4)}
        jobs = {}
        last_report, last_adjust = 0, time.time()
        try:
            self.report()
            while not self.stop.is_set():
                self.check_space()
                if time.time() - getattr(self, "last_credentials_check", 0) > 15:
                    self.api.refresh_credentials()
                    self.last_credentials_check = time.time()
                for future in list(jobs):
                    if not future.done():
                        continue
                    lane, row = jobs.pop(future)
                    try:
                        result = future.result()
                    except Exception as exc:
                        self.state.event("worker_unhandled_error", lane=lane, reason=str(exc)[-1200:])
                        result = "transient"
                    if lane == "git":
                        if result == "success":
                            self.successes[row["host"]] += 1
                        elif result in ("transient", "rate_limited"):
                            self.transient_errors[row["host"]] += 1
                            if result == "rate_limited":
                                self.host_pause[row["host"]] = time.time() + 600
                counts = collections.Counter(lane for lane, row in jobs.values())
                host_active = collections.Counter(row["host"] for lane, row in jobs.values() if lane == "git")
                if counts["git"] < self.args.git_max:
                    candidates = self.state.rows("""SELECT * FROM (
                        SELECT *,ROW_NUMBER() OVER (PARTITION BY host ORDER BY
                            CASE WHEN status='retry_wait' THEN 0 ELSE 1 END,next_at,attempts,repo_id) priority
                        FROM repos WHERE status IN ('pending','retry_wait') AND next_at<=?)
                        WHERE priority<=64 ORDER BY priority,host LIMIT 1500""", (time.time(),))
                    # Round-robin hosts, so GitHub does not crowd out smaller sites.
                    by_host = collections.defaultdict(collections.deque)
                    for row in candidates:
                        by_host[row["host"]].append(row)
                    while counts["git"] < self.args.git_max and by_host:
                        advanced = False
                        for host in list(by_host):
                            if (host_active[host] >= self.host_limits[host] or
                                    self.host_pause.get(host, 0) > time.time()):
                                continue
                            row = by_host[host].popleft()
                            if not by_host[host]:
                                del by_host[host]
                            row["attempts"] += 1
                            self.state.execute("UPDATE repos SET status='running',attempts=?,started_at=? WHERE repo_id=?",
                                               (row["attempts"], utc_now(), row["repo_id"]))
                            jobs[pools["git"].submit(self.mirror, row)] = ("git", row)
                            host_active[host] += 1
                            counts["git"] += 1
                            advanced = True
                            if counts["git"] >= self.args.git_max:
                                break
                        if not advanced:
                            break
                api_active = {urllib.parse.urlsplit(row["url"]).netloc for lane, row in jobs.values() if lane == "api"}
                if counts["api"] < 8:
                    for row in self.state.rows("""SELECT * FROM (
                        SELECT *,ROW_NUMBER() OVER (PARTITION BY host ORDER BY attempts,task_id) priority
                        FROM api_tasks WHERE status IN ('pending','retry_wait') AND next_at<=?)
                        WHERE priority<=4 ORDER BY priority,host""", (time.time(),)):
                        api_host = urllib.parse.urlsplit(row["url"]).netloc
                        if api_host in api_active or self.api.next_at.get(api_host, 0) > time.time():
                            continue
                        row["attempts"] += 1
                        self.state.execute("UPDATE api_tasks SET status='running',attempts=? WHERE task_id=?", (row["attempts"], row["task_id"]))
                        jobs[pools["api"].submit(self.api.run_task, row)] = ("api", row)
                        api_active.add(api_host)
                        counts["api"] += 1
                        if counts["api"] >= 8:
                            break
                for lane, table, key, limit, method in (("package", "packages", "origin_id", 2, packages.resolve),
                                                        ("asset", "assets", "asset_id", 4, packages.download)):
                    if counts[lane] >= limit:
                        continue
                    for row in self.state.rows("SELECT * FROM {} WHERE status IN ('pending','retry_wait') AND next_at<=? ORDER BY attempts LIMIT ?".format(table), (time.time(), limit - counts[lane])):
                        row["attempts"] += 1
                        self.state.execute("UPDATE {} SET status='running',attempts=? WHERE {}=?".format(table, key), (row["attempts"], row[key]))
                        jobs[pools[lane].submit(method, row)] = (lane, row)
                if time.time() - last_adjust >= 60:
                    for host in list(self.host_limits):
                        previous = self.host_limits[host]
                        errors = self.transient_errors[host]
                        successes = self.successes[host]
                        if errors >= 3 or (errors and not successes):
                            self.host_limits[host] = max(1, previous // 2)
                        elif (successes >= max(1, previous // 4) or
                              (host_active[host] >= previous and not self.host_pause.get(host, 0) > time.time())):
                            maximum = self.args.github_max if host == "github.com" else 8
                            self.host_limits[host] = min(maximum, previous + 4)
                        if previous != self.host_limits[host]:
                            self.state.event("concurrency_adjusted", host=host, previous=previous,
                                             current=self.host_limits[host], successes=self.successes[host],
                                             transient_errors=self.transient_errors[host])
                    self.successes.clear()
                    self.transient_errors.clear()
                    last_adjust = time.time()
                if time.time() - last_report >= self.args.report_seconds:
                    self.report()
                    last_report = time.time()
                pending = sum(self.state.rows("SELECT COUNT(*) n FROM {} WHERE status IN ('pending','retry_wait','running')".format(table))[0]["n"]
                              for table in ("repos", "api_tasks", "packages", "assets"))
                if not pending and not jobs:
                    self.pause_reason = "queues_processed_review_gaps"
                    break
                self.stop.wait(1)
        except Paused:
            pass
        finally:
            self.stop.set()
            for pool in pools.values():
                pool.shutdown(wait=True)
            self.report(final=True)
            self.state.event("run_stopped", reason=self.pause_reason or "user_stop")


def datetime_from_epoch(value):
    import datetime
    return datetime.datetime.fromtimestamp(value, datetime.timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path("data/raw/swh_p4/download_lists/swh_20260604_origin_download_list_v1.csv"))
    parser.add_argument("--output", type=Path, default=Path("data/raw/repositories/swh_full_history"))
    parser.add_argument("--proxy", default="http://127.0.0.1:7892", help="empty string for direct connections")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--git-exec-path", type=Path, help="optional local OpenSSL Git HTTP helper directory")
    parser.add_argument("--git-max", type=int, default=48)
    parser.add_argument("--github-start", type=int, default=8)
    parser.add_argument("--github-max", type=int, default=32)
    parser.add_argument("--min-free-gib", type=float, default=100)
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--report-seconds", type=int, default=30)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()
    if not 0 <= args.shard_index < args.shard_count:
        parser.error("require 0 <= shard-index < shard-count")
    if not 1 <= args.github_start <= args.github_max <= args.git_max:
        parser.error("require 1 <= github-start <= github-max <= git-max")
    collector = Collector(args)
    def stop_handler(number, frame):
        collector.pause_reason = "signal_{}".format(number)
        collector.stop.set()
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    collector.run()


if __name__ == "__main__":
    main()
