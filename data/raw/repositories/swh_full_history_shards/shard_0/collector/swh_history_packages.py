#!/usr/bin/env python3
"""Enumerate public source-release archives; preserve unresolved origins."""

import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from swh_history_api import RetryRequest
from swh_history_common import atomic_json, git_source, stable_id, utc_now


class Packages:
    def __init__(self, collector):
        self.collector = collector
        self.state = collector.state
        self.api = collector.api

    def asset(self, origin_id, url, filename=None, checksum=None):
        self.collector.check_space()
        if urllib.parse.urlsplit(url).scheme not in ("https", "http"):
            return
        asset_id = stable_id(origin_id + "\n" + url)
        name = filename or Path(urllib.parse.urlsplit(url).path).name or "archive"
        name = re.sub(r"[^A-Za-z0-9._-]", "_", name)[-180:]
        self.state.execute("INSERT OR IGNORE INTO assets(asset_id,origin_id,url,filename) VALUES (?,?,?,?)",
                           (asset_id, origin_id, url, name))
        atomic_json(self.state.root / "packages" / origin_id / "asset_metadata" / (asset_id + ".json"),
                    {"url": url, "filename": name, "provider_checksum": checksum})

    def metadata(self, origin_id, url):
        self.collector.check_space()
        target = self.state.root / "packages" / origin_id / "metadata" / (stable_id(url) + ".json")
        if target.exists():
            return json.loads(target.read_text())["response"]
        data, meta = self.api.request(url)
        atomic_json(target, {"request_url": url, "metadata": meta, "response": data})
        return data

    def raw_metadata(self, origin_id, url):
        self.collector.check_space()
        target = self.state.root / "packages" / origin_id / "metadata" / (stable_id(url) + ".data")
        if target.exists():
            return target.read_text()
        host = urllib.parse.urlsplit(url).netloc
        with self.api.lock:
            delay = self.api.next_at.get(host, 0) - time.time()
            if delay > 0:
                raise RetryRequest("host_quota_wait", delay)
            self.api.next_at[host] = time.time() + 2
        with self.api.opener.open(urllib.request.Request(url, headers={"User-Agent": "p4-research-history-collector/1.0"}), timeout=60) as response:
            text = response.read().decode("utf-8", "replace")
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        temporary.write_text(text)
        os.replace(str(temporary), str(target))
        return text

    def go_versions(self, origin_id, module):
        parts = module.split("/")
        while len(parts) >= 3:
            name = "/".join(parts)
            escaped = "".join("!" + char.lower() if char.isupper() else char for char in name)
            base = "https://proxy.golang.org/" + escaped
            try:
                versions = set(self.raw_metadata(origin_id, base + "/@v/list").splitlines())
                try:
                    latest = self.metadata(origin_id, base + "/@latest")
                    versions.add(latest["Version"])
                except urllib.error.HTTPError as exc:
                    if exc.code not in (404, 410):
                        raise
                for version in sorted(versions):
                    for extension in ("zip", "mod", "info"):
                        self.asset(origin_id, base + "/@v/" + version + "." + extension,
                                   version + "." + extension)
                return "go_version_index_saved" if versions else "go_empty_version_index"
            except urllib.error.HTTPError as exc:
                if exc.code not in (404, 410):
                    raise
                parts.pop()
        return "package_version_resolution_pending"

    def resolve(self, task):
        origin_id = task["origin_id"]
        row = self.state.rows("SELECT * FROM origins WHERE origin_id=?", (origin_id,))[0]
        url = row["origin_url"]
        parsed = urllib.parse.urlsplit(url)
        status = "version_index_saved"
        try:
            if parsed.hostname == "cache.nixos.org":
                self.asset(origin_id, url)
            elif parsed.hostname == "pypi.org":
                package = parsed.path.strip("/").split("/")[1]
                data = self.metadata(origin_id, "https://pypi.org/pypi/{}/json".format(package))
                for version, artifacts in data["releases"].items():
                    for artifact in artifacts:
                        if artifact["packagetype"] == "sdist":
                            self.asset(origin_id, artifact["url"], artifact["filename"],
                                       "sha256:" + artifact["digests"]["sha256"])
            elif parsed.hostname == "www.npmjs.com":
                package = parsed.path.split("/package/", 1)[1]
                data = self.metadata(origin_id, "https://registry.npmjs.org/" + urllib.parse.quote(package, safe=""))
                for version in data.get("versions", {}).values():
                    if version.get("dist", {}).get("tarball"):
                        self.asset(origin_id, version["dist"]["tarball"], checksum="sha1:" + version["dist"]["shasum"])
            elif parsed.hostname == "doi.org" and "/zenodo." in parsed.path:
                record_id = parsed.path.rsplit(".", 1)[1]
                record = self.metadata(origin_id, "https://zenodo.org/api/records/" + record_id)
                next_url = record.get("links", {}).get("versions")
                records = [record]
                while next_url:
                    data = self.metadata(origin_id, next_url)
                    records.extend(data.get("hits", {}).get("hits", []))
                    next_url = data.get("links", {}).get("next")
                for record in {item["id"]: item for item in records}.values():
                    for artifact in record.get("files", []):
                        download = artifact.get("links", {}).get("self") or artifact.get("links", {}).get("content")
                        if download:
                            self.asset(origin_id, download, "{}-{}".format(record["id"], artifact["key"]), artifact.get("checksum"))
            elif parsed.hostname == "pkg.go.dev":
                # The package page must explicitly link its suggested upstream.
                # Keep the original package origin separate from this upstream hint.
                module = parsed.path.strip("/").split("@")[0]
                upstream = None
                parts = module.split("/")
                if parts[0] == "github.com" and len(parts) >= 3:
                    upstream = git_source("https://" + "/".join(parts[:3]))
                status = "package_version_resolution_pending"
                if upstream:
                    try:
                        page = self.raw_metadata(origin_id, url)
                    except urllib.error.HTTPError as exc:
                        if exc.code not in (401, 403, 404, 410):
                            raise
                        page = ""
                    target = self.state.root / "packages" / origin_id
                    target.mkdir(parents=True, exist_ok=True)
                    (target / "package_page.html").write_text(page)
                    if upstream["canonical_url"].lower() in page.lower():
                        owner_shard = int(upstream["repo_id"], 16) % self.collector.args.shard_count
                        if owner_shard == self.collector.args.shard_index:
                            self.state.execute("""INSERT OR IGNORE INTO repos
                                (repo_id,canonical_url,clone_url,host,forge,repo_path) VALUES (?,?,?,?,?,?)""",
                                               tuple(upstream[key] for key in ("repo_id", "canonical_url", "clone_url", "host", "forge", "repo_path")))
                        atomic_json(target / "upstream_evidence.json", {
                            "origin_url": url, "upstream_repository": upstream,
                            "basis": "repository URL appears in fetched package page",
                            "identity": "upstream link only; package origin is not merged into Git identity",
                            "package_versions_status": "pending", "observed_at": utc_now(),
                            "upstream_owner_shard": owner_shard,
                        })
                        status = "upstream_queued_package_versions_pending"
                status = self.go_versions(origin_id, module)
            elif parsed.scheme == "deb" and parsed.netloc.lower() == "debian":
                package = parsed.path.rsplit("/", 1)[1]
                data = self.metadata(origin_id, "https://snapshot.debian.org/mr/package/" + package + "/")
                for version in data.get("result", []):
                    version_url = "https://snapshot.debian.org/mr/package/{}/{}/srcfiles".format(package, urllib.parse.quote(version["version"], safe=""))
                    files = self.metadata(origin_id, version_url)
                    for artifact in files.get("result", []):
                        digest = artifact["hash"]
                        self.asset(origin_id, "https://snapshot.debian.org/file/" + digest, digest,
                                   checksum="sha1:" + digest)
            elif parsed.scheme == "deb" and parsed.netloc.lower() == "ubuntu":
                package = parsed.path.rsplit("/", 1)[1]
                for publication_status in ("Published", "Superseded", "Deleted", "Obsolete"):
                    next_url = "https://api.launchpad.net/1.0/ubuntu/+archive/primary?" + urllib.parse.urlencode({
                        "ws.op": "getPublishedSources", "source_name": package,
                        "exact_match": "true", "status": publication_status})
                    while next_url:
                        publications = self.metadata(origin_id, next_url)
                        for publication in publications.get("entries", []):
                            files = self.metadata(origin_id, publication["self_link"] + "?ws.op=sourceFileUrls")
                            for archive in files:
                                self.asset(origin_id, archive)
                        next_url = publications.get("next_collection_link")
            elif url.startswith("opam+https://opam.ocaml.org/packages/"):
                page_url = url[len("opam+"):]
                package = page_url.rstrip("/").rsplit("/", 1)[1]
                page = self.raw_metadata(origin_id, page_url)
                versions = sorted(set(re.findall(re.escape(package) + r"\.[0-9][A-Za-z0-9._+~-]*", page)))
                for version in versions:
                    opam = self.raw_metadata(origin_id, "https://opam.ocaml.org/packages/{}/{}/opam".format(package, version))
                    source = re.search(r'url\s*\{.*?src\s*:\s*"([^"]+)"', opam, re.S)
                    checksum = re.search(r'"(sha256|sha1|md5)=([0-9a-f]+)"', opam)
                    if source:
                        self.asset(origin_id, source.group(1), version + "-" + Path(urllib.parse.urlsplit(source.group(1)).path).name,
                                   ":".join(checksum.groups()) if checksum else None)
                if not versions:
                    status = "package_version_resolution_pending"
            else:
                # Unknown providers remain explicit; no false completion flag.
                status = "provider_resolution_pending"
            self.state.execute("UPDATE packages SET status=?,error=NULL WHERE origin_id=?", (status, origin_id))
            self.state.event("package_resolved", origin_id=origin_id, url=url, status=status)
        except RetryRequest as exc:
            self.state.execute("UPDATE packages SET status='retry_wait',attempts=attempts-1,next_at=?,error=? WHERE origin_id=?",
                               (time.time() + exc.delay, str(exc), origin_id))
        except Exception as exc:
            if self.collector.stop.is_set():
                self.state.execute("UPDATE packages SET status='retry_wait',attempts=MAX(0,attempts-1),next_at=0,error='interrupted' WHERE origin_id=?", (origin_id,))
                return
            terminal = isinstance(exc, urllib.error.HTTPError) and exc.code in (401, 403, 404, 410)
            status = "provider_unavailable" if terminal else "failed" if task["attempts"] >= 5 else "retry_wait"
            self.state.execute("UPDATE packages SET status=?,next_at=?,error=? WHERE origin_id=?",
                               (status, time.time() + 30 * 2 ** min(task["attempts"], 6), str(exc)[-800:], origin_id))

    def download(self, task):
        target = self.state.root / "packages" / task["origin_id"] / "archives"
        target.mkdir(parents=True, exist_ok=True)
        destination = target / (task["asset_id"][:16] + "-" + task["filename"])
        temporary = destination.with_name(destination.name + ".part")
        meta_path = destination.with_name(destination.name + ".transfer.json")
        started = time.time()
        try:
            complete_marker = destination.with_name(destination.name + ".complete.json")
            if destination.exists() and complete_marker.exists():
                saved = json.loads(complete_marker.read_text())
                digest = hashlib.sha256()
                with destination.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                        digest.update(chunk)
                if destination.stat().st_size == saved["bytes"] and digest.hexdigest() == saved["sha256"]:
                    self.state.execute("UPDATE assets SET status='complete',bytes=?,sha256=?,error=NULL WHERE asset_id=?",
                                       (saved["bytes"], saved["sha256"], task["asset_id"]))
                    return
            headers = {"User-Agent": "p4-research-history-collector/1.0", "Accept-Encoding": "identity"}
            previous = json.loads(meta_path.read_text()) if meta_path.exists() else {}
            offset = temporary.stat().st_size if temporary.exists() else 0
            validator = previous.get("etag") or previous.get("last_modified")
            if offset and validator:
                headers.update({"Range": "bytes={}-".format(offset), "If-Range": validator})
            else:
                offset = 0
            request = urllib.request.Request(task["url"], headers=headers)
            with self.api.opener.open(request, timeout=90) as response:
                if response.status == 206:
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
                    if not match or int(match.group(1)) != offset:
                        raise ValueError("invalid resume Content-Range")
                    expected_size = int(match.group(3))
                elif response.status == 200:
                    offset = 0
                    expected_size = int(response.headers["Content-Length"]) if response.headers.get("Content-Length") else None
                else:
                    raise ValueError("unexpected archive HTTP {}".format(response.status))
                atomic_json(meta_path, {"url": task["url"], "resolved_url": response.geturl(),
                                        "etag": response.headers.get("ETag"), "last_modified": response.headers.get("Last-Modified"),
                                        "expected_size": expected_size, "updated_at": utc_now()})
                with temporary.open("ab" if offset else "wb") as output:
                    while True:
                        self.collector.check_space()
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                if expected_size is not None and temporary.stat().st_size != expected_size:
                    raise ValueError("archive truncated")
            digests = {name: hashlib.new(name) for name in ("sha256", "sha1", "md5")}
            with temporary.open("rb") as stream:
                for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                    for digest in digests.values():
                        digest.update(chunk)
            source_meta = json.loads((self.state.root / "packages" / task["origin_id"] / "asset_metadata" / (task["asset_id"] + ".json")).read_text())
            checksum = source_meta.get("provider_checksum")
            if checksum:
                algorithm, value = checksum.split(":", 1)
                if algorithm in digests and digests[algorithm].hexdigest() != value:
                    raise ValueError("provider checksum mismatch")
            os.replace(str(temporary), str(destination))
            atomic_json(destination.with_name(destination.name + ".complete.json"), {
                "url": task["url"], "sha256": digests["sha256"].hexdigest(), "bytes": destination.stat().st_size,
                "finished_at": utc_now(), "elapsed_s": round(time.time() - started, 2),
                "provider_checksum": checksum,
            })
            self.state.execute("UPDATE assets SET status='complete',bytes=?,sha256=?,error=NULL WHERE asset_id=?",
                               (destination.stat().st_size, digests["sha256"].hexdigest(), task["asset_id"]))
        except Exception as exc:
            if self.collector.stop.is_set():
                self.state.execute("UPDATE assets SET status='retry_wait',attempts=MAX(0,attempts-1),next_at=0,error='interrupted' WHERE asset_id=?", (task["asset_id"],))
                return
            self.state.execute("UPDATE assets SET status=?,next_at=?,error=? WHERE asset_id=?",
                               ("failed" if task["attempts"] >= 5 else "retry_wait",
                                time.time() + 60 * 2 ** min(task["attempts"], 6), str(exc)[-800:], task["asset_id"]))
