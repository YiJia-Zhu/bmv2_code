#!/usr/bin/env python3
"""Durable, independently rate-limited collaboration API queue."""

import email.utils
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from swh_history_common import atomic_json, stable_id, utc_now


class RetryRequest(Exception):
    def __init__(self, reason, delay=60):
        super().__init__(reason)
        self.delay = delay


class StripAuthRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, newurl):
        redirected = super().redirect_request(request, response, code, message, headers, newurl)
        if redirected and urllib.parse.urlsplit(request.full_url).netloc != urllib.parse.urlsplit(newurl).netloc:
            for key in ("Authorization", "Private-token"):
                redirected.remove_header(key)
        return redirected


class API:
    def __init__(self, state, proxy, env_file, github_requests_per_hour=3000):
        if not 1 <= github_requests_per_hour <= 5000:
            raise ValueError("GitHub API rate must be between 1 and 5000 requests/hour")
        self.state = state
        self.env_file = Path(env_file)
        self.github_requests_per_hour = github_requests_per_hour
        handlers = [StripAuthRedirect(), urllib.request.ProxyHandler(
            {"http": proxy, "https": proxy} if proxy else {})]
        self.opener = urllib.request.build_opener(*handlers)
        self.lock = threading.Lock()
        self.next_at = {}
        self.github_token_fingerprint = None

    def refresh_credentials(self):
        token = self.github_token()
        fingerprint = stable_id(token) if token else "anonymous"
        with self.lock:
            previous = self.github_token_fingerprint
            self.github_token_fingerprint = fingerprint
            if previous and previous != fingerprint:
                self.next_at.pop("api.github.com", None)
                self.state.execute("""UPDATE api_tasks SET next_at=0 WHERE host='github.com'
                    AND status='retry_wait' AND (error='host_quota_wait' OR error LIKE '%rate_limited%')""")
                self.state.execute("""UPDATE api_tasks SET status='pending',next_at=0,attempts=0
                    WHERE host='github.com' AND status IN ('unavailable_http_401','unavailable_http_403')""")
                self.state.event("github_credentials_changed", authenticated=bool(token))

    def github_token(self):
        return self.token("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_API_TOKEN")

    def token(self, *keys):
        # Reload selected keys only: adding a token does not require restarting.
        values = {}
        if self.env_file.exists():
            for line in self.env_file.read_text().splitlines():
                key, separator, value = line.strip().partition("=")
                if separator and key in keys:
                    values[key] = value.strip().strip("\"'")
        return next((os.environ.get(key) or values.get(key) for key in keys
                     if os.environ.get(key) or values.get(key)), None)

    def request(self, url):
        parsed = urllib.parse.urlsplit(url)
        headers = {"User-Agent": "p4-research-history-collector/1.0",
                   "Accept": "application/json"}
        token = None
        if parsed.hostname == "api.github.com":
            token = self.github_token()
            headers["Accept"] = "application/vnd.github+json"
            headers["X-GitHub-Api-Version"] = "2022-11-28"
            if token:
                headers["Authorization"] = "Bearer " + token
        elif parsed.hostname == "gitlab.com":
            token = self.token("GITLAB_TOKEN")
            if token:
                headers["PRIVATE-TOKEN"] = token
        host = parsed.netloc
        with self.lock:
            delay = self.next_at.get(host, 0) - time.time()
            if delay > 0:
                raise RetryRequest("host_quota_wait", delay)
            interval = 1.2 if token else 2.0
            if parsed.hostname == "api.github.com" and token:
                interval = 3600.0 / self.github_requests_per_hour
            self.next_at[host] = time.time() + interval
        try:
            with self.opener.open(urllib.request.Request(url, headers=headers), timeout=60) as response:
                raw = response.read()
                content_type = response.headers.get("Content-Type", "")
                if "json" not in content_type.lower():
                    raise ValueError("API returned HTTP {} content type {} (expected JSON)".format(
                        response.status, content_type[:120]))
                meta = {key: value for key, value in response.headers.items()
                        if key.lower() in ("link", "etag", "last-modified", "x-next-page",
                                           "x-ratelimit-remaining", "x-ratelimit-reset",
                                           "x-ratelimit-limit", "retry-after")}
                if response.headers.get("X-RateLimit-Remaining") == "0":
                    with self.lock:
                        self.next_at[host] = max(self.next_at.get(host, 0),
                                                 float(response.headers.get("X-RateLimit-Reset", time.time() + 3600)) + 3)
                return json.loads(raw), {"headers": meta, "url": response.geturl(),
                                         "fetched_at": utc_now(), "http_status": response.status}
        except urllib.error.HTTPError as exc:
            body = exc.read(4096).decode("utf-8", "replace").lower()
            if exc.code == 429 or (exc.code == 403 and (
                    exc.headers.get("X-RateLimit-Remaining") == "0" or exc.headers.get("Retry-After")
                    or "rate limit" in body or "abuse detection" in body)):
                retry = exc.headers.get("Retry-After", "60")
                try:
                    retry = float(retry)
                except ValueError:
                    retry = max(1, email.utils.parsedate_to_datetime(retry).timestamp() - time.time())
                reset = float(exc.headers.get("X-RateLimit-Reset", "0")) - time.time() + 3
                retry = max(retry, reset, 60)
                with self.lock:
                    self.next_at[host] = max(self.next_at.get(host, 0), time.time() + retry)
                raise RetryRequest("HTTP {} rate_limited".format(exc.code), retry)
            raise

    def seed(self, repo):
        path = repo["repo_path"]
        if repo["forge"] == "github":
            base = "https://api.github.com/repos/" + path
            self.state.enqueue_api(repo, base, "repository")
            self.state.enqueue_api(repo, base + "/issues?state=all&per_page=100&page=1", "issues")
            self.state.enqueue_api(repo, base + "/pulls?state=all&per_page=100&page=1", "pulls")
            for endpoint in ("labels", "milestones?state=all"):
                join = "&" if "?" in endpoint else "?"
                self.state.enqueue_api(repo, base + "/" + endpoint + join + "per_page=100&page=1", "metadata_list")
        elif repo["forge"] == "gitlab":
            base = "https://{}/api/v4/projects/{}".format(repo["host"], urllib.parse.quote(path, safe=""))
            self.state.enqueue_api(repo, base, "repository")
            for endpoint, kind in (("issues?state=all", "issues"),
                                   ("merge_requests?state=all", "pulls"),
                                   ("labels?", "metadata_list"), ("milestones?state=all", "metadata_list")):
                self.state.enqueue_api(repo, base + "/" + endpoint + "&per_page=100&page=1", kind)
        elif repo["forge"] == "bitbucket":
            base = "https://api.bitbucket.org/2.0/repositories/" + path
            self.state.enqueue_api(repo, base, "repository")
            self.state.enqueue_api(repo, base + "/issues?pagelen=100", "issues")
            self.state.enqueue_api(repo, base + "/pullrequests?state=OPEN&state=MERGED&state=DECLINED&state=SUPERSEDED&pagelen=50", "pulls")

    def children(self, task, data, meta):
        repo = self.state.rows("SELECT * FROM repos WHERE repo_id=?", (task["repo_id"],))[0]
        parsed = urllib.parse.urlsplit(task["url"])
        # Do not send tokens or follow pagination to an unrelated host.
        def enqueue(url, kind):
            if urllib.parse.urlsplit(url).netloc != parsed.netloc:
                raise ValueError("pagination changed API host")
            self.state.enqueue_api(repo, url, kind)

        links = meta["headers"].get("Link", meta["headers"].get("link", ""))
        match = re.search(r'<([^>]+)>;\s*rel="next"', links)
        if match:
            enqueue(match.group(1), task["kind"])
        elif repo["forge"] == "gitlab":
            next_page = meta["headers"].get("X-Next-Page", meta["headers"].get("x-next-page"))
            if next_page:
                query = urllib.parse.parse_qs(parsed.query)
                query["page"] = [next_page]
                enqueue(urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query, doseq=True))), task["kind"])
        elif isinstance(data, dict) and data.get("next"):
            enqueue(data["next"], task["kind"])
        items = data if isinstance(data, list) else data.get("values", []) if isinstance(data, dict) else []
        if task["kind"] not in ("issues", "pulls"):
            return len(items) if isinstance(data, list) or "values" in data else 1
        for item in items:
            if repo["forge"] == "github":
                number = item["number"]
                base = "https://api.github.com/repos/" + repo["repo_path"]
                if task["kind"] == "issues":
                    for endpoint in ("comments", "events", "timeline"):
                        enqueue(base + "/issues/{}/{}?per_page=100&page=1".format(number, endpoint), "issue_" + endpoint)
                elif task["kind"] == "pulls":
                    enqueue(base + "/pulls/{}".format(number), "pull_detail")
                    for endpoint in ("reviews", "comments", "commits", "files"):
                        enqueue(base + "/pulls/{}/{}?per_page=100&page=1".format(number, endpoint), "pull_" + endpoint)
            elif repo["forge"] == "gitlab":
                base = task["url"].split("?")[0] + "/" + str(item["iid"])
                enqueue(base, "detail")
                for endpoint in ("notes", "discussions", "resource_state_events", "resource_label_events"):
                    enqueue(base + "/" + endpoint + "?per_page=100&page=1", endpoint)
                if task["kind"] == "pulls":
                    for endpoint in ("commits", "diffs"):
                        enqueue(base + "/" + endpoint + "?per_page=100&page=1", endpoint)
            elif repo["forge"] == "bitbucket":
                base = task["url"].split("?")[0] + "/" + str(item["id"])
                enqueue(base, "detail")
                for endpoint in (("comments", "changes") if task["kind"] == "issues" else ("comments", "activity", "commits", "diffstat", "tasks")):
                    enqueue(base + "/" + endpoint + "?pagelen=100", endpoint)
        return len(items)

    def run_task(self, task):
        destination = self.state.root / "trackers" / task["repo_id"] / "pages" / (task["task_id"] + ".json")
        try:
            if destination.exists():
                saved = json.loads(destination.read_text())
                data, meta = saved["response"], saved["request_metadata"]
            else:
                data, meta = self.request(task["url"])
                atomic_json(destination, {"request_metadata": meta, "response": data,
                                           "request_url": task["url"], "kind": task["kind"]})
            count = self.children(task, data, meta)
            self.state.execute("UPDATE api_tasks SET status='complete',records=?,finished_at=?,error=NULL WHERE task_id=?",
                               (count, utc_now(), task["task_id"]))
        except RetryRequest as exc:
            # Quota waits do not consume the network retry allowance.
            self.state.execute("UPDATE api_tasks SET status='retry_wait',attempts=attempts-1,next_at=?,error=? WHERE task_id=?",
                               (time.time() + exc.delay, str(exc), task["task_id"]))
            self.state.event("api_wait", host=task["host"], reason=str(exc), delay_s=exc.delay)
        except urllib.error.HTTPError as exc:
            terminal = exc.code in (401, 403, 404, 410, 422)
            status = "unavailable_http_{}".format(exc.code) if terminal else "retry_wait"
            if not terminal and task["attempts"] >= 5:
                status = "failed"
            self.state.execute("UPDATE api_tasks SET status=?,next_at=?,error=? WHERE task_id=?",
                               (status, time.time() + 60 * 2 ** min(task["attempts"], 6),
                                "HTTP {} (not a completed endpoint)".format(exc.code), task["task_id"]))
        except Exception as exc:
            self.state.execute("UPDATE api_tasks SET status=?,next_at=?,error=? WHERE task_id=?",
                               ("failed" if task["attempts"] >= 5 else "retry_wait",
                                time.time() + 60 * 2 ** min(task["attempts"], 6),
                                "{}: {}".format(type(exc).__name__, str(exc))[:800], task["task_id"]))
