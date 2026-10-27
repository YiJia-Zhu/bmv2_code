"""Smoke-test BigQuery -> GitHub and Software Heritage access.

Examples:
    python tmp.py
    python tmp.py --stages github swh
    python tmp.py --repo p4lang/tutorials --max-discovery-bytes 0

# ssh -p 2221 -L 8080:127.0.0.1:8080 xiegr@127.0.0.1

# ssh -L 11452:127.0.0.1:11452 zhenning@101.35.216.87
# 此外还得注意一下unset https_proxy http_proxy
The default BigQuery discovery query uses the public ``sample_files`` table.
It is always dry-run first and is executed only when its estimated scan size is
within ``--max-discovery-bytes``. Passing 0 keeps discovery dry-run-only.
"""

from __future__ import annotations

import argparse
import errno
import os
import sys
from pathlib import Path
from typing import Any

import requests


BASE_DIR = Path(__file__).resolve().parent
CLIENT_SECRET_FILE = (
    BASE_DIR
    / "client_secret_108904989975-j74k2fu8q3elq5kggvdgu4t3or80kvql.apps.googleusercontent.com.json"
)
TOKEN_FILE = BASE_DIR / "token.json"

PROJECT_ID = "ornate-grail-473307-m8"
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

DEFAULT_REPO = "p4lang/tutorials"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_DISCOVERY_BYTES = 1024**3  # 1 GiB

GITHUB_API = "https://api.github.com"
SWH_API = "https://archive.softwareheritage.org/api/1"

# A small public sample is sufficient for a smoke test. The production query
# should use bigquery-public-data.github_repos.files instead.
DISCOVERY_SQL = r"""
SELECT
  repo_name,
  path
FROM `bigquery-public-data.github_repos.sample_files`
WHERE REGEXP_CONTAINS(LOWER(path), r'(^|/)[^/]+\.p4$')
LIMIT 1
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test BigQuery, GitHub, and Software Heritage."
    )
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=("bigquery", "github", "swh"),
        default=("bigquery", "github", "swh"),
        help="Stages to run (default: all three).",
    )
    parser.add_argument(
        "--repo",
        default=DEFAULT_REPO,
        help="Fallback GitHub owner/repo used when discovery is skipped or empty.",
    )
    parser.add_argument(
        "--max-discovery-bytes",
        type=int,
        default=DEFAULT_MAX_DISCOVERY_BYTES,
        help=(
            "Run the public-dataset discovery query only when dry-run bytes are "
            "at most this value; 0 means dry-run only (default: 1 GiB)."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds (default: 20).",
    )
    parser.add_argument(
        "--oauth-port",
        type=int,
        default=8080,
        help="Local OAuth callback port used by BigQuery login (default: 11452).",
    )
    return parser.parse_args()


def get_credentials(oauth_port: int) -> Any:
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError as exc:
        raise RuntimeError(
            "BigQuery dependencies are missing. Install google-cloud-bigquery "
            "and google-auth-oauthlib in this Python environment."
        ) from exc

    credentials = None

    if TOKEN_FILE.exists():
        print("    Loading existing OAuth token...", flush=True)
        credentials = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if credentials and credentials.expired and credentials.refresh_token:
        print("    Refreshing OAuth token...", flush=True)
        credentials.refresh(Request())
        save_credentials(credentials)

    if not credentials or not credentials.valid:
        if not CLIENT_SECRET_FILE.exists():
            raise FileNotFoundError(f"OAuth client secret not found: {CLIENT_SECRET_FILE}")

        print(
            f"    Starting OAuth login on http://localhost:{oauth_port} ...",
            flush=True,
        )
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        try:
            credentials = flow.run_local_server(
                host="localhost",
                port=oauth_port,
                open_browser=False,
            )
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                raise RuntimeError(
                    f"OAuth callback port {oauth_port} is already in use; choose "
                    "another one with --oauth-port and update the SSH tunnel."
                ) from exc
            raise
        save_credentials(credentials)

    return credentials


def save_credentials(credentials: Any) -> None:
    TOKEN_FILE.write_text(credentials.to_json(), encoding="utf-8")
    TOKEN_FILE.chmod(0o600)


def smoke_bigquery(max_discovery_bytes: int, oauth_port: int) -> str | None:
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError(
            "BigQuery dependencies are missing. Install google-cloud-bigquery "
            "and google-auth-oauthlib in this Python environment."
        ) from exc

    credentials = get_credentials(oauth_port)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

    row = next(iter(client.query("SELECT 1 AS test").result()))
    if row.test != 1:
        raise RuntimeError(f"Unexpected SELECT 1 result: {row.test!r}")
    print(f"    SELECT 1 = {row.test}; project={PROJECT_ID}", flush=True)

    dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(DISCOVERY_SQL, job_config=dry_config)
    estimated_bytes = int(dry_job.total_bytes_processed or 0)
    print(
        "    github_repos.sample_files dry-run bytes: "
        f"{estimated_bytes:,}",
        flush=True,
    )

    if max_discovery_bytes <= 0:
        print("    Discovery execution skipped (--max-discovery-bytes=0).", flush=True)
        return None
    if estimated_bytes > max_discovery_bytes:
        print(
            "    Discovery execution skipped: estimate exceeds limit "
            f"({estimated_bytes:,} > {max_discovery_bytes:,}).",
            flush=True,
        )
        return None

    query_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=max_discovery_bytes,
        use_query_cache=True,
    )
    query_job = client.query(DISCOVERY_SQL, job_config=query_config)
    rows = list(query_job.result())
    print(f"    Discovery job id: {query_job.job_id}", flush=True)
    if not rows:
        print("    No .p4 row found in sample_files; using fallback repo.", flush=True)
        return None

    repo_name = str(rows[0].repo_name)
    print(f"    Candidate: {repo_name} ({rows[0].path})", flush=True)
    return repo_name


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "p4-source-smoke-test/1.0",
        }
    )
    return session


def github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    github_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    return headers


def request_json(
    session: requests.Session,
    url: str,
    timeout: float,
    *,
    headers: dict[str, str] | None = None,
) -> Any:
    response = session.get(url, timeout=timeout, headers=headers)
    if not response.ok:
        snippet = response.text[:300].replace("\n", " ")
        raise RuntimeError(f"GET {url} -> HTTP {response.status_code}: {snippet}")
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError as exc:
        raise RuntimeError(f"GET {url} did not return JSON") from exc


def smoke_github(
    session: requests.Session,
    candidate_repo: str,
    fallback_repo: str,
    timeout: float,
) -> dict[str, str]:
    attempted: list[str] = []
    last_error: Exception | None = None
    headers = github_headers()

    for repo in dict.fromkeys((candidate_repo, fallback_repo)):
        attempted.append(repo)
        try:
            metadata = request_json(
                session,
                f"{GITHUB_API}/repos/{repo}",
                timeout,
                headers=headers,
            )
            default_branch = str(metadata["default_branch"])
            commit = request_json(
                session,
                f"{GITHUB_API}/repos/{repo}/commits/{default_branch}",
                timeout,
                headers=headers,
            )
            sha = str(commit["sha"])
            full_name = str(metadata["full_name"])
            html_url = str(metadata["html_url"]).rstrip("/")
            print(f"    Repository: {full_name}", flush=True)
            print(f"    Default branch: {default_branch}", flush=True)
            print(f"    Head commit: {sha}", flush=True)
            return {
                "repo": full_name,
                "origin_url": html_url,
                "commit_sha": sha,
            }
        except (KeyError, RuntimeError, requests.RequestException) as exc:
            last_error = exc
            if repo != fallback_repo:
                print(
                    f"    Candidate {repo} is unavailable on GitHub; "
                    f"trying fallback {fallback_repo}.",
                    flush=True,
                )

    raise RuntimeError(
        f"GitHub smoke failed for {', '.join(attempted)}: {last_error}"
    ) from last_error


def smoke_swh(
    session: requests.Session,
    origin_url: str,
    timeout: float,
) -> dict[str, str]:
    # SWH origins may have been ingested with or without the conventional .git
    # suffix. Try both exact origin spellings without treating a 404 as success.
    origin_variants = [origin_url.rstrip("/")]
    if not origin_variants[0].endswith(".git"):
        origin_variants.append(f"{origin_variants[0]}.git")

    swh_headers: dict[str, str] = {"Accept": "application/json"}
    swh_token = os.environ.get("SWH_API_TOKEN")
    if swh_token:
        swh_headers["Authorization"] = f"Bearer {swh_token}"

    errors: list[str] = []
    for origin in origin_variants:
        endpoint = f"{SWH_API}/origin/{origin}/get/"
        try:
            response = session.get(endpoint, timeout=timeout, headers=swh_headers)
        except requests.RequestException as exc:
            raise RuntimeError(f"GET {endpoint} failed: {exc}") from exc

        if response.status_code == 404:
            errors.append(f"{origin}: not archived")
            continue
        if not response.ok:
            snippet = response.text[:300].replace("\n", " ")
            raise RuntimeError(
                f"GET {endpoint} -> HTTP {response.status_code}: {snippet}"
            )

        origin_record = response.json()
        visit_endpoint = (
            f"{SWH_API}/origin/{origin}/visit/latest/?require_snapshot=true"
        )
        latest_visit = request_json(
            session,
            visit_endpoint,
            timeout,
            headers=swh_headers,
        )
        snapshot = latest_visit.get("snapshot")
        if not snapshot:
            raise RuntimeError(
                f"GET {visit_endpoint} returned a visit without a snapshot"
            )
        visit_date = latest_visit.get("date") or "<unknown>"
        visit_status = latest_visit.get("status") or "<unknown>"
        print(f"    Archived origin: {origin_record.get('url', origin)}", flush=True)
        print(f"    Latest snapshot visit: {visit_date} ({visit_status})", flush=True)
        print(f"    Snapshot: {snapshot}", flush=True)
        return {
            "origin_url": str(origin_record.get("url", origin)),
            "snapshot": str(snapshot),
        }

    raise RuntimeError("SWH origin lookup failed: " + "; ".join(errors))


def main() -> int:
    args = parse_args()
    if args.max_discovery_bytes < 0:
        print("error: --max-discovery-bytes must be non-negative", file=sys.stderr)
        return 2
    if args.timeout <= 0:
        print("error: --timeout must be positive", file=sys.stderr)
        return 2
    if not 1 <= args.oauth_port <= 65535:
        print("error: --oauth-port must be between 1 and 65535", file=sys.stderr)
        return 2
    if args.repo.count("/") != 1:
        print("error: --repo must have the form owner/repo", file=sys.stderr)
        return 2

    stages = set(args.stages)
    failures: dict[str, str] = {}
    candidate_repo = args.repo
    github_result: dict[str, str] | None = None
    session = build_http_session()

    if "bigquery" in stages:
        print("[BigQuery] Starting smoke test...", flush=True)
        try:
            candidate_repo = (
                smoke_bigquery(args.max_discovery_bytes, args.oauth_port) or args.repo
            )
            print("[BigQuery] PASS", flush=True)
        except Exception as exc:  # Keep the remaining independent smokes running.
            failures["BigQuery"] = str(exc)
            print(f"[BigQuery] FAIL: {exc}", flush=True)

    if "github" in stages:
        print("[GitHub] Starting smoke test...", flush=True)
        try:
            github_result = smoke_github(
                session,
                candidate_repo,
                args.repo,
                args.timeout,
            )
            print("[GitHub] PASS", flush=True)
        except Exception as exc:
            failures["GitHub"] = str(exc)
            print(f"[GitHub] FAIL: {exc}", flush=True)

    if "swh" in stages:
        print("[SWH] Starting smoke test...", flush=True)
        origin_url = (
            github_result["origin_url"]
            if github_result
            else f"https://github.com/{args.repo}"
        )
        try:
            smoke_swh(session, origin_url, args.timeout)
            print("[SWH] PASS", flush=True)
        except Exception as exc:
            failures["SWH"] = str(exc)
            print(f"[SWH] FAIL: {exc}", flush=True)

    if failures:
        print("\nSmoke summary: FAIL", flush=True)
        for stage, message in failures.items():
            print(f"  - {stage}: {message}", flush=True)
        return 1

    print("\nSmoke summary: PASS", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
