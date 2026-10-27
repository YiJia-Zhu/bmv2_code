#!/usr/bin/env python3
"""Map candidate .p4 rows to SWH origins through the public Graph HTTP API.

The Graph API performs reverse provenance traversal server-side. The script
deduplicates by direct parent directory, checkpoints each completed request,
resolves origin SWHIDs to URLs through the small Athena origin table, then
writes row-level and unique-repository CSVs.
"""

from __future__ import print_function

import argparse
import csv
import datetime
import gzip
import hashlib
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


GRAPH_API = "https://archive.softwareheritage.org/api/1/graph"
DEFAULT_REGION = "us-east-1"
DEFAULT_WORKGROUP = "swh-p4-pilot"
DEFAULT_DATABASE = "swh_p4"
DEFAULT_ORIGIN_TABLE = "origin_20260604"
DEFAULT_CANDIDATE_ROOT = (
    "s3://swh2p4/file-screen/swh-p4-20260604/full-128-20261020/"
    "filtered_v2/candidate_csv"
)
DEFAULT_OUTPUT_ROOT = (
    "s3://swh2p4/glue-results/swh-p4-20260604/"
    "api-graph-mapper/candidate-0-7-v1"
)
HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
MAPPING_COLUMNS = [
    "source_shard", "source_csv", "source_row", "directory_id", "name",
    "content_id", "version_status", "quality_status", "origin_id",
    "origin_url", "repository_name", "origin_is_github",
]


def log(event, **fields):
    row = {"event": event, "timestamp_utc": datetime.datetime.now(
        datetime.timezone.utc).isoformat()}
    row.update(fields)
    print(json.dumps(row, sort_keys=True), flush=True)


def run_aws(args, capture=True):
    command = ["aws"] + list(args)
    result = subprocess.run(command, check=False, stdout=subprocess.PIPE if capture else None,
                            stderr=subprocess.PIPE if capture else None,
                            universal_newlines=True)
    if result.returncode:
        raise RuntimeError("AWS CLI failed ({}): {}".format(
            " ".join(command[:3]), (result.stderr or "").strip()))
    return result.stdout if capture else ""


def s3_client(region):
    import boto3
    from botocore.config import Config

    credentials = json.loads(run_aws(["configure", "export-credentials",
                                      "--format", "process"]))
    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials.get("SessionToken"),
        region_name=region,
    )
    return session.client(
        "s3", config=Config(connect_timeout=5, read_timeout=30,
                            retries={"total_max_attempts": 3, "mode": "standard"}))


def s3_location(uri):
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError("invalid S3 URI: {}".format(uri))
    return parsed.netloc, parsed.path.lstrip("/")


def candidate_uris(root, shards, region):
    parsed = urllib.parse.urlparse(root)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("candidate root must be an S3 URI")
    client = s3_client(region)
    result = []
    for shard in shards:
        prefix = "{}/source_shard={}/".format(parsed.path.lstrip("/").rstrip("/"), shard)
        listing = list(client.get_paginator("list_objects_v2").paginate(
            Bucket=parsed.netloc, Prefix=prefix))
        objects = [obj for page in listing for obj in page.get("Contents", [])
                   if obj["Key"].endswith(".csv")]
        if len(objects) != 1:
            raise RuntimeError("expected one CSV in source_shard={}: {}".format(
                shard, len(objects)))
        obj = objects[0]
        result.append({"uri": "s3://{}/{}".format(parsed.netloc, obj["Key"]),
                       "size": obj["Size"]})
    return result


def load_candidate_csvs(uris, work_dir, region):
    """Download the selected candidate shards and keep only mapping columns."""
    normalized_path = os.path.join(work_dir, "candidate_hits.csv")
    directories = set()
    hit_count = 0
    client = s3_client(region)
    temporary_output = normalized_path + ".partial"
    with open(temporary_output, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=MAPPING_COLUMNS[:8])
        writer.writeheader()
        for item in uris:
            uri = item["uri"] if isinstance(item, dict) else item
            expected_size = item.get("size") if isinstance(item, dict) else None
            bucket, key = s3_location(uri)
            if expected_size is None:
                expected_size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
            local_path = os.path.join(work_dir, "input-{}.csv".format(
                hashlib.sha256(uri.encode("utf-8")).hexdigest()[:16]))
            if not os.path.exists(local_path) or os.path.getsize(local_path) != expected_size:
                log("candidate_download_started", uri=uri)
                temporary_path = local_path + ".partial"
                client.download_file(bucket, key, temporary_path)
                if os.path.getsize(temporary_path) != expected_size:
                    raise RuntimeError("incomplete candidate download: {}".format(uri))
                os.replace(temporary_path, local_path)
            match = re.search(r"source_shard=(\d+)", uri)
            if not match:
                raise ValueError("candidate URI must contain source_shard=N: {}".format(uri))
            shard = int(match.group(1))
            with open(local_path, "r", newline="", encoding="utf-8-sig") as source:
                reader = csv.DictReader(source)
                required = {"directory_id", "name", "content_id"}
                if not required.issubset(set(reader.fieldnames or [])):
                    raise ValueError("candidate CSV missing required columns: {}".format(uri))
                for row in reader:
                    if row.get("discovery_disposition") != "p4_16_candidate":
                        continue
                    directory_id = (row.get("directory_id") or "").strip().lower()
                    if not HEX40_RE.fullmatch(directory_id):
                        continue
                    source_path = row.get("source_path", "")
                    source_match = re.search(r"source_shard=(\d+)", source_path)
                    source_row = row.get("source_row") or row.get("row_number") or ""
                    writer.writerow({
                        "source_shard": source_match.group(1) if source_match else shard,
                        "source_csv": row.get("source_csv", ""),
                        "source_row": source_row,
                        "directory_id": directory_id,
                        "name": row.get("name", ""),
                        "content_id": row.get("content_id", ""),
                        "version_status": row.get("version_status", ""),
                        "quality_status": row.get("quality_status", ""),
                    })
                    directories.add(directory_id)
                    hit_count += 1
            log("candidate_shard_read", shard=shard, cumulative_candidate_rows=hit_count,
                unique_directories=len(directories))
    os.replace(temporary_output, normalized_path)
    log("candidate_input_ready", candidate_rows=hit_count,
        unique_directories=len(directories), local_csv=normalized_path)
    return normalized_path, sorted(directories), hit_count


def load_local_quality_shards(root, shards, work_dir):
    """Use completed local pass+excluded exports; quality never gates mapping."""
    normalized_path = os.path.join(work_dir, "candidate_hits.csv")
    temporary_output = normalized_path + ".partial"
    directories = set()
    hit_count = 0
    with open(temporary_output, "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=MAPPING_COLUMNS[:8])
        writer.writeheader()
        for shard in shards:
            checkpoint_path = os.path.join(root, "checkpoints", "shard-{}.json".format(shard))
            with open(checkpoint_path, "r", encoding="utf-8") as source:
                checkpoint = json.load(source)["result"]
            shard_count = 0
            for category in ("quality_pass_csv", "quality_excluded_csv"):
                path = os.path.join(root, category, "source_shard={}".format(shard),
                                    "part.csv.gz")
                with gzip.open(path, "rt", newline="", encoding="utf-8-sig") as source:
                    for row in csv.DictReader(source):
                        if row.get("discovery_disposition") != "p4_16_candidate":
                            raise RuntimeError("local quality export includes noncandidate row")
                        directory_id = (row.get("directory_id") or "").strip().lower()
                        if not HEX40_RE.fullmatch(directory_id):
                            raise RuntimeError("invalid directory ID in local quality export")
                        writer.writerow({
                            "source_shard": row.get("source_shard") or str(shard),
                            "source_csv": row.get("source_csv", ""),
                            "source_row": row.get("source_row", ""),
                            "directory_id": directory_id,
                            "name": row.get("name", ""),
                            "content_id": row.get("content_id", ""),
                            "version_status": row.get("version_status", ""),
                            "quality_status": row.get("quality_v1_status", ""),
                        })
                        directories.add(directory_id)
                        hit_count += 1
                        shard_count += 1
            if shard_count != checkpoint["rows"]:
                raise RuntimeError("shard {} has {} rows, checkpoint expects {}".format(
                    shard, shard_count, checkpoint["rows"]))
            log("candidate_shard_read", shard=shard, shard_candidate_rows=shard_count,
                cumulative_candidate_rows=hit_count, unique_directories=len(directories),
                source="completed_local_quality_export")
    os.replace(temporary_output, normalized_path)
    log("candidate_input_ready", candidate_rows=hit_count,
        unique_directories=len(directories), local_csv=normalized_path)
    return normalized_path, sorted(directories), hit_count


def load_completed_cache(cache_path):
    completed = {}
    if not os.path.exists(cache_path):
        return completed
    with open(cache_path, "r", encoding="utf-8") as stream:
        for line in stream:
            try:
                record = json.loads(line)
            except ValueError:
                continue
            directory_id = record.get("directory_id")
            if HEX40_RE.fullmatch(directory_id or "") and record.get("status") in (
                    "complete", "not_found"):
                completed[directory_id] = record
    return completed


def request_graph_origins(directory_id, token, timeout):
    query = urllib.parse.urlencode({
        "direction": "backward",
        "edges": "*",
        "return_types": "ori",
        "max_edges": "0",
        "max_matching_nodes": "0",
    })
    url = "{}/leaves/swh:1:dir:{}/?{}".format(GRAPH_API, directory_id, query)
    request = urllib.request.Request(url)
    request.add_header("Accept", "text/plain")
    request.add_header("User-Agent", "swh-p4-repository-mapper/1.0")
    request.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace")
            headers = response.headers
    except urllib.error.HTTPError as exc:
        detail = exc.read(500).decode("utf-8", "replace")
        if exc.code == 404:
            return {"status": "not_found", "origin_ids": [],
                    "http_status": exc.code, "detail": detail}, exc.headers
        if exc.code == 401:
            raise RuntimeError("SWH Graph API rejected SWH_API_TOKEN (401)")
        if exc.code == 429:
            return {"status": "rate_limited", "origin_ids": [],
                    "http_status": exc.code, "detail": detail}, exc.headers
        raise RuntimeError("SWH Graph API HTTP {}: {}".format(exc.code, detail))
    origin_ids = sorted({
        line.strip().lower().split(":")[-1]
        for line in body.splitlines()
        if line.strip().lower().startswith("swh:1:ori:")
        and HEX40_RE.fullmatch(line.strip().lower().split(":")[-1])
    })
    return {"status": "complete", "origin_ids": origin_ids,
            "http_status": 200}, headers


def rate_delay(headers):
    """Spread requests across the server-advertised rate-limit window."""
    try:
        limit = int(headers.get("X-Ratelimit-Limit", "0"))
        remaining = int(headers.get("X-Ratelimit-Remaining", "0"))
        reset = int(headers.get("X-Ratelimit-Reset", "0"))
    except (TypeError, ValueError):
        return 0.0, None
    if limit <= 0 or remaining <= 0 or reset <= 0:
        return 0.0, {"limit": limit, "remaining": remaining, "reset": reset}
    seconds_left = max(0, reset - int(time.time()))
    return max(0.0, float(seconds_left) / remaining), {
        "limit": limit, "remaining": remaining, "reset": reset,
    }


def map_directories(directories, token, cache_path, timeout, upload_root=None,
                    region=DEFAULT_REGION):
    completed = load_completed_cache(cache_path)
    pending = [item for item in directories if item not in completed]
    started = time.monotonic()
    log("graph_mapping_started", total_directories=len(directories),
        already_complete=len(completed), pending=len(pending), cache_path=cache_path)
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "a", encoding="utf-8") as output:
        for index, directory_id in enumerate(pending, 1):
            while True:
                record, headers = request_graph_origins(directory_id, token, timeout)
                delay, limit = rate_delay(headers)
                if record["status"] != "rate_limited":
                    record["directory_id"] = directory_id
                    record["completed_at_utc"] = datetime.datetime.now(
                        datetime.timezone.utc).isoformat()
                    output.write(json.dumps(record, sort_keys=True) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
                    completed[directory_id] = record
                    break
                wait = max(30.0, delay)
                log("graph_api_rate_limited", directory_id=directory_id,
                    wait_seconds=round(wait, 1), rate_limit=limit)
                time.sleep(wait)
            if index % 10 == 0 or index == len(pending):
                total_done = len(completed)
                elapsed = max(0.001, time.monotonic() - started)
                rate = index / elapsed
                eta = (len(pending) - index) / rate if rate else None
                log("graph_mapping_progress", completed=total_done,
                    total=len(directories), percent=round(100.0 * total_done / len(directories), 2),
                    new_origins=sum(len(x["origin_ids"]) for x in completed.values()),
                    elapsed_seconds=round(elapsed, 1), eta_seconds=round(eta) if eta else None,
                    rate_limit=limit)
            if upload_root and (index % 50 == 0 or index == len(pending)):
                run_aws(["s3", "cp", cache_path, upload_root.rstrip("/") + "/" +
                         os.path.basename(cache_path), "--region", region,
                         "--only-show-errors"])
                log("graph_cache_uploaded", completed=len(completed),
                    total=len(directories))
            if delay:
                time.sleep(delay)
    return completed


def resolve_origin_urls(origin_ids, args, work_dir):
    if not origin_ids:
        return {}
    result = {}
    total_scanned = 0
    for name in os.listdir(work_dir):
        if name.startswith("origin-") and name.endswith(".query_id"):
            with open(os.path.join(work_dir, name), "r", encoding="ascii") as source:
                old_query_id = source.read().strip()
            old_response = json.loads(run_aws([
                "athena", "get-query-execution", "--region", args.region,
                "--query-execution-id", old_query_id,
            ]))["QueryExecution"]
            total_scanned += old_response.get("Statistics", {}).get("DataScannedInBytes", 0)
    for start in range(0, len(origin_ids), 2000):
        batch = origin_ids[start:start + 2000]
        if any(not HEX40_RE.fullmatch(item) for item in batch):
            raise ValueError("invalid origin ID in Graph cache")
        batch_hash = hashlib.sha256("\n".join(batch).encode("ascii")).hexdigest()[:16]
        sql_path = os.path.join(work_dir, "origin-{}.sql".format(batch_hash))
        query_id_path = os.path.join(work_dir, "origin-{}.query_id".format(batch_hash))
        result_path = os.path.join(work_dir, "origin-{}.csv".format(batch_hash))
        if not os.path.exists(result_path):
            new_query = not os.path.exists(query_id_path)
            if os.path.exists(query_id_path):
                with open(query_id_path, "r", encoding="ascii") as source:
                    query_id = source.read().strip()
            else:
                if total_scanned >= args.max_athena_scanned_bytes:
                    raise RuntimeError("Athena scan budget reached before all URLs resolved")
                with open(sql_path, "w", encoding="ascii") as sql:
                    sql.write("SELECT id, url FROM {}.{} WHERE id IN ({});\n".format(
                        args.database, args.origin_table,
                        ",".join("'{}'".format(item) for item in batch)))
                response = json.loads(run_aws([
                    "athena", "start-query-execution", "--region", args.region,
                    "--work-group", args.workgroup,
                    "--query-execution-context", "Database={}".format(args.database),
                    "--query-string", "file://" + sql_path,
                ]))
                query_id = response["QueryExecutionId"]
                with open(query_id_path, "w", encoding="ascii") as output:
                    output.write(query_id + "\n")
                log("origin_url_query_started", query_execution_id=query_id,
                    batch_size=len(batch), batch_start=start,
                    total_origin_ids=len(origin_ids))
            while True:
                response = json.loads(run_aws([
                    "athena", "get-query-execution", "--region", args.region,
                    "--query-execution-id", query_id,
                ]))["QueryExecution"]
                state = response["Status"]["State"]
                stats = response.get("Statistics", {})
                log("origin_url_query_progress", query_execution_id=query_id, state=state,
                    scanned_bytes=stats.get("DataScannedInBytes", 0))
                if state == "SUCCEEDED":
                    result_uri = response["ResultConfiguration"]["OutputLocation"]
                    break
                if state in ("FAILED", "CANCELLED"):
                    raise RuntimeError("Athena origin lookup {}: {}".format(
                        state, response["Status"].get("StateChangeReason")))
                time.sleep(10)
            run_aws(["s3", "cp", result_uri, result_path, "--region", args.region,
                     "--only-show-errors"])
            if new_query:
                total_scanned += stats.get("DataScannedInBytes", 0)
        with open(result_path, "r", newline="", encoding="utf-8-sig") as source:
            for row in csv.DictReader(source):
                origin_id = (row.get("id") or "").strip().lower()
                if origin_id in batch and row.get("url"):
                    result[origin_id] = row["url"].strip()
        log("origin_url_batch_completed", batch_start=start, batch_size=len(batch),
            urls_resolved=len(result), total_origin_ids=len(origin_ids),
            new_athena_scanned_bytes=total_scanned)
    log("origin_urls_resolved", origin_ids=len(origin_ids), urls=len(result),
        missing_origin_ids=len(set(origin_ids) - set(result)),
        new_athena_scanned_bytes=total_scanned)
    return result


def write_outputs(candidate_csv, directories, cache, urls, work_dir):
    origins_by_directory = {
        directory: [origin for origin in cache[directory].get("origin_ids", [])
                    if origin in urls]
        for directory in directories
    }
    mapping_path = os.path.join(work_dir, "candidate_file_repository_mapping.csv")
    repo_path = os.path.join(work_dir, "repository_candidates.csv")
    unmapped_path = os.path.join(work_dir, "unmapped_candidate_hits.csv")
    repositories = {}
    with open(candidate_csv, "r", newline="", encoding="utf-8") as source, \
            open(mapping_path, "w", newline="", encoding="utf-8") as output, \
            open(unmapped_path, "w", newline="", encoding="utf-8") as unmatched:
        reader = csv.DictReader(source)
        writer = csv.DictWriter(output, fieldnames=MAPPING_COLUMNS)
        writer.writeheader()
        unmatched_writer = csv.DictWriter(
            unmatched, fieldnames=MAPPING_COLUMNS[:8] + ["unmapped_reason"])
        unmatched_writer.writeheader()
        for hit in reader:
            directory_id = hit["directory_id"]
            resolved = origins_by_directory.get(directory_id, [])
            if not resolved:
                record = cache[directory_id]
                reason = ("origin_url_not_found" if record.get("origin_ids")
                          else "no_origin_in_graph" if record["status"] == "complete"
                          else "directory_absent_from_graph")
                unmatched_writer.writerow(dict(hit, unmapped_reason=reason))
            for origin_id in resolved:
                url = urls[origin_id]
                name = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
                if name.lower().endswith(".git"):
                    name = name[:-4]
                github = (urllib.parse.urlparse(url).hostname or "").lower() in (
                    "github.com", "www.github.com")
                writer.writerow(dict(hit, origin_id=origin_id, origin_url=url,
                                     repository_name=name,
                                     origin_is_github=str(github).lower()))
                repositories[origin_id] = (url, name, github)
    with open(repo_path, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["origin_id", "origin_url", "repository_name", "origin_is_github"])
        for origin_id, (url, name, github) in sorted(repositories.items(), key=lambda item: item[1][0]):
            writer.writerow([origin_id, url, name, str(github).lower()])
    mapped_dirs = sum(bool(items) for items in origins_by_directory.values())
    log("mapping_outputs_written", directories=len(directories), mapped_directories=mapped_dirs,
        distinct_origins=len(repositories), mapping_csv=mapping_path,
        repository_csv=repo_path, unmapped_csv=unmapped_path)
    return mapping_path, repo_path, unmapped_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-uri", action="append",
                        help="S3 candidate CSV URI; repeat once for each selected shard")
    parser.add_argument("--candidate-root", default=DEFAULT_CANDIDATE_ROOT)
    parser.add_argument("--shards", default="0-7")
    parser.add_argument("--local-quality-root")
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--workgroup", default=DEFAULT_WORKGROUP)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--origin-table", default=DEFAULT_ORIGIN_TABLE)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--expected-hits", type=int, default=716961)
    parser.add_argument("--expected-directories", type=int, default=4185)
    parser.add_argument("--max-athena-scanned-bytes", type=int, default=200000000000)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args(argv)
    if not os.path.isabs(args.work_dir):
        args.work_dir = os.path.abspath(args.work_dir)
    os.makedirs(args.work_dir, exist_ok=True)
    match = re.fullmatch(r"(\d+)-(\d+)", args.shards)
    if not match or int(match.group(1)) > int(match.group(2)):
        parser.error("--shards must be an inclusive range such as 0-7")
    shards = range(int(match.group(1)), int(match.group(2)) + 1)
    if args.local_quality_root:
        candidate_csv, directories, hit_count = load_local_quality_shards(
            args.local_quality_root, shards, args.work_dir)
    elif args.candidate_uri:
        uris = args.candidate_uri
        candidate_csv, directories, hit_count = load_candidate_csvs(uris, args.work_dir,
                                                                     args.region)
    else:
        uris = candidate_uris(args.candidate_root, shards, args.region)
        candidate_csv, directories, hit_count = load_candidate_csvs(uris, args.work_dir,
                                                                     args.region)
    if args.expected_hits and hit_count != args.expected_hits:
        raise RuntimeError("expected {} candidate rows, found {}".format(
            args.expected_hits, hit_count))
    if args.expected_directories and len(directories) != args.expected_directories:
        raise RuntimeError("expected {} parent directories, found {}".format(
            args.expected_directories, len(directories)))
    directories_path = os.path.join(args.work_dir, "directory_ids.txt")
    with open(directories_path, "w", encoding="ascii") as output:
        output.write("\n".join(directories) + "\n")
    if args.prepare_only:
        if args.upload:
            for path in (candidate_csv, directories_path):
                run_aws(["s3", "cp", path,
                         args.output_root.rstrip("/") + "/" + os.path.basename(path),
                         "--region", args.region, "--only-show-errors"])
        log("preparation_completed", candidate_rows=hit_count,
            unique_directories=len(directories), local_work_dir=args.work_dir,
            output_root=args.output_root if args.upload else None)
        return
    token = os.environ.get("SWH_API_TOKEN", "").strip()
    if not token:
        raise SystemExit("Set SWH_API_TOKEN locally; token values are never logged.")
    cache_path = os.path.join(args.work_dir, "directory_origin_cache.jsonl")
    if args.upload and not os.path.exists(cache_path):
        try:
            run_aws(["s3", "cp", args.output_root.rstrip("/") + "/" +
                     os.path.basename(cache_path), cache_path, "--region", args.region,
                     "--only-show-errors"])
            log("graph_cache_restored", local_path=cache_path)
        except RuntimeError:
            pass
    cache = map_directories(directories, token, cache_path, args.timeout,
                            upload_root=args.output_root if args.upload else None,
                            region=args.region)
    origin_ids = sorted({origin for row in cache.values() for origin in row.get("origin_ids", [])})
    urls = resolve_origin_urls(origin_ids, args, args.work_dir)
    mapping_path, repo_path, unmapped_path = write_outputs(
        candidate_csv, directories, cache, urls, args.work_dir)
    if args.upload:
        prefix = args.output_root.rstrip("/") + "/"
        for path in (candidate_csv, cache_path, mapping_path, repo_path, unmapped_path):
            run_aws(["s3", "cp", path, prefix + os.path.basename(path),
                     "--region", args.region, "--only-show-errors"])
        log("outputs_uploaded", output_root=args.output_root)
    log("run_completed", candidate_rows=hit_count, unique_directories=len(directories),
        graph_directories_completed=len(cache), origin_ids=len(origin_ids),
        origin_urls=len(urls), mapped_file_rows=sum(1 for _ in open(mapping_path, "r",
                                                                    encoding="utf-8")) - 1,
        local_work_dir=args.work_dir)


if __name__ == "__main__":
    main()
