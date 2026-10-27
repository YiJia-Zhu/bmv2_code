#!/usr/bin/env python3
"""Submit or monitor the bounded, shard-resumable D0 parent scan."""

import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

import boto3
from botocore.config import Config
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import ClientError
from botocore.session import get_session

from swh_glue_d0_parent_scan import VERSION, fingerprint, validate_config


ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "data/raw/swh_p4/discovery/d0_parent_scan_v2"
INVENTORY = ROOT / "data/raw/swh_p4/discovery/probe8_spark_v1/inventory.json"
SCRIPT = ROOT / "data/tools/swh_glue_d0_parent_scan.py"
JOB = "swh-p4-d0-parent-scan-v2"
REGION = "us-east-1"
ROLE = "arn:aws:iam::728516300718:role/service-role/AWSGlueServiceRole"
OUTPUT = "s3://swh2p4/glue-results/swh-p4-20260604/d0-parent-scan-v2"
SEEDS = ("s3://swh2p4/file-screen/swh-p4-20260604/"
         "full-128-20261020/candidate-parent-index-v1/parent_directory_parquet")
WORKERS = 16
TIMEOUT_MINUTES = 720
DPU_HOUR_USD = 0.44
TOTAL_BUDGET_USD = 110.0
OTHER_CHARGES_RESERVE_USD = 15.0
# The first v1 run failed after its completed shard lacked a _SUCCESS marker.
# Include its actual Glue DPUSeconds in the same overall spending ceiling.
PRIOR_V1_DPU_SECONDS = 5162.0
PRIOR_V1_COMPUTE_USD = PRIOR_V1_DPU_SECONDS * DPU_HOUR_USD / 3600
COMPUTE_BUDGET_USD = (TOTAL_BUDGET_USD - OTHER_CHARGES_RESERVE_USD -
                      PRIOR_V1_COMPUTE_USD)
ACTIVE = {"STARTING", "RUNNING", "WAITING", "STOPPING"}


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def emit(event, **fields):
    print(json.dumps(dict(fields, event=event, timestamp_utc=utc_now()),
                     sort_keys=True, default=str), flush=True)


def s3_parts(uri):
    return uri[5:].split("/", 1)


def aws_session_from_cli_login():
    def refresh():
        result = subprocess.run(["aws", "configure", "export-credentials",
                                 "--format", "process"], capture_output=True,
                                text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("AWS CLI login credentials are unavailable")
        value = json.loads(result.stdout)
        required = {"AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"}
        if not required.issubset(value):
            raise RuntimeError("AWS CLI did not export temporary session credentials")
        return {"access_key": value["AccessKeyId"],
                "secret_key": value["SecretAccessKey"],
                "token": value["SessionToken"],
                "expiry_time": value["Expiration"]}

    session = get_session()
    session._credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method="aws-cli-login")
    session.set_config_variable("region", REGION)
    return boto3.Session(botocore_session=session)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, indent=2, default=str)
        handle.write("\n")
    os.replace(str(temporary), str(path))


def read_remote_json(s3, uri):
    bucket, key = s3_parts(uri)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        return None
    with response["Body"] as body:
        return json.load(body)


def put_remote_json(s3, uri, value):
    bucket, key = s3_parts(uri)
    s3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(value, sort_keys=True, default=str).encode("utf-8"),
                  ContentType="application/json")


def build_config(s3):
    with INVENTORY.open(encoding="utf-8") as handle:
        inventory = json.load(handle)
    sources = sorted(inventory["tables"]["directory_entry"]["objects"],
                     key=lambda item: item["key"])
    seed_bucket, seed_prefix = s3_parts(SEEDS)
    listing = s3.list_objects_v2(Bucket=seed_bucket, Prefix=seed_prefix.rstrip("/") + "/")
    parquet = [item for item in listing.get("Contents", [])
               if item["Key"].endswith(".parquet")]
    if listing.get("IsTruncated") or len(parquet) != 1:
        raise RuntimeError("expected one immutable D0 Parquet object")
    seed = parquet[0]
    script_sha = hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    config = {"version": VERSION, "code_sha256": script_sha,
              "source_objects": sources, "seed_count": 65321,
              "seed_uri": SEEDS, "seed_object": {
                  "uri": "s3://{}/{}".format(seed_bucket, seed["Key"]),
                  "etag": seed["ETag"], "size": seed["Size"]},
              "output_uri": OUTPUT}
    config["config_id"] = fingerprint(config)
    validate_config(config)
    return config


def ensure_deployed(s3, glue, config):
    config_uri = OUTPUT + "/inputs/config.json"
    old = read_remote_json(s3, config_uri)
    if old is None:
        put_remote_json(s3, config_uri, config)
    elif old != config:
        raise RuntimeError("remote configuration differs; refusing to reuse checkpoints")
    script_uri = ("s3://swh2p4/glue-scripts/swh-p4-20260604/d0-parent-scan-v2/"
                  + config["code_sha256"] + "/swh_glue_d0_parent_scan.py")
    bucket, key = s3_parts(script_uri)
    try:
        remote = s3.get_object(Bucket=bucket, Key=key)
    except s3.exceptions.NoSuchKey:
        s3.put_object(Bucket=bucket, Key=key, Body=SCRIPT.read_bytes(),
                      ContentType="text/x-python")
    else:
        with remote["Body"] as body:
            if hashlib.sha256(body.read()).hexdigest() != config["code_sha256"]:
                raise RuntimeError("deployed script bytes differ from code SHA-256")
    try:
        job = glue.get_job(JobName=JOB)["Job"]
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(
            Name=JOB, Role=ROLE, GlueVersion="5.0", MaxRetries=0,
            Description="D0 direct-parent ORC scan with per-source checkpoints",
            Command={"Name": "glueetl", "PythonVersion": "3",
                     "ScriptLocation": script_uri},
            WorkerType="G.1X", NumberOfWorkers=WORKERS, Timeout=TIMEOUT_MINUTES,
            ExecutionProperty={"MaxConcurrentRuns": 1},
            DefaultArguments={"--job-language": "python", "--enable-metrics": "true",
                              "--enable-observability-metrics": "true",
                              "--enable-continuous-cloudwatch-log": "true",
                              "--TempDir": "s3://swh2p4/glue-temp/swh-p4-20260604/d0-parent-scan-v2/"})
    else:
        if (job["Command"]["ScriptLocation"] != script_uri or
                job.get("WorkerType") != "G.1X" or
                job.get("NumberOfWorkers") != WORKERS or
                job.get("Timeout") != TIMEOUT_MINUTES or
                job.get("MaxRetries") != 0):
            raise RuntimeError("existing Glue job does not match bounded configuration")
    return config_uri, script_uri


def matching_runs(glue, config_uri):
    runs = []
    for page in glue.get_paginator("get_job_runs").paginate(JobName=JOB):
        runs.extend(run for run in page["JobRuns"]
                    if run.get("Arguments", {}).get("--CONFIG_URI") == config_uri)
    return sorted(runs, key=lambda run: run["StartedOn"])


def charged_dpu_seconds(run):
    actual = run.get("DPUSeconds")
    elapsed_estimate = float(run.get("ExecutionTime", 0)) * WORKERS
    return max(float(actual) if actual is not None else 0, elapsed_estimate)


def allowed_timeout(runs):
    spent = sum(charged_dpu_seconds(run) for run in runs) * DPU_HOUR_USD / 3600
    available = COMPUTE_BUDGET_USD - spent
    minutes = min(TIMEOUT_MINUTES, math.floor(available * 60 / (WORKERS * DPU_HOUR_USD)))
    if minutes < 10:
        raise RuntimeError("compute budget exhausted; no further Glue run permitted")
    return minutes, spent


def save_manifest(s3, manifest):
    write_json(LOCAL / "run.json", manifest)
    put_remote_json(s3, OUTPUT + "/run-manifest.json", manifest)


def submit_or_attach(s3, glue, config, config_uri, launch, resume):
    runs = matching_runs(glue, config_uri)
    active = [run for run in runs if run["JobRunState"] in ACTIVE]
    if len(active) > 1:
        raise RuntimeError("more than one active run for this configuration")
    if active:
        return active[-1]["Id"], runs
    completed = read_remote_json(s3, OUTPUT + "/complete.json")
    if completed is not None:
        if completed.get("config_id") != config["config_id"]:
            raise RuntimeError("completion marker belongs to another configuration")
        emit("already_complete", output_uri=OUTPUT)
        return None, runs
    if runs and not resume:
        return runs[-1]["Id"], runs
    if not runs and not launch:
        raise RuntimeError("no run exists; use --launch to submit the first run")
    previous_intent = read_remote_json(s3, OUTPUT + "/run-manifest.json")
    if not runs and previous_intent and previous_intent.get("config_id") == config["config_id"]:
        intent = previous_intent.get("submission_intent_at_utc")
        if intent and not previous_intent.get("run_id"):
            age = (dt.datetime.now(dt.timezone.utc) -
                   dt.datetime.fromisoformat(intent)).total_seconds()
            if age < 600:
                raise RuntimeError("recent submission has no response; inspect Glue before retrying")
    timeout, spent = allowed_timeout(runs)
    manifest = {"created_at_utc": utc_now(), "job_name": JOB,
                "config_id": config["config_id"], "config_uri": config_uri,
                "output_uri": OUTPUT, "workers": WORKERS,
                "timeout_minutes": timeout, "compute_budget_usd": COMPUTE_BUDGET_USD,
                "total_budget_usd": TOTAL_BUDGET_USD,
                "other_charges_reserve_usd": OTHER_CHARGES_RESERVE_USD,
                "estimated_prior_compute_usd": round(spent, 4),
                "submission_intent_at_utc": utc_now(),
                "previous_run_ids": [run["Id"] for run in runs]}
    save_manifest(s3, manifest)
    response = glue.start_job_run(JobName=JOB, Timeout=timeout,
                                  Arguments={"--CONFIG_URI": config_uri,
                                             "--SHARD_LIMIT": "128"})
    run_id = response["JobRunId"]
    manifest["run_id"] = run_id
    manifest["submitted_at_utc"] = utc_now()
    save_manifest(s3, manifest)
    emit("job_submitted", run_id=run_id, workers=WORKERS, timeout_minutes=timeout,
         max_new_compute_usd=round(WORKERS * timeout * DPU_HOUR_USD / 60, 2),
         prior_compute_usd=round(spent, 2), output_uri=OUTPUT)
    return run_id, runs


def monitor(s3, glue, run_id, once, poll_seconds):
    while True:
        run = glue.get_job_run(JobName=JOB, RunId=run_id)["JobRun"]
        status = read_remote_json(s3, OUTPUT + "/status.json") or {}
        cost = charged_dpu_seconds(run) * DPU_HOUR_USD / 3600
        emit("job_progress", run_id=run_id, state=run["JobRunState"],
             execution_seconds=run.get("ExecutionTime"),
             estimated_compute_usd=round(cost, 3),
             phase=status.get("phase", "starting"),
             shards_complete=status.get("source_files_complete", 0),
             shards_total=status.get("source_files_total", 128),
             raw_parent_edge_rows=status.get("raw_parent_edge_rows"),
             eta_seconds=status.get("eta_seconds"),
             error=run.get("ErrorMessage"))
        write_json(LOCAL / "job-run-latest.json", run)
        if not once and run["JobRunState"] not in ACTIVE:
            completed = read_remote_json(s3, OUTPUT + "/complete.json")
            emit("job_finished", run_id=run_id, state=run["JobRunState"],
                 output_complete=completed is not None,
                 d1_new_frontier_count=(completed or {}).get("d1_new_frontier_count"))
            if run["JobRunState"] == "SUCCEEDED" and completed is None:
                raise RuntimeError("Glue succeeded without a D1 completion marker")
        if once or run["JobRunState"] not in ACTIVE:
            return run["JobRunState"]
        time.sleep(poll_seconds)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--launch", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=int, default=60)
    args = parser.parse_args()
    if args.poll_seconds < 10:
        parser.error("poll interval must be at least 10 seconds")
    LOCAL.mkdir(parents=True, exist_ok=True)
    session = aws_session_from_cli_login()
    client_config = Config(retries={"mode": "standard", "total_max_attempts": 5},
                           connect_timeout=10, read_timeout=90)
    s3 = session.client("s3", config=client_config)
    glue = session.client("glue", config=client_config)
    config = build_config(s3)
    write_json(LOCAL / "config.json", config)
    config_uri, script_uri = ensure_deployed(s3, glue, config)
    emit("deployment_verified", config_id=config["config_id"], script_uri=script_uri,
         compute_ceiling_usd=COMPUTE_BUDGET_USD,
         max_first_run_compute_usd=round(WORKERS * TIMEOUT_MINUTES *
                                         DPU_HOUR_USD / 60, 2))
    run_id, _ = submit_or_attach(s3, glue, config, config_uri, args.launch, args.resume)
    if run_id:
        monitor(s3, glue, run_id, args.once, args.poll_seconds)


if __name__ == "__main__":
    try:
        main()
    except (ClientError, RuntimeError, ValueError) as error:
        print("D0 parent scan controller error: {}".format(error), file=sys.stderr)
        sys.exit(1)
