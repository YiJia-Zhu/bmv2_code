#!/usr/bin/env python3
"""Submit once / reattach to the complete eight-directory Glue probe."""

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import time

from run_swh_glue_parent_probe import aws_json, write_json


ROOT = Path(__file__).resolve().parents[2]
LOCAL = ROOT / "data/raw/swh_p4/discovery/probe8_spark_v1"
JOB = "swh-p4-probe8-complete-v1"
OUTPUT = "s3://swh2p4/glue-results/swh-p4-20260604/probe8-complete-v1"
ROLE = "arn:aws:iam::728516300718:role/service-role/AWSGlueServiceRole"
PAIRS = [
    ("294ca7131a98ab63083208d45d2b85f81208154b", "4c63ecf4a6d16cc3387aeaa0edbe5d85b561f941"),
    ("488727636c23225350dce8ada7d07e81d0d0628e", "40412d7df30fec2414adf910f56c09718798a267"),
    ("48a1e22a0b4effac0bba896fe776441cb03699b0", "64989d28863ddfb740e01b6b5ce7179ebaf93e56"),
    ("5275b1cc402040045fc830d56daf7d1be4b2e7d1", "de1b6b8999930ccf51a772ca2600b8845419048f"),
    ("65e5cf2ed926d31d597e55e556cd5bc70376d759", "22bbe1786e7ab6fb020af80e961ff47caf97c082"),
    ("a03e2602eb9e8de2b12e33c5fe002bb6357a7fbb", "cb0dae1754656bb3e5a602262067ed3b62296cfe"),
    ("eb76378d6665f5549df1b5afa597ce86efebb1fd", "57f47c2384f397e4863f4ae4eff163cbc1a40193"),
    ("0c846910e03e5359c6c68cbb07561c978ac6d02f", None),
]


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def emit(event, **value):
    print(json.dumps(dict(value, event=event, timestamp_utc=now()), sort_keys=True), flush=True)


def aws(args, check=True):
    return aws_json(args, "us-east-1", check=check, timeout=90)[0]


def upload(path, uri):
    subprocess.run(["aws", "s3", "cp", str(path), uri, "--region", "us-east-1",
                    "--only-show-errors", "--no-cli-pager"], check=True, timeout=120)


def config_and_upload():
    with (LOCAL / "inventory.json").open() as handle:
        inventory = json.load(handle)
    source = ROOT / "data/tools/swh_glue_probe8.py"
    helper = ROOT / "data/tools/swh_glue_graph_mapper.py"
    digest = hashlib.sha256(source.read_bytes() + helper.read_bytes()).hexdigest()
    scripts = "s3://swh2p4/glue-scripts/swh-p4-20260604/probe8-v1/" + digest
    config = {
        "version": "probe8-v1.1", "code_sha256": digest, "inventory": inventory,
        "seeds": [{"directory_id": directory, "candidate_rows": 223218 if revision else 4972}
                  for directory, revision in PAIRS],
        "initial_roots": [{"directory": directory, "id": revision} for directory, revision in PAIRS if revision],
        "initial_root_query": {"query_id": "bfd82ab3-edb2-48d2-ba08-044cc76b1916",
                               "scope": "all_128_revision_ORC_files",
                               "result_s3_uri": "s3://swh2p4/athena-results/swh-p4-20260604/_athena-query-results/bfd82ab3-edb2-48d2-ba08-044cc76b1916.csv"},
        "directory_batch_files": 4, "max_driver_rows": 500000,
        "max_directory_depth": 128, "max_revision_depth": 10000, "max_release_depth": 128,
        "lookup_indexes": {
            "release": {"column": "target", "after_distinct_predicates": 2},
            "revision_history": {"column": "parent_id", "after_distinct_predicates": 1},
            "snapshot_branch": {"column": "target", "after_distinct_predicates": 2},
        },
    }
    path = LOCAL / "config.json"
    write_json(path, config)
    upload(source, scripts + "/swh_glue_probe8.py")
    upload(helper, scripts + "/swh_glue_graph_mapper.py")
    upload(path, OUTPUT + "/inputs/config.json")
    upload(LOCAL / "inventory.json", OUTPUT + "/inputs/inventory.json")
    return config, scripts


def deploy_retry(manifest):
    config = json.loads((LOCAL / "config.json").read_text())
    config_fingerprint = hashlib.sha256(json.dumps(config, sort_keys=True).encode("utf-8")).hexdigest()
    if remote_json("config-fingerprint.json") != {"fingerprint": config_fingerprint}:
        raise RuntimeError("remote configuration differs from local checkpoint configuration")
    source = ROOT / "data/tools/swh_glue_probe8.py"
    helper = ROOT / "data/tools/swh_glue_graph_mapper.py"
    digest = hashlib.sha256(source.read_bytes() + helper.read_bytes()).hexdigest()
    scripts = "s3://swh2p4/glue-scripts/swh-p4-20260604/probe8-v1/" + digest
    upload(source, scripts + "/swh_glue_probe8.py")
    upload(helper, scripts + "/swh_glue_graph_mapper.py")
    job = aws(["glue", "get-job", "--job-name", JOB])["Job"]
    update = {"Role": job["Role"], "GlueVersion": job["GlueVersion"],
              "MaxRetries": job.get("MaxRetries", 0), "WorkerType": job["WorkerType"],
              "NumberOfWorkers": job["NumberOfWorkers"], "Timeout": job["Timeout"],
              "ExecutionProperty": job["ExecutionProperty"],
              "DefaultArguments": job["DefaultArguments"],
              "Command": dict(job["Command"], ScriptLocation=scripts + "/swh_glue_probe8.py")}
    update_path = LOCAL / "job-retry-update.json"
    write_json(update_path, update)
    aws(["glue", "update-job", "--job-name", JOB, "--job-update", "file://" + str(update_path)])
    deployed = aws(["glue", "get-job", "--job-name", JOB])["Job"]
    if deployed["Command"]["ScriptLocation"] != scripts + "/swh_glue_probe8.py":
        raise RuntimeError("Glue job did not bind the updated script")
    manifest["scripts_root"] = scripts
    manifest["deployed_code_sha256"] = digest
    manifest["deployed_at"] = now()
    write_json(LOCAL / "run.json", manifest)
    emit("retry_code_deployed", script=deployed["Command"]["ScriptLocation"], code_sha256=digest)


def remote_json(name):
    target = LOCAL / name
    value = aws(["s3api", "get-object", "--bucket", "swh2p4", "--key",
                 OUTPUT.split("swh2p4/", 1)[1] + "/" + name, str(target)], check=False)
    if value is None:
        return None
    return json.loads(target.read_text())


def collect_logs(run_id, tokens):
    for group in ["/aws-glue/jobs/output", "/aws-glue/jobs/error"]:
        streams = aws(["logs", "describe-log-streams", "--log-group-name", group,
                       "--log-stream-name-prefix", run_id], check=False)
        if streams is None:
            continue
        for stream in streams.get("logStreams", []):
            stream_name = stream["logStreamName"]
            if stream_name != run_id and not stream_name.endswith("-driver"):
                continue
            key = (group, stream_name)
            args = ["logs", "get-log-events", "--log-group-name", group,
                    "--log-stream-name", stream_name, "--start-from-head", "--limit", "1000"]
            if key in tokens:
                args += ["--next-token", tokens[key]]
            response = aws(args, check=False)
            if response is None:
                continue
            tokens[key] = response["nextForwardToken"]
            suffix = group.rsplit("/", 1)[-1]
            with (LOCAL / (stream_name + "-" + suffix + ".log")).open("a") as handle:
                for event in response.get("events", []):
                    handle.write(json.dumps(event) + "\n")
                    message = event["message"].strip()
                    if group.endswith("output") and '"event"' in message:
                        print(message, flush=True)


def download_results():
    for name in ["status.json", "current-scan.json", "coverage.json", "origin-evidence.json", "origin-mapping.csv", "repository-urls.txt"]:
        subprocess.run(["aws", "s3", "cp", OUTPUT + "/" + name, str(LOCAL / name),
                        "--only-show-errors", "--no-cli-pager"], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=90)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--launch", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    LOCAL.mkdir(parents=True, exist_ok=True)
    manifest_path = LOCAL / "run.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    if manifest is None:
        if not args.launch:
            raise RuntimeError("No run exists; pass --launch to submit this authorized probe")
        config, scripts = config_and_upload()
        job, error = aws_json(["glue", "get-job", "--job-name", JOB], "us-east-1", check=False)
        if job is None:
            if "EntityNotFoundException" not in error:
                raise RuntimeError(error)
            spec = {
                "Name": JOB, "Role": ROLE, "GlueVersion": "5.0",
                "Description": "Eight directory probes to evidenced origin URLs, resumable S3 scan batches",
                "Command": {"Name": "glueetl", "PythonVersion": "3", "ScriptLocation": scripts + "/swh_glue_probe8.py"},
                "WorkerType": "G.1X", "NumberOfWorkers": 20, "Timeout": 180, "MaxRetries": 0,
                "ExecutionProperty": {"MaxConcurrentRuns": 1},
                "DefaultArguments": {"--job-language": "python", "--enable-metrics": "true",
                                     "--enable-observability-metrics": "true",
                                     "--enable-continuous-cloudwatch-log": "true",
                                     "--TempDir": "s3://swh2p4/glue-temp/swh-p4-20260604/probe8-v1/"}}
            spec_path = LOCAL / "job-spec.json"
            write_json(spec_path, spec)
            aws(["glue", "create-job", "--cli-input-json", "file://" + str(spec_path)])
        else:
            # Bind the job command explicitly. A run's --scriptLocation argument
            # is not a substitute for updating Command.ScriptLocation.
            spec = {"Role": ROLE, "GlueVersion": "5.0", "MaxRetries": 0,
                    "Command": {"Name": "glueetl", "PythonVersion": "3",
                                "ScriptLocation": scripts + "/swh_glue_probe8.py"},
                    "WorkerType": "G.1X", "NumberOfWorkers": 20, "Timeout": 180,
                    "ExecutionProperty": {"MaxConcurrentRuns": 1}}
            spec_path = LOCAL / "job-update.json"
            write_json(spec_path, spec)
            aws(["glue", "update-job", "--job-name", JOB, "--job-update", "file://" + str(spec_path)])
        manifest = {"created_at": now(), "job_name": JOB, "output_root": OUTPUT,
                    "code_sha256": config["code_sha256"], "scripts_root": scripts,
                    "worker_type": "G.1X", "workers": 20, "timeout_minutes": 180,
                    "estimated_max_compute_usd_per_attempt": 20 * 180 / 60 * 0.44,
                    "attempts": []}
        write_json(manifest_path, manifest)
    runs = aws(["glue", "get-job-runs", "--job-name", JOB, "--max-items", "100"])["JobRuns"]
    matching = [r for r in runs if r.get("Arguments", {}).get("--OUTPUT_ROOT") == OUTPUT]
    run = matching[0] if matching else None
    failed = run and run["JobRunState"] in {"FAILED", "TIMEOUT", "STOPPED", "ERROR", "EXPIRED"}
    if failed and not args.retry_failed:
        emit("previous_run_failed_no_automatic_retry", run_id=run["Id"], state=run["JobRunState"])
        download_results()
        return
    if run is None or (failed and args.retry_failed):
        if not args.launch:
            raise RuntimeError("Pass --launch to submit a new attempt")
        if failed:
            deploy_retry(manifest)
        # Save intent before submitting. Re-entry searches Glue by stable OUTPUT_ROOT,
        # including runs whose start response was lost; it never blindly re-submits.
        job_args = {"--CONFIG_URI": OUTPUT + "/inputs/config.json", "--OUTPUT_ROOT": OUTPUT,
                    "--extra-py-files": manifest["scripts_root"] + "/swh_glue_graph_mapper.py"}
        argument_path = LOCAL / "job-arguments.json"
        write_json(argument_path, job_args)
        manifest["submission_intent_at"] = now()
        write_json(manifest_path, manifest)
        response = aws_json(["glue", "start-job-run", "--job-name", JOB,
                            "--worker-type", "G.1X", "--number-of-workers", "20", "--timeout", "180",
                            "--arguments", "file://" + str(argument_path),
                            "--cli-connect-timeout", "10", "--cli-read-timeout", "90"],
                            "us-east-1", timeout=180, attempts=1)[0]
        run_id = response["JobRunId"]
        manifest["attempts"].append({"run_id": run_id, "submitted_at": now()})
        manifest["run_id"] = run_id
        write_json(manifest_path, manifest)
        upload(manifest_path, OUTPUT + "/run-manifest.json")
    else:
        run_id = run["Id"]
        manifest["run_id"] = run_id
        write_json(manifest_path, manifest)
    emit("monitor_started", run_id=run_id, output_root=OUTPUT, max_compute_usd_per_attempt=manifest["estimated_max_compute_usd_per_attempt"])
    tokens = {}
    while True:
        run = aws(["glue", "get-job-run", "--job-name", JOB, "--run-id", run_id])["JobRun"]
        write_json(LOCAL / "job-run-latest.json", run)
        manifest.update(updated_at=now(), glue_state=run["JobRunState"])
        write_json(manifest_path, manifest)
        status = remote_json("status.json")
        current_scan = remote_json("current-scan.json")
        emit("job_state", run_id=run_id, state=run["JobRunState"], execution_seconds=run.get("ExecutionTime"),
             phase=status.get("phase") if status else "starting", mapped=status.get("resolved_seeds") if status else 0,
             current_scan=current_scan,
             estimated_compute_usd=round(run.get("ExecutionTime", 0) * 20 * 0.44 / 3600, 4),
             error=run.get("ErrorMessage"))
        collect_logs(run_id, tokens)
        if run["JobRunState"] not in {"STARTING", "RUNNING", "WAITING", "STOPPING"} or args.once:
            download_results()
            upload(manifest_path, OUTPUT + "/run-manifest.json")
            break
        time.sleep(30)


if __name__ == "__main__":
    main()
