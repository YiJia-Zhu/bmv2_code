#!/usr/bin/env python3
"""Run and resume one bounded AWS Glue parent-directory probe.

The local and S3 manifests record the Glue JobRunId before polling. If the
submission response is lost, a resume discovers the run by its stable probe
ID instead of starting a duplicate billable run.
"""

from __future__ import print_function

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


SCRIPT_VERSION = "1.0.0"
GRAPH_RELEASE = "2026-06-04"
DEFAULT_REGION = "us-east-1"
DEFAULT_JOB_NAME = "swh-p4-parent-probe"
DEFAULT_TARGET_DIRECTORY_ID = "e0b550437f963ed02b8ed2dd546b0a4fcb02696b"
DEFAULT_OUTPUT_BUCKET = "swh2p4"
DEFAULT_OUTPUT_PREFIX = "glue-results/swh-p4-20260604/graph-map-v1"
DEFAULT_SCRIPT_URI = (
    "s3://swh2p4/glue-scripts/swh-p4-20260604/swh_glue_parent_probe.py"
)
DEFAULT_LOCAL_MANIFEST = Path(
    "data/raw/swh_p4/discovery/glue_parent_probe_manifest.json"
)
ACTIVE_GLUE_STATES = {"STARTING", "RUNNING", "STOPPING", "WAITING"}
RETRYABLE_MANIFEST_STATES = {"failed", "timeout", "stopped", "expired"}


class AwsCliError(RuntimeError):
    pass


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(str(temporary), str(path))


def read_json(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def aws_json(arguments, region, check=True, timeout=60, attempts=3):
    command = ["aws"] + list(arguments)
    if "--region" not in command:
        command.extend(["--region", region])
    if "--output" not in command:
        command.extend(["--output", "json"])
    command.append("--no-cli-pager")
    environment = dict(os.environ)
    environment["AWS_PAGER"] = ""
    last_error = ""
    for attempt in range(attempts):
        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            last_error = "{} timed out after {} seconds".format(
                " ".join(command[:3]), timeout
            )
        else:
            if result.returncode == 0:
                output = result.stdout.strip()
                return (json.loads(output) if output else {}), ""
            last_error = result.stderr.strip()
            if not check:
                return None, last_error
        if attempt + 1 < attempts:
            time.sleep(2 ** attempt)
    if check:
        raise AwsCliError(last_error)
    return None, last_error


def validate_directory_id(value):
    if len(value or "") != 40 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("target directory ID must be a lowercase 40-character SHA1")
    return value


def stable_probe_id(target_directory_id):
    value = "{}\n{}\nparent-probe-v1".format(
        GRAPH_RELEASE, validate_directory_id(target_directory_id)
    )
    return "parent-{}".format(hashlib.sha256(value.encode("utf-8")).hexdigest()[:24])


def manifest_key(output_prefix, target_directory_id):
    return "{}/manifests/parent-probe-{}.json".format(
        output_prefix.rstrip("/"), validate_directory_id(target_directory_id)
    )


def run_output_prefix(output_prefix, target_directory_id, job_run_id):
    return "{}/parent-probes/{}/attempts/{}/".format(
        output_prefix.rstrip("/"),
        validate_directory_id(target_directory_id),
        job_run_id,
    )


def run_matches_probe(job_run, probe_id, target_directory_id):
    arguments = job_run.get("Arguments", {})
    return (
        arguments.get("--PROBE_ID") == probe_id
        and arguments.get("--TARGET_DIRECTORY_ID") == target_directory_id
    )


def manifest_status_for_glue_state(state):
    if state in ACTIVE_GLUE_STATES:
        return "running"
    if state == "SUCCEEDED":
        return "job_succeeded"
    if state in {"FAILED", "ERROR"}:
        return "failed"
    if state == "TIMEOUT":
        return "timeout"
    if state == "STOPPED":
        return "stopped"
    if state == "EXPIRED":
        return "expired"
    raise ValueError("unknown Glue job state: {}".format(state))


class Runner(object):
    def __init__(self, args):
        self.args = args
        self.region = args.region
        self.target = validate_directory_id(args.target_directory_id)
        self.probe_id = stable_probe_id(self.target)
        self.output_prefix = args.output_prefix.rstrip("/")
        self.manifest_key = manifest_key(self.output_prefix, self.target)
        self.local_manifest = args.local_manifest
        self.manifest = None

    def s3_uri(self, key):
        return "s3://{}/{}".format(self.args.output_bucket, key.lstrip("/"))

    def new_manifest(self):
        return {
            "schema_version": 1,
            "script_version": SCRIPT_VERSION,
            "graph_release": GRAPH_RELEASE,
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "region": self.region,
            "job_name": self.args.job_name,
            "job_script_s3_uri": self.args.script_s3_uri,
            "target_directory_id": self.target,
            "probe_id": self.probe_id,
            "output_bucket": self.args.output_bucket,
            "output_prefix": self.output_prefix,
            "manifest_s3_uri": self.s3_uri(self.manifest_key),
            "status": "pending",
            "attempts": [],
        }

    def immutable_manifest_fields(self):
        return {
            "graph_release": GRAPH_RELEASE,
            "region": self.region,
            "job_name": self.args.job_name,
            "job_script_s3_uri": self.args.script_s3_uri,
            "target_directory_id": self.target,
            "probe_id": self.probe_id,
            "output_bucket": self.args.output_bucket,
            "output_prefix": self.output_prefix,
        }

    def validate_manifest(self):
        for key, expected in self.immutable_manifest_fields().items():
            if self.manifest.get(key) != expected:
                raise RuntimeError(
                    "manifest {} differs: found {!r}, expected {!r}".format(
                        key, self.manifest.get(key), expected
                    )
                )

    def head_object(self, key):
        value, error = aws_json(
            [
                "s3api",
                "head-object",
                "--bucket",
                self.args.output_bucket,
                "--key",
                key,
            ],
            self.region,
            check=False,
        )
        if value is None:
            lowered = error.lower()
            if "not found" in lowered or "404" in lowered or "nosuchkey" in lowered:
                return None
            raise AwsCliError(error)
        return value

    def download_remote_manifest(self):
        if self.head_object(self.manifest_key) is None:
            return False
        temporary = self.local_manifest.with_name(self.local_manifest.name + ".download")
        temporary.parent.mkdir(parents=True, exist_ok=True)
        aws_json(
            [
                "s3api",
                "get-object",
                "--bucket",
                self.args.output_bucket,
                "--key",
                self.manifest_key,
                str(temporary),
            ],
            self.region,
        )
        os.replace(str(temporary), str(self.local_manifest))
        return True

    def upload_manifest(self):
        aws_json(
            [
                "s3api",
                "put-object",
                "--bucket",
                self.args.output_bucket,
                "--key",
                self.manifest_key,
                "--body",
                str(self.local_manifest),
                "--content-type",
                "application/json",
                "--server-side-encryption",
                "AES256",
                "--checksum-algorithm",
                "SHA256",
            ],
            self.region,
        )

    def persist(self):
        self.manifest["updated_at"] = utc_now()
        write_json(self.local_manifest, self.manifest)
        self.upload_manifest()

    def load_or_create_manifest(self):
        if self.args.resume:
            self.download_remote_manifest()
        if self.local_manifest.is_file():
            self.manifest = read_json(self.local_manifest)
            self.validate_manifest()
        else:
            self.manifest = self.new_manifest()

    def verify_job_definition(self):
        value, _ = aws_json(
            ["glue", "get-job", "--job-name", self.args.job_name], self.region
        )
        job = value["Job"]
        actual = {
            "script": job.get("Command", {}).get("ScriptLocation"),
            "glue_version": job.get("GlueVersion"),
            "worker_type": job.get("WorkerType"),
            "workers": int(job.get("NumberOfWorkers", 0)),
            "timeout": int(job.get("Timeout", 0)),
            "max_retries": int(job.get("MaxRetries", 0)),
        }
        expected = {
            "script": self.args.script_s3_uri,
            "glue_version": "5.0",
            "worker_type": "G.1X",
            "workers": 2,
            "timeout": 15,
            "max_retries": 0,
        }
        if actual != expected:
            raise RuntimeError(
                "Glue job definition is not at the bounded probe configuration: "
                "found {}, expected {}".format(actual, expected)
            )

    def matching_runs(self):
        value, _ = aws_json(
            ["glue", "get-job-runs", "--job-name", self.args.job_name, "--max-results", "200"],
            self.region,
        )
        return [
            run
            for run in value.get("JobRuns", [])
            if run_matches_probe(run, self.probe_id, self.target)
        ]

    def apply_job_run(self, job_run):
        job_run_id = job_run["Id"]
        state = job_run["JobRunState"]
        status = manifest_status_for_glue_state(state)
        attempt = next(
            (
                item
                for item in self.manifest["attempts"]
                if item.get("job_run_id") == job_run_id
            ),
            None,
        )
        if attempt is None:
            attempt = {"job_run_id": job_run_id}
            self.manifest["attempts"].append(attempt)
        attempt.update(
            {
                "glue_state": state,
                "status": status,
                "started_on": job_run.get("StartedOn"),
                "completed_on": job_run.get("CompletedOn"),
                "execution_seconds": job_run.get("ExecutionTime"),
                "dpu_seconds": job_run.get("DPUSeconds"),
                "error_message": job_run.get("ErrorMessage"),
            }
        )
        self.manifest["job_run_id"] = job_run_id
        self.manifest["glue_state"] = state
        self.manifest["status"] = status
        return status

    def recover_matching_run(self):
        runs = self.matching_runs()
        active = [run for run in runs if run.get("JobRunState") in ACTIVE_GLUE_STATES]
        if len(active) > 1:
            raise RuntimeError("multiple active Glue runs match this probe; refusing to submit")
        if active:
            self.apply_job_run(active[0])
            return True
        known_run_id = self.manifest.get("job_run_id")
        if known_run_id:
            known = next((run for run in runs if run.get("Id") == known_run_id), None)
            if known:
                self.apply_job_run(known)
                return True
        if runs and self.manifest.get("status") in {"submitting", "submitted", "running"}:
            self.apply_job_run(runs[0])
            return True
        return False

    def start_job(self):
        self.manifest["status"] = "submitting"
        self.persist()
        arguments = {
            "--TARGET_DIRECTORY_ID": self.target,
            "--OUTPUT_ROOT": self.s3_uri(
                self.output_prefix + "/parent-probes/"
            ),
            "--PROBE_ID": self.probe_id,
        }
        value, _ = aws_json(
            [
                "glue",
                "start-job-run",
                "--job-name",
                self.args.job_name,
                "--arguments",
                json.dumps(arguments, separators=(",", ":")),
            ],
            self.region,
        )
        job_run_id = value["JobRunId"]
        self.manifest["job_run_id"] = job_run_id
        self.manifest["status"] = "submitted"
        self.manifest["attempts"].append(
            {
                "job_run_id": job_run_id,
                "status": "submitted",
                "submitted_at": utc_now(),
            }
        )
        self.persist()

    def get_job_run(self):
        value, _ = aws_json(
            [
                "glue",
                "get-job-run",
                "--job-name",
                self.args.job_name,
                "--run-id",
                self.manifest["job_run_id"],
                "--predecessors-included",
            ],
            self.region,
        )
        return value["JobRun"]

    def verify_success_outputs(self):
        run_prefix = run_output_prefix(
            self.output_prefix, self.target, self.manifest["job_run_id"]
        )
        markers = [run_prefix + "parents/_SUCCESS", run_prefix + "metrics/_SUCCESS"]
        missing = [key for key in markers if self.head_object(key) is None]
        if missing:
            raise RuntimeError(
                "Glue reported success but output completion markers are missing: {}".format(
                    ", ".join(missing)
                )
            )
        self.manifest["status"] = "complete"
        self.manifest["result_prefix"] = self.s3_uri(run_prefix)
        self.manifest["completion_markers"] = [self.s3_uri(key) for key in markers]

    def run(self):
        self.verify_job_definition()
        self.load_or_create_manifest()
        if self.manifest.get("status") == "complete":
            self.verify_success_outputs()
            self.persist()
            print(json.dumps(self.manifest, ensure_ascii=False, indent=2))
            return 0

        recovered = self.recover_matching_run()
        if recovered:
            self.persist()
        if self.manifest.get("status") in RETRYABLE_MANIFEST_STATES:
            if not self.args.retry_failed:
                raise RuntimeError(
                    "previous probe ended as {}; inspect it before --retry-failed".format(
                        self.manifest.get("status")
                    )
                )
            self.manifest["status"] = "pending"
            self.manifest.pop("job_run_id", None)
            self.manifest.pop("glue_state", None)

        if self.manifest.get("status") == "job_succeeded":
            self.verify_success_outputs()
            self.persist()
            print(json.dumps(self.manifest, ensure_ascii=False, indent=2))
            return 0

        if self.manifest.get("status") == "pending":
            self.start_job()

        while self.manifest.get("status") in {"submitted", "running", "submitting"}:
            job_run = self.get_job_run()
            self.apply_job_run(job_run)
            self.persist()
            print(
                "probe {} run {} state={}".format(
                    self.probe_id,
                    self.manifest.get("job_run_id"),
                    self.manifest.get("glue_state"),
                ),
                flush=True,
            )
            if self.manifest.get("status") in {"submitted", "running", "submitting"}:
                time.sleep(self.args.poll_seconds)

        if self.manifest.get("status") == "job_succeeded":
            self.verify_success_outputs()
            self.persist()
        print(json.dumps(self.manifest, ensure_ascii=False, indent=2))
        return 0 if self.manifest.get("status") == "complete" else 2


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default=DEFAULT_REGION)
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--target-directory-id", default=DEFAULT_TARGET_DIRECTORY_ID)
    parser.add_argument("--output-bucket", default=DEFAULT_OUTPUT_BUCKET)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--script-s3-uri", default=DEFAULT_SCRIPT_URI)
    parser.add_argument("--local-manifest", type=Path, default=DEFAULT_LOCAL_MANIFEST)
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--retry-failed", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.poll_seconds < 0:
        raise SystemExit("poll-seconds must be non-negative")
    runner = Runner(args)
    try:
        return runner.run()
    except KeyboardInterrupt:
        if runner.manifest is not None:
            write_json(runner.local_manifest, runner.manifest)
        print("interrupted; rerun with --resume", file=sys.stderr)
        return 130
    except Exception as exc:
        if runner.manifest is not None:
            write_json(runner.local_manifest, runner.manifest)
        print("failed; rerun with --resume after inspection: {}".format(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
