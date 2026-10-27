#!/usr/bin/env python3
"""Traverse typed reverse graph from eight verified P4 directory seeds."""

import argparse
from collections import defaultdict, deque
import datetime as dt
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_full import Checkpoints, digest_json, put_parquet
from run_swh_route_a_sample import PROBE_IDS, sha256, utc_now, write_json


ROOT = Path('/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1')
GRAPH_ROOT = ROOT.parent
VERSION = 'route-a-origin-sample-v1'
NODES = 58675740295
CHECKPOINT_SCHEMA = pa.schema([
    ('node_id', pa.int64()), ('node_type', pa.int8()), ('outdegree', pa.int64()),
    ('truncated', pa.bool_()), ('neighbor_type_counts', pa.list_(pa.int64())),
    ('neighbors', pa.list_(pa.struct([('node_id', pa.int64()), ('node_type', pa.int8())]))),
])
EDGE_SCHEMA = pa.schema([('source_node_id', pa.int64()), ('target_node_id', pa.int64()),
                         ('source_type', pa.int8()), ('target_type', pa.int8())])
SEED_REV_SCHEMA = pa.schema([
    ('seed_directory_id', pa.string()), ('seed_node_id', pa.int64()),
    ('root_directory_node_id', pa.int64()), ('candidate_revision_node_id', pa.int64()),
    ('directory_hops', pa.int32()),
])


def sample_entries(merge_root):
    seeds = {row['seed_directory_id']: row['seed_node_id']
             for row in pq.read_table(merge_root / 'seed_nodes.parquet').to_pylist()}
    parents = defaultdict(set)
    revisions = defaultdict(set)
    for row in pq.read_table(merge_root / 'parent_edges.parquet').to_pylist():
        parents[row['child_node_id']].add(row['parent_node_id'])
    for row in pq.read_table(merge_root / 'directory_revision.parquet').to_pylist():
        revisions[row['directory_node_id']].add(row['revision_node_id'])
    rows = []
    for seed_id in PROBE_IDS:
        seed = seeds[seed_id]
        frontier = deque([(seed, 0)])
        seen = {seed}
        while frontier:
            directory, depth = frontier.popleft()
            for revision in revisions[directory]:
                rows.append({'seed_directory_id': seed_id, 'seed_node_id': seed,
                             'root_directory_node_id': directory,
                             'candidate_revision_node_id': revision,
                             'directory_hops': depth})
            for parent in sorted(parents[directory]):
                if parent not in seen:
                    seen.add(parent)
                    frontier.append((parent, depth + 1))
    rows.sort(key=lambda x: (x['seed_directory_id'], x['candidate_revision_node_id']))
    if len({row['seed_directory_id'] for row in rows}) != 8:
        raise RuntimeError('Some sample directory lacks a revision entry')
    return rows


class Sample:
    def __init__(self, args):
        self.args = args
        self.output = args.output
        self.output.mkdir(parents=True, exist_ok=True)
        self.started = time.monotonic()
        control_path = self.output / 'control.json'
        if control_path.exists():
            self.control = json.loads(control_path.read_text())
            if (self.control['max_nodes'] != args.max_nodes or
                    self.control['max_edges'] != args.max_edges):
                raise RuntimeError('Cannot change sample limits on resume')
        else:
            now = dt.datetime.now(dt.timezone.utc)
            self.control = {'first_started_at_utc': now.isoformat(),
                            'deadline_utc': (now + dt.timedelta(hours=args.max_hours)).isoformat(),
                            'max_hours': args.max_hours, 'aws_budget_usd': args.aws_budget_usd,
                            's3_reserve_usd': 10, 'max_nodes': args.max_nodes,
                            'max_edges': args.max_edges, 'batch_size': args.batch_size}
            write_json(control_path, self.control)
        self.deadline = dt.datetime.fromisoformat(self.control['deadline_utc'])
        self.state = {'phase': 'preparing', 'started_at_utc': utc_now(),
                      'deadline_utc': self.control['deadline_utc']}
        self.reader = None
        self.stderr = None

    def check(self):
        if dt.datetime.now(dt.timezone.utc) >= self.deadline:
            raise TimeoutError('Original 10-hour deadline reached')

    def emit(self, event, **fields):
        row = dict(fields, event=event, timestamp_utc=utc_now(),
                   elapsed_seconds=round(time.monotonic() - self.started, 3))
        print(json.dumps(row, sort_keys=True), flush=True)
        with (self.output / 'events.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(row, sort_keys=True) + '\n')
        self.state.update(row)
        write_json(self.output / 'status.json', self.state)

    def prepare(self):
        self.check()
        merge_root = self.args.merge
        merge_complete = json.loads((merge_root / 'complete.json').read_text())
        if merge_complete['status'] != 'passed' or merge_complete['seed_nodes'] != 65321:
            raise RuntimeError('Directory/revision merge is incomplete')
        for item in merge_complete['files']:
            path = merge_root / item['path']
            if path.stat().st_size != item['bytes'] or sha256(path) != item['sha256']:
                raise RuntimeError('Merge input changed')
        self.entries = sample_entries(merge_root)
        self.entry_nodes = sorted({row['candidate_revision_node_id'] for row in self.entries})
        if len(self.entry_nodes) != 8:
            raise RuntimeError('Expected eight unique revision entries')
        binary = self.args.binary
        graph_identity = json.loads((self.args.graph / 'route-a-d9-d16-v1/identity.json').read_text())
        for name, metadata in graph_identity['graph_files'].items():
            path = self.args.graph / name
            stat = path.stat()
            if stat.st_size != metadata['size'] or stat.st_mtime_ns != metadata['mtime_ns']:
                raise RuntimeError('Local graph source changed: {}'.format(name))
        identity = {'version': VERSION, 'merge_sha256': sha256(merge_root / 'complete.json'),
                    'binary_sha256': sha256(binary), 'reader_source_sha256': sha256(
                        Path(__file__).parent / 'swh_graph_reader/src/bin/origin.rs'),
                    'driver_sha256': sha256(Path(__file__)),
                    'graph_identity': graph_identity['source_run_identity'],
                    'entries': self.entries, 'batch_size': self.args.batch_size}
        path = self.output / 'identity.json'
        if path.exists() and json.loads(path.read_text()) != identity:
            raise RuntimeError('Sample checkpoint identity changed')
        write_json(path, identity)
        self.identity = digest_json(identity)
        self.checkpoints = Checkpoints(self.output / 'shards', self.identity)
        put_parquet(self.output / 'seed_revision.parquet', self.entries, SEED_REV_SCHEMA)
        self.emit('preflight_complete', seeds=8, entry_revisions=len(self.entry_nodes),
                  aws_compute_jobs=0)

    def start_reader(self):
        self.stderr = (self.output / 'reader.stderr.log').open('a', encoding='utf-8')
        self.reader = subprocess.Popen([str(self.args.binary), str(self.args.graph)],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=self.stderr, text=True, bufsize=1)
        for line in self.reader.stdout:
            value = json.loads(line)
            if value['event'] == 'ready':
                if value['nodes'] != NODES:
                    raise RuntimeError('Reader node count mismatch')
                self.emit('reader_ready', load_seconds=value['load_seconds'])
                return
        raise RuntimeError('Reader exited before ready')

    def query(self, nodes):
        self.reader.stdin.write(json.dumps({'op': 'neighbors', 'nodes': nodes}) + '\n')
        self.reader.stdin.flush()
        line = self.reader.stdout.readline()
        if not line:
            raise RuntimeError('Graph reader exited; inspect stderr')
        response = json.loads(line)
        if response['event'] != 'neighbors' or [r['node_id'] for r in response['results']] != nodes:
            raise RuntimeError('Reader batch mismatch')
        return response['results']

    def run(self):
        self.prepare()
        queue = list(self.entry_nodes)
        seen = set(queue)
        head = 0
        count_edges = 0
        origins = set()
        truncated = set()
        overflow = set()
        unsupported = set()
        all_edges = []
        last_log = 0.0
        batch_number = 0
        stopped_for_limit = False
        while head < len(queue):
            self.check()
            nodes = queue[head:head + self.args.batch_size]
            rows = self.checkpoints.load('nodes', batch_number, nodes)
            if rows is None:
                if self.reader is None:
                    self.start_reader()
                rows = self.query(nodes)
                self.checkpoints.save('nodes', batch_number, nodes, rows, CHECKPOINT_SCHEMA)
            if [row['node_id'] for row in rows] != nodes:
                raise RuntimeError('Checkpoint batch membership changed')
            for row in rows:
                if row['node_type'] not in (3, 4, 5):
                    raise RuntimeError('Scheduled non-revision/release/snapshot node')
                if row['truncated']:
                    truncated.add(row['node_id'])
                    continue
                neighbors = row['neighbors']
                if len(neighbors) != row['outdegree']:
                    raise RuntimeError('Missing non-truncated neighbors')
                if count_edges + len(neighbors) > self.args.max_edges:
                    stopped_for_limit = True
                    break
                count_edges += len(neighbors)
                for target in neighbors:
                    node, kind = target['node_id'], target['node_type']
                    all_edges.append({'source_node_id': row['node_id'], 'target_node_id': node,
                                      'source_type': row['node_type'], 'target_type': kind})
                    if kind == 2:
                        origins.add(node)
                    elif kind in (3, 4, 5):
                        if node not in seen:
                            seen.add(node)
                            if len(seen) <= self.args.max_nodes:
                                queue.append(node)
                            else:
                                overflow.add(node)
                    else:
                        unsupported.add(node)
            if stopped_for_limit:
                break
            head += len(nodes)
            batch_number += 1
            if time.monotonic() - last_log > 30 or head == len(queue):
                last_log = time.monotonic()
                self.emit('traversal_progress', queried_nodes=head, queued_nodes=len(queue),
                          pending_nodes=len(queue) - head, edges=count_edges,
                          origins=len(origins), truncated_nodes=len(truncated),
                          overflow_nodes=len(overflow), rss_kib=resource.getrusage(
                              resource.RUSAGE_SELF).ru_maxrss,
                          eta_seconds=None)
        path = self.output / 'traversal_edges.parquet'
        put_parquet(path, all_edges, EDGE_SCHEMA)
        limited = bool(truncated or overflow or stopped_for_limit)
        summary = {'status': 'limited' if limited else 'passed',
                   'identity': self.identity, 'completed_at_utc': utc_now(),
                   'queried_nodes': head, 'queued_nodes': len(queue),
                   'pending_nodes': len(queue) - head, 'edges': count_edges,
                   'origin_nodes': len(origins), 'truncated_nodes': sorted(truncated),
                   'overflow_nodes_count': len(overflow),
                   'edge_limit_reached': stopped_for_limit,
                   'elapsed_seconds': round(time.monotonic() - self.started, 3),
                   'edge_file_sha256': sha256(path), 'reader_peak_rss_kib': resource.getrusage(
                       resource.RUSAGE_CHILDREN).ru_maxrss,
                   'unhandled_target_nodes': sorted(unsupported),
                   'aws_compute_jobs': 0, 'network_requests_by_traversal': 0}
        write_json(self.output / 'complete.json', summary)
        self.emit('traversal_complete', **summary)
        return summary

    def close(self):
        if self.reader is not None:
            if self.reader.poll() is None:
                self.reader.terminate()
            self.reader.wait(timeout=10)
            self.stderr.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--graph', type=Path, default=GRAPH_ROOT)
    parser.add_argument('--merge', type=Path, default=ROOT / 'merge')
    parser.add_argument('--output', type=Path, default=ROOT / 'sample8')
    parser.add_argument('--binary', type=Path, default=ROOT / 'build/release/origin')
    parser.add_argument('--max-hours', type=float, default=10)
    parser.add_argument('--aws-budget-usd', type=float, default=100)
    parser.add_argument('--max-nodes', type=int, default=1000000)
    parser.add_argument('--max-edges', type=int, default=10000000)
    parser.add_argument('--batch-size', type=int, default=16)
    args = parser.parse_args()
    runner = Sample(args)
    try:
        runner.run()
    except BaseException as error:
        runner.emit('run_interrupted', error_type=type(error).__name__, error=str(error))
        raise
    finally:
        runner.close()


if __name__ == '__main__':
    main()
