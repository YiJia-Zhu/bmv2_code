#!/usr/bin/env python3
"""Merge verified Route A directory edges and cached revision adjacency."""

import argparse
import json
from pathlib import Path
import time

import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_full import digest_json, put_parquet
from run_swh_route_a_sample import sha256, utc_now, write_json


GRAPH_ROOT = Path('/mnt/8T/xgr/swh-graph-poc-20260604')
VERSION = 'route-a-revision-merge-v1'
PARENT_SCHEMA = pa.schema([('child_node_id', pa.int64()), ('parent_node_id', pa.int64())])
REV_SCHEMA = pa.schema([('directory_node_id', pa.int64()), ('revision_node_id', pa.int64())])
SEED_SCHEMA = pa.schema([('seed_directory_id', pa.string()), ('seed_node_id', pa.int64())])


def validate_file(path, expected):
    if (not path.is_file() or path.stat().st_size != expected['bytes']
            or sha256(path) != expected['sha256']):
        raise RuntimeError('Archived input changed: {}'.format(path))


def validate_archives(roots):
    for root in roots:
        archive = json.loads((root / 'archive.json').read_text())
        verified = archive.get('all_sha256_verified', all(
            item.get('remote_checksum_verified', False) for item in archive['objects']))
        if archive['status'] != 'passed' or not verified:
            raise RuntimeError('Source archive not verified: {}'.format(root))
        prefix = archive.get('prefix', archive.get('destination', '')).replace('s3://swh2p4/', '')
        for entry in archive['objects']:
            key = entry['key']
            if not key.startswith(prefix):
                raise RuntimeError('Unexpected source archive key')
            if key.endswith('/archive-manifest.json') and not (root / 'archive-manifest.json').exists():
                continue
            validate_file(root / key[len(prefix):], entry)


def validate_layer(root, number):
    layer = root / 'layers' / 'd{:02d}'.format(number)
    marker = json.loads((layer / 'complete.json').read_text())
    if marker['status'] != 'passed' or marker['layer'] != number:
        raise RuntimeError('Incomplete directory layer {}'.format(number))
    for entry in marker['files']:
        path = layer / entry['path']
        validate_file(path, entry)
        if pq.read_metadata(path).num_rows != entry['rows']:
            raise RuntimeError('Directory layer row count mismatch: {}'.format(path))
    return layer


def merge(graph_root, output):
    started = time.monotonic()
    d0 = graph_root / 'route-a-d0-d1-v1/full-d0-d1-v1'
    d8 = graph_root / 'route-a-d2-d8-v1'
    d16 = graph_root / 'route-a-d9-d16-v1'
    roots = (d0, d8, d16)
    validate_archives(roots)
    complete = json.loads((d16 / 'complete.json').read_text())
    if (complete['status'] != 'passed' or complete['unique_directory_nodes'] != 523435
            or complete['expanded_nodes'] != 523435 or complete['boundary_nodes'] != 0):
        raise RuntimeError('Directory traversal is not complete')
    identity = {'version': VERSION, 'sources': {
        str(root): sha256(root / 'archive.json') for root in roots},
        'directory_total_sha256': sha256(d16 / 'all_directory_nodes.parquet')}
    output.mkdir(parents=True, exist_ok=True)
    identity_path = output / 'identity.json'
    if identity_path.exists() and json.loads(identity_path.read_text()) != identity:
        raise RuntimeError('Merge output belongs to a different source')
    write_json(identity_path, identity)
    done = output / 'complete.json'
    if done.exists():
        summary = json.loads(done.read_text())
        if summary['identity'] != digest_json(identity):
            raise RuntimeError('Merge checkpoint identity changed')
        for item in summary['files']:
            path = output / item['path']
            validate_file(path, item)
            if pq.read_metadata(path).num_rows != item['rows']:
                raise RuntimeError('Merge checkpoint rows changed')
        return summary
    directories = pq.read_table(d16 / 'all_directory_nodes.parquet')
    all_nodes = set(directories.column('node_id').to_pylist())
    if len(all_nodes) != 523435:
        raise RuntimeError('Directory node duplicates')
    known = pq.read_table(d16 / 'known_directory_ids.parquet')
    known_ids = dict(zip(known.column('directory_id').to_pylist(), known.column('node_id').to_pylist()))
    seed_table = pq.read_table(d0 / 'results/d0_node_mapping.parquet')
    seeds = seed_table.to_pylist()
    if len(seeds) != 65321 or len({row['node_id'] for row in seeds}) != len(seeds):
        raise RuntimeError('D0 seed mapping invalid')
    parents = set()
    revs = set()
    d0_parents = set()
    shard_files = sorted((d0 / 'shards/node_edges').glob('part-*.parquet'))
    if not shard_files:
        raise RuntimeError('Missing D0 node edge checkpoints')
    d0_count = 0
    for shard in shard_files:
        marker = json.loads(shard.with_suffix('.complete.json').read_text())
        if marker['identity'] != json.loads((d0 / 'complete.json').read_text())['identity']:
            raise RuntimeError('D0 checkpoint identity mismatch')
        if sha256(shard) != marker['sha256'] or pq.read_metadata(shard).num_rows != marker['rows']:
            raise RuntimeError('D0 checkpoint mismatch: {}'.format(shard))
        for batch in pq.ParquetFile(shard).iter_batches(batch_size=512):
            for row in batch.to_pylist():
                d0_count += 1
                child = row['node_id']
                if child not in all_nodes or known_ids.get(row['directory_id']) != child:
                    raise RuntimeError('D0 node mapping mismatch')
                for parent in row['parent_nodes']:
                    parents.add((child, parent))
                    d0_parents.add((child, parent))
                for revision in row['revision_nodes']:
                    revs.add((child, revision))
    if d0_count != 65321 or len(d0_parents) != 345515:
        raise RuntimeError('D0 edge count mismatch')
    baseline = pq.read_table(d0 / 'results/parent_pairs.parquet').to_pylist()
    baseline_pairs = {(known_ids[row['child_id']], known_ids[row['parent_id']]) for row in baseline}
    if baseline_pairs != d0_parents:
        raise RuntimeError('D0 parent hash/node results disagree')
    print(json.dumps({'event': 'd0_verified', 'timestamp_utc': utc_now(),
                      'directory_nodes': d0_count, 'parent_edges': len(d0_parents),
                      'revision_edges': len(revs)}), flush=True)
    for depth in range(2, 17):
        root = d8 if depth <= 8 else d16
        layer = validate_layer(root, depth)
        for row in pq.read_table(layer / 'parent_edges.parquet').to_pylist():
            parents.add((row['child_node_id'], row['parent_node_id']))
        for row in pq.read_table(layer / 'revision_neighbors.parquet').to_pylist():
            revs.add((row['directory_node_id'], row['revision_node_id']))
        print(json.dumps({'event': 'layer_merged', 'timestamp_utc': utc_now(),
                          'layer': depth, 'parent_edges_total': len(parents),
                          'revision_edges_total': len(revs)}), flush=True)
    if any(child not in all_nodes or parent not in all_nodes for child, parent in parents):
        raise RuntimeError('Parent edge has unknown directory')
    if any(directory not in all_nodes for directory, _ in revs):
        raise RuntimeError('Revision edge has unknown directory')
    files = [
        ('directory_nodes.parquet', directories.to_pylist(), directories.schema),
        ('seed_nodes.parquet', [{'seed_directory_id': row['directory_id'], 'seed_node_id': row['node_id']}
                                for row in seeds], SEED_SCHEMA),
        ('parent_edges.parquet', [{'child_node_id': a, 'parent_node_id': b} for a, b in sorted(parents)],
         PARENT_SCHEMA),
        ('directory_revision.parquet', [{'directory_node_id': a, 'revision_node_id': b}
                                        for a, b in sorted(revs)], REV_SCHEMA),
    ]
    metadata = []
    for name, rows, schema in files:
        path = output / name
        put_parquet(path, rows, schema)
        metadata.append({'path': name, 'bytes': path.stat().st_size,
                         'sha256': sha256(path), 'rows': len(rows)})
    summary = {'status': 'passed', 'identity': digest_json(identity),
               'completed_at_utc': utc_now(), 'directory_nodes': len(all_nodes),
               'seed_nodes': len(seeds), 'parent_edges': len(parents),
               'directory_revision_edges': len(revs),
               'directories_with_revision': len({a for a, _ in revs}),
               'unique_revision_nodes': len({b for _, b in revs}),
               'elapsed_seconds': round(time.monotonic() - started, 3), 'files': metadata,
               'aws_compute_jobs': 0, 'network_requests': 0}
    write_json(done, summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--graph-root', type=Path, default=GRAPH_ROOT)
    parser.add_argument('--output', type=Path,
                        default=GRAPH_ROOT / 'route-a-revision-origin-v1/merge')
    args = parser.parse_args()
    print(json.dumps(merge(args.graph_root, args.output), sort_keys=True), flush=True)


if __name__ == '__main__':
    main()
