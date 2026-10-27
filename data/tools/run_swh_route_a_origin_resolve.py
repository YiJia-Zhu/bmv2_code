#!/usr/bin/env python3
"""Resolve the eight-directory graph sample to archived origin URLs."""

import argparse
from collections import defaultdict, deque
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import time

import boto3
import botocore.session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import BotoCoreError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_full import Budget, digest_json, put_parquet
from run_swh_route_a_sample import GRAPH_PREFIX, sha256, utc_now, write_json


ROOT = Path('/mnt/8T/xgr/swh-graph-poc-20260604/route-a-revision-origin-v1')
OUTPUT_URI = 's3://swh2p4/new-account-588301174757/route-a/revision-origin-v1/'
SOURCE_KEY = GRAPH_PREFIX + 'graph.node2swhid.bin'
NODES = 58675740295
ORIGIN_SCHEMA = pa.schema([('origin_node_id', pa.int64()), ('origin_id', pa.string())])
EVIDENCE_SCHEMA = pa.schema([
    ('seed_directory_id', pa.string()), ('seed_node_id', pa.int64()),
    ('root_directory_node_id', pa.int64()), ('candidate_revision_node_id', pa.int64()),
    ('origin_node_id', pa.int64()), ('origin_id', pa.string()), ('origin_url', pa.string()),
    ('graph_path_nodes', pa.list_(pa.int64())), ('directory_hops', pa.int32()),
    ('status', pa.string()),
])


def authenticated_session():
    session = boto3.Session(region_name='us-east-1')
    if session.get_credentials() is not None:
        return session

    def refresh():
        result = subprocess.run(
            ['aws', 'configure', 'export-credentials', '--format', 'process'],
            capture_output=True, text=True, check=True, timeout=30)
        credentials = json.loads(result.stdout)
        return {'access_key': credentials['AccessKeyId'],
                'secret_key': credentials['SecretAccessKey'],
                'token': credentials.get('SessionToken'),
                'expiry_time': credentials['Expiration']}

    raw = botocore.session.get_session()
    credentials = RefreshableCredentials.create_from_metadata(
        metadata=refresh(), refresh_using=refresh, method='aws-cli-login')
    credentials._advisory_refresh_timeout = 60
    credentials._mandatory_refresh_timeout = 30
    raw._credentials = credentials
    return boto3.Session(botocore_session=raw, region_name='us-east-1')


def shortest_origin_paths(edges, entries):
    adjacency = defaultdict(set)
    for row in edges:
        adjacency[row['source_node_id']].add((row['target_node_id'], row['target_type']))
    result = []
    for entry in entries:
        start = entry['candidate_revision_node_id']
        queue = deque([start])
        predecessor = {start: None}
        while queue:
            source = queue.popleft()
            for target, kind in sorted(adjacency[source]):
                if target in predecessor:
                    continue
                predecessor[target] = source
                if kind in (3, 4, 5):
                    queue.append(target)
        for origin in sorted(node for node in predecessor if node != start and
                             any(kind == 2 and target == node for source in predecessor
                                 for target, kind in adjacency[source])):
            path = []
            current = origin
            while current is not None:
                path.append(current)
                current = predecessor[current]
            result.append(dict(entry, origin_node_id=origin, graph_path_nodes=list(reversed(path))))
    return result


class Resolver:
    def __init__(self, args):
        self.args = args
        self.root = args.root
        self.output = self.root / 'sample8'
        self.control = json.loads((self.output / 'control.json').read_text())
        self.deadline = dt.datetime.fromisoformat(self.control['deadline_utc'])
        if self.control['aws_budget_usd'] > args.aws_budget_usd:
            raise RuntimeError('Cannot lower the saved task budget below the authorized run')
        self.budget = Budget(self.output / 'budget.json', args.aws_budget_usd)
        self.public = boto3.Session(region_name='us-east-1').client(
            's3', endpoint_url='https://s3.amazonaws.com',
            config=Config(signature_version=UNSIGNED, proxies={},
                          s3={'addressing_style': 'virtual'},
                          retries={'total_max_attempts': 1},
                          connect_timeout=8, read_timeout=20))
        self.session = None
        self.started = time.monotonic()

    def check(self):
        if dt.datetime.now(dt.timezone.utc) >= self.deadline:
            raise TimeoutError('Original sample deadline reached')

    def emit(self, event, **fields):
        row = dict(fields, event=event, timestamp_utc=utc_now(),
                   elapsed_seconds=round(time.monotonic() - self.started, 3))
        print(json.dumps(row, sort_keys=True), flush=True)
        with (self.output / 'events.jsonl').open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(row, sort_keys=True) + '\n')
        write_json(self.output / 'resolve-status.json', row)

    def map_origins(self, nodes):
        baseline = json.loads((self.root.parent /
            'route-a-d0-d1-v1/full-d0-d1-v1/identity.json').read_text())
        expected = baseline['mapping_objects']['graph.node2swhid.bin']
        self.budget.reserve(1)
        response = self.public.head_object(Bucket='softwareheritage', Key=SOURCE_KEY)
        if response['ContentLength'] != NODES * 22 or response['ETag'] != expected['etag']:
            raise RuntimeError('Public node mapping object changed')
        cache_path = self.output / 'origin_nodes.json'
        saved = json.loads(cache_path.read_text()) if cache_path.exists() else {}
        if saved.get('source_etag', expected['etag']) != expected['etag']:
            raise RuntimeError('Origin cache source changed')
        records = saved.get('records', {})
        for number, node in enumerate(nodes, 1):
            self.check()
            if str(node) in records:
                continue
            if node < 0 or node >= NODES:
                raise RuntimeError('Origin node out of bounds')
            start = node * 22
            self.budget.reserve(3, 66)
            last_error = None
            for attempt in range(3):
                try:
                    response = self.public.get_object(
                        Bucket='softwareheritage', Key=SOURCE_KEY,
                        Range='bytes={}-{}'.format(start, start + 21), IfMatch=expected['etag'])
                    with response['Body'] as body:
                        if (response['ResponseMetadata']['HTTPStatusCode'] != 206
                                or response['ContentLength'] != 22
                                or response['ContentRange'] !=
                                'bytes {}-{}/{}'.format(start, start + 21, NODES * 22)):
                            raise RuntimeError('Invalid public S3 Range response')
                        data = body.read(23)
                    if len(data) != 22 or data[:2] != b'\x01\x02':
                        raise RuntimeError('Origin node type or record length mismatch')
                    records[str(node)] = data[2:].hex()
                    write_json(cache_path, {'source_etag': expected['etag'], 'records': records})
                    self.emit('origin_id_progress', mapped=len(records), total=len(nodes))
                    break
                except (BotoCoreError, ClientError, OSError) as error:
                    last_error = error
                    if attempt < 2:
                        time.sleep(0.25 * (2 ** attempt))
            else:
                raise RuntimeError('Origin mapping range failed') from last_error
        return {int(node): value for node, value in records.items() if int(node) in nodes}

    def clients(self):
        if self.session is None:
            self.session = authenticated_session()
        return (self.session.client('athena', config=Config(retries={
                    'mode': 'standard', 'total_max_attempts': 3})),
                self.session.client('s3', config=Config(retries={
                    'mode': 'standard', 'total_max_attempts': 3})))

    def athena_query(self, name, sql, database):
        athena, _ = self.clients()
        digest = hashlib.sha256((database + '\n' + sql).encode()).hexdigest()
        state_path = self.output / ('athena-' + name + '.json')
        state = json.loads(state_path.read_text()) if state_path.exists() else {}
        if state and state['sql_sha256'] != digest:
            raise RuntimeError('Athena query changed during resume')
        if not state:
            self.check()
            if name == 'urls':
                budget_path = self.output / 'athena-budget.json'
                budget = json.loads(budget_path.read_text()) if budget_path.exists() else {
                    'limit_usd': self.args.aws_budget_usd,
                    'reserved_usd': 0, 'model': 'Reserve $1 for <=16 GiB origin ORC scan'}
                if budget['reserved_usd'] + 1 > budget['limit_usd']:
                    raise RuntimeError('Athena task budget exceeded')
                budget['reserved_usd'] += 1
                write_json(budget_path, budget)
            response = athena.start_query_execution(
                QueryString=sql, ClientRequestToken=digest,
                QueryExecutionContext={'Database': database}, WorkGroup='primary',
                ResultConfiguration={'OutputLocation': OUTPUT_URI + 'athena-results/',
                                     'ExpectedBucketOwner': '728516300718'})
            state = {'sql_sha256': digest, 'execution_id': response['QueryExecutionId'],
                     'submitted_at_utc': utc_now()}
            write_json(state_path, state)
        while True:
            self.check()
            response = athena.get_query_execution(QueryExecutionId=state['execution_id'])['QueryExecution']
            status = response['Status']['State']
            if status in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
                state.update(status=status, completed_at_utc=utc_now(),
                             scanned_bytes=response.get('Statistics', {}).get('DataScannedInBytes', 0),
                             reason=response['Status'].get('StateChangeReason'))
                write_json(state_path, state)
                self.emit('athena_query_done', name=name, **state)
                if status != 'SUCCEEDED':
                    raise RuntimeError('Athena {} query {}'.format(name, status))
                return state['execution_id']
            time.sleep(2)

    def resolve_urls(self, ids):
        self.athena_query('database', 'CREATE DATABASE IF NOT EXISTS swh_p4_20260604', 'default')
        self.athena_query('table',
            "CREATE EXTERNAL TABLE IF NOT EXISTS swh_p4_20260604.origin_20260604 "
            "(id string, url string) STORED AS ORC LOCATION "
            "'s3://softwareheritage/graph/2026-06-04/orc/origin/'",
            'swh_p4_20260604')
        quoted = ','.join("'{}'".format(value) for value in sorted(set(ids)))
        query = ('SELECT id, url FROM swh_p4_20260604.origin_20260604 '
                 'WHERE id IN ({})'.format(quoted))
        query_id = self.athena_query('urls', query, 'swh_p4_20260604')
        athena, _ = self.clients()
        urls = defaultdict(set)
        paginator = athena.get_paginator('get_query_results')
        for page in paginator.paginate(QueryExecutionId=query_id):
            for row in page['ResultSet']['Rows']:
                values = [cell.get('VarCharValue', '') for cell in row['Data']]
                if len(values) == 2 and values[0] in ids:
                    urls[values[0]].add(values[1])
        return {key: sorted(values) for key, values in urls.items()}

    def archive(self, paths):
        _, s3 = self.clients()
        prefix = OUTPUT_URI.split('s3://swh2p4/', 1)[1] + 'sample8/'
        entries = []
        for path in paths:
            self.check()
            key = prefix + path.name
            digest = sha256(path)
            s3.upload_file(str(path), 'swh2p4', key, ExtraArgs={
                'ChecksumAlgorithm': 'SHA256'})
            head = s3.head_object(Bucket='swh2p4', Key=key,
                                  ExpectedBucketOwner='728516300718')
            if head['ContentLength'] != path.stat().st_size:
                raise RuntimeError('Archive size mismatch: {}'.format(path))
            entries.append({'key': key, 'bytes': path.stat().st_size, 'sha256': digest})
            self.emit('archive_progress', uploaded=len(entries), total=len(paths))
        return entries

    def run(self):
        self.check()
        sample = json.loads((self.output / 'complete.json').read_text())
        if sample['status'] != 'passed' or sample['pending_nodes'] != 0:
            raise RuntimeError('Eight-directory traversal incomplete')
        edges_path = self.output / 'traversal_edges.parquet'
        if sha256(edges_path) != sample['edge_file_sha256']:
            raise RuntimeError('Graph traversal edges changed')
        entries = pq.read_table(self.output / 'seed_revision.parquet').to_pylist()
        paths = shortest_origin_paths(pq.read_table(edges_path).to_pylist(), entries)
        origin_nodes = sorted({row['origin_node_id'] for row in paths})
        if len(origin_nodes) != sample['origin_nodes']:
            raise RuntimeError('Some origin has no seed evidence')
        self.emit('paths_ready', seed_origin_pairs=len(paths), origin_nodes=len(origin_nodes))
        ids = self.map_origins(origin_nodes)
        put_parquet(self.output / 'origin_nodes.parquet', [
            {'origin_node_id': node, 'origin_id': ids[node]} for node in origin_nodes], ORIGIN_SCHEMA)
        urls = self.resolve_urls(set(ids.values()))
        evidence = []
        for row in paths:
            origin_id = ids[row['origin_node_id']]
            matches = urls.get(origin_id, [])
            if not matches:
                evidence.append(dict(row, origin_id=origin_id, origin_url=None,
                                     status='url_unresolved'))
            else:
                evidence.extend(dict(row, origin_id=origin_id, origin_url=url,
                                     status='mapped') for url in matches)
        evidence.sort(key=lambda x: (x['seed_directory_id'], x['origin_node_id'],
                                     x['origin_url'] or ''))
        put_parquet(self.output / 'seed_origin.parquet', evidence, EVIDENCE_SCHEMA)
        repository_urls = sorted({row['origin_url'] for row in evidence if row['origin_url']})
        url_path = self.output / 'repository_urls.txt'
        url_path.write_text(''.join(url + '\n' for url in repository_urls), encoding='utf-8')
        summary = {'status': 'passed' if all(row['status'] == 'mapped' for row in evidence)
                   else 'url_unresolved', 'completed_at_utc': utc_now(),
                   'seed_origin_rows': len(evidence), 'origin_nodes': len(origin_nodes),
                   'unique_urls': len(repository_urls),
                   'mapped_seeds': len({row['seed_directory_id'] for row in evidence
                                        if row['status'] == 'mapped'}),
                   'aws_budget_limit_usd': self.args.aws_budget_usd,
                   's3_reserve_usd': 10,
                   'athena_scanned_bytes': json.loads((self.output / 'athena-urls.json').read_text())['scanned_bytes'],
                   'range_budget_reserved_usd': self.budget.cost(),
                   'files': []}
        files = [self.output / name for name in (
            'seed_revision.parquet', 'traversal_edges.parquet', 'origin_nodes.json',
            'origin_nodes.parquet', 'seed_origin.parquet', 'repository_urls.txt',
            'control.json', 'budget.json', 'athena-budget.json',
            'athena-database.json', 'athena-table.json', 'athena-urls.json',
            'identity.json', 'complete.json', 'events.jsonl', 'run.log',
            'reader.stderr.log', 'exit-code.txt') if (self.output / name).exists()]
        summary['files'] = [{'name': path.name, 'sha256': sha256(path),
                             'bytes': path.stat().st_size} for path in files]
        write_json(self.output / 'url-complete.json', summary)
        archive = self.archive(files + [self.output / 'url-complete.json'])
        write_json(self.output / 'archive.json', {'status': 'passed', 'objects': archive,
                                                  'verified_at_utc': utc_now()})
        self.emit('run_complete', **{key: value for key, value in summary.items() if key != 'files'})
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--aws-budget-usd', type=float, default=100)
    args = parser.parse_args()
    runner = Resolver(args)
    try:
        runner.run()
    except BaseException as error:
        runner.emit('run_interrupted', error_type=type(error).__name__, error=str(error))
        raise


if __name__ == '__main__':
    main()
