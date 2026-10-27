import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from run_swh_route_a_extend import ExtensionRunner, source_state
from run_swh_route_a_full import Checkpoints, digest_json, put_parquet


class TinyExtension(ExtensionRunner):
    def __init__(self, args):
        super().__init__(args)
        self.queries = []

    def prepare(self):
        self.source_depth = 2
        self.source_complete = {'run_identity': 'earlier-fixture', 'unique_directory_nodes': 3}
        self.depths = {1: 0, 2: 1, 3: 2}
        self.expanded = {1, 2}
        self.frontier = [3]
        self.identity = digest_json({'extension_fixture': 1})
        self.checkpoints = Checkpoints(self.output / 'shards', self.identity)

    def archive_request(self, root, summary):
        pass

    def query(self, nodes):
        self.queries.extend(nodes)
        graph = {3: [2, 4], 4: [5], 5: []}
        return [{'node_id': node, 'parent_nodes': graph[node], 'revision_nodes': [],
                 'outdegree': len(graph[node]),
                 'neighbor_type_counts': [0, len(graph[node]), 0, 0, 0, 0]} for node in nodes]


class ExtensionTests(unittest.TestCase):
    def fixture(self, root):
        args = SimpleNamespace(output=Path(root), source=Path(root) / 'prior', max_depth=5,
                               max_hours=1, batch_size=1,
                               s3_prefix='new-account-588301174757/route-a/extension-test/')
        runner = TinyExtension(args)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                runner.run()
            return runner.queries
        finally:
            runner.close()

    def test_continuation_starts_at_boundary_and_retains_old_parent_edges(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(self.fixture(folder), [3, 4, 5])
            root = Path(folder)
            edges = pq.read_table(root / 'layers/d03/parent_edges.parquet').to_pylist()
            self.assertEqual({(row['child_node_id'], row['parent_node_id']) for row in edges}, {(3, 2), (3, 4)})
            result = json.loads((root / 'complete.json').read_text())
            self.assertEqual(result['new_directory_nodes'], 2)
            self.assertEqual(result['unique_directory_nodes'], 5)
            self.assertFalse(result['depth_limit_reached'])
            self.assertFalse(result['revision_origin_mapping_completed'])

    def test_extension_resume_reuses_committed_layers(self):
        with tempfile.TemporaryDirectory() as folder:
            self.fixture(folder)
            self.assertEqual(self.fixture(folder), [])
            (Path(folder) / 'layers/d04/complete.json').unlink()
            self.assertEqual(self.fixture(folder), [])

    def test_source_boundary_must_match_saved_discovery_depth(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            put_parquet(root / 'all_directory_nodes.parquet',
                        [{'node_id': 1, 'minimum_depth': 0}, {'node_id': 2, 'minimum_depth': 1},
                         {'node_id': 3, 'minimum_depth': 2}],
                        pa.schema([('node_id', pa.int64()), ('minimum_depth', pa.int32())]))
            schema = pa.schema([('node_id', pa.int64())])
            target = root / 'layers/d02/frontier.parquet'
            put_parquet(target, [{'node_id': 3}], schema)
            summary = {'max_depth': 2, 'unique_directory_nodes': 3, 'boundary_nodes': 1, 'expanded_nodes': 2}
            depths, expanded, frontier = source_state(root, summary)
            self.assertEqual(expanded, {1, 2})
            self.assertEqual(frontier, [3])
            put_parquet(target, [{'node_id': 2}], schema)
            with self.assertRaisesRegex(RuntimeError, 'disagree'):
                source_state(root, summary)


if __name__ == '__main__':
    unittest.main()
