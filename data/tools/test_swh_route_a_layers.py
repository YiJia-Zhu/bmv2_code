import contextlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import pyarrow.parquet as pq

from run_swh_route_a_full import Checkpoints, digest_json
from run_swh_route_a_layers import LayerRunner, next_frontier, validate_neighbors


class TinyRunner(LayerRunner):
    def __init__(self, args):
        super().__init__(args)
        self.queries = []

    def prepare(self):
        self.identity = digest_json({'tiny_fixture': 1})
        self.checkpoints = Checkpoints(self.output / 'shards', self.identity)
        self.d0_nodes = {1, 2}
        self.frontier = [3, 4]
        self.depths = {1: 0, 2: 0, 3: 1, 4: 1}
        self.expanded = {1, 2}

    def archive_request(self, root, summary):
        pass

    def query(self, nodes):
        self.queries.extend(nodes)
        graph = {3: ([2, 5], [101]), 4: ([5, 6], []),
                 5: ([7], [102]), 6: ([], [103]), 7: ([], [])}
        result = []
        for node in nodes:
            parents, revisions = graph[node]
            result.append({'node_id': node, 'parent_nodes': parents, 'revision_nodes': revisions,
                           'outdegree': len(parents) + len(revisions),
                           'neighbor_type_counts': [0, len(parents), 0, 0, len(revisions), 0]})
        return result


class DirectoryLayerTests(unittest.TestCase):
    def args(self, root):
        return SimpleNamespace(output=Path(root), max_hours=1, max_depth=3,
                               batch_size=1, s3_prefix='new-account-588301174757/route-a/test/')

    def run_fixture(self, root):
        runner = TinyRunner(self.args(root))
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                runner.run()
            return runner.queries
        finally:
            runner.close()

    def test_shared_parents_existing_nodes_and_boundary(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(self.run_fixture(root), [3, 4, 5, 6])
            base = Path(root)
            edges = pq.read_table(base / 'layers/d02/parent_edges.parquet').to_pylist()
            self.assertEqual({(row['child_node_id'], row['parent_node_id']) for row in edges},
                             {(3, 2), (3, 5), (4, 5), (4, 6)})
            self.assertEqual(pq.read_table(base / 'layers/d02/frontier.parquet').column(0).to_pylist(), [5, 6])
            self.assertEqual(pq.read_table(base / 'layers/d03/frontier.parquet').column(0).to_pylist(), [7])
            self.assertEqual(pq.read_table(base / 'layers/d03/no_directory_parent.parquet').column(0).to_pylist(), [6])
            complete = json.loads((base / 'complete.json').read_text())
            self.assertEqual(complete['unique_directory_nodes'], 7)
            self.assertTrue(complete['depth_limit_reached'])
            self.assertFalse(complete['revision_origin_mapping_completed'])

    def test_completed_layers_and_batches_resume_without_requery(self):
        with tempfile.TemporaryDirectory() as root:
            self.run_fixture(root)
            self.assertEqual(self.run_fixture(root), [])
            (Path(root) / 'layers/d03/complete.json').unlink()
            self.assertEqual(self.run_fixture(root), [])

    def test_corrupt_completed_layer_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            self.run_fixture(root)
            with (Path(root) / 'layers/d02/frontier.parquet').open('ab') as handle:
                handle.write(b'corrupt')
            with self.assertRaisesRegex(RuntimeError, 'corrupt'):
                self.run_fixture(root)

    def test_neighbor_validation_rejects_partial_and_invalid_batches(self):
        with self.assertRaisesRegex(RuntimeError, 'membership'):
            validate_neighbors([1], [])
        with self.assertRaisesRegex(RuntimeError, 'duplicate'):
            validate_neighbors([1], [{'node_id': 1, 'node_type': 1, 'outdegree': 2,
                                      'neighbor_type_counts': [0, 2, 0, 0, 0, 0],
                                      'parent_nodes': [3, 3], 'revision_nodes': []}])
        self.assertEqual(next_frontier([1, 2, 3, 3, 4], {1}, [2]), [3, 4])


if __name__ == '__main__':
    unittest.main()
