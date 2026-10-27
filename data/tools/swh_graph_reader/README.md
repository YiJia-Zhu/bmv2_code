# Route A reverse-graph reader

This adapter opens the downloaded, unlabelled SWH `graph-transposed` with
WebGraph, hashes directory SWHIDs using `ph::fmph::GOFunction`, and enumerates
one-hop directory parents. It does not rebuild the graph or load either of the
large complete SWHID mapping files. The Python driver
[`run_swh_route_a_sample.py`](../run_swh_route_a_sample.py) reads the necessary
mapping records with bounded S3 Range requests, verifies every input's exact
SWHID roundtrip, and compares 16 selected D0 inputs against route B.

The graph files must be from the same export. Expected filenames:

- `graph-transposed.graph`, `graph-transposed.ef`, `graph-transposed.properties`
- `graph.fmphgo`
- `graph.node2type.bin` (decompressed from `.zst`)

Dependencies are pinned by `Cargo.lock`. Build with the isolated toolchain:

```bash
export RUSTUP_HOME=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-d0-d1-v1/toolchain/rustup
export CARGO_HOME=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-d0-d1-v1/toolchain/cargo
export CARGO_TARGET_DIR=/mnt/8T/xgr/swh-graph-poc-20260604/route-a-d0-d1-v1/build
export PATH="$CARGO_HOME/bin:$PATH"
cargo build --release --locked --jobs 8 --manifest-path data/tools/swh_graph_reader/Cargo.toml
cargo test --release --locked --jobs 8 --manifest-path data/tools/swh_graph_reader/Cargo.toml
```

The sample driver runs under the existing `effthink` Python environment, which
provides boto3 and PyArrow. It reuses verified local baseline downloads and caches
Range records by public object ETag and offset. The AWS CLI login is reused through
the existing S3 credential helper; secrets are never written into output files.

```bash
/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python -u data/tools/run_swh_route_a_sample.py \
  --graph-dir /mnt/8T/xgr/swh-graph-poc-20260604
/mnt/8T/xgr/zhuyijia/envs/effthink/bin/python -m unittest discover \
  -s data/tools -p test_swh_route_a_sample.py -q
```

Run logs and outputs are under `GRAPH_DIR/route-a-d0-d1-v1/`:

- `status.json`, `events.jsonl`, `sample.log`, `reader.stderr.log`
- `run_identity.json`, `mapping_identity.json`, `sample_inputs.json`
- `sample_id_mapping.json`, `sample_node_neighbors.json`, `sample_results.json`
- `sample_parent_pairs.csv`, `comparison_sample.json`

`comparison_sample.json` covers the sample only. Full D0→D1 execution and deeper
traversal are separate stages. Parent pairs preserve multiple parents; an empty
directory-parent list does not prove the node is a repository root. Unlabelled
edges cannot recover filenames or permission bits.

The full D0→D1 driver is
[`run_swh_route_a_full.py`](../run_swh_route_a_full.py), launched by
[`run_swh_route_a_full.sh`](../run_swh_route_a_full.sh). It reuses this binary,
keeps the sample untouched, and writes under `route-a-d0-d1-v1/full-d0-d1-v1/`.
It commits verified D0 mappings, local neighbor batches, and unique-parent
mappings separately. Resume requires the original input/implementation/object
identities and validated output hashes; the original deadline and cumulative
budget reservations persist. See the
[full-run record](../../raw/swh_p4/ROUTE_A_FULL_D1_20261023.md) for limits,
progress commands, output schemas, and comparison criteria.
