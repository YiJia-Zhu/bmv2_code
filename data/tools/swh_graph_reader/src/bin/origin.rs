//! Read typed reverse neighbors for revision, release, snapshot, and origin traversal.

use anyhow::{bail, ensure, Context, Result};
use dsi_bitstream::prelude::BE;
use memmap2::{Mmap, MmapOptions};
use serde_json::{json, Value};
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::Instant;
use webgraph::prelude::*;

const MAX_DEGREE: usize = 100_000;
const MAX_BATCH: usize = 512;

fn node_type(data: &[u8], node: usize, nodes: usize) -> Result<u8> {
    ensure!(node < nodes, "node ID out of bounds: {node}");
    let bit = node.checked_mul(3).context("type offset overflow")?;
    let offset = bit / 64 * 8;
    let shift = bit % 64;
    let mut value = u64::from_le_bytes(data[offset..offset + 8].try_into()?) >> shift;
    if shift > 61 {
        value |= u64::from_le_bytes(data[offset + 8..offset + 16].try_into()?) << (64 - shift);
    }
    let kind = (value & 7) as u8;
    ensure!(kind < 6, "invalid node type {kind}");
    Ok(kind)
}

fn output(value: Value) -> Result<()> {
    let stdout = io::stdout();
    let mut lock = stdout.lock();
    serde_json::to_writer(&mut lock, &value)?;
    writeln!(lock)?;
    lock.flush()?;
    Ok(())
}

fn run() -> Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).context("usage: origin-reader GRAPH_DIR")?);
    let started = Instant::now();
    output(json!({"event":"loading_graph"}))?;
    let graph = BvGraph::with_basename(root.join("graph-transposed"))
        .endianness::<BE>()
        .flags(MemoryFlags::RANDOM_ACCESS)
        .load()
        .context("load graph-transposed")?;
    let nodes = graph.num_nodes();
    let type_file = File::open(root.join("graph.node2type.bin"))?;
    let expected = nodes.checked_mul(3).context("type length overflow")?.div_ceil(64) * 8;
    ensure!(type_file.metadata()?.len() == expected as u64, "node2type length mismatch");
    let types: Mmap = unsafe { MmapOptions::new().map(&type_file)? };
    output(json!({"event":"ready", "nodes":nodes, "arcs":graph.num_arcs(),
                  "load_seconds":started.elapsed().as_secs_f64()}))?;
    for line in io::stdin().lock().lines() {
        let value: Value = serde_json::from_str(&line?)?;
        match value["op"].as_str().context("missing operation")? {
            "neighbors" => {
                let requested = value["nodes"].as_array().context("missing nodes")?;
                ensure!(requested.len() <= MAX_BATCH, "batch exceeds {MAX_BATCH} nodes");
                let mut results = Vec::with_capacity(requested.len());
                for raw in requested {
                    let node = usize::try_from(raw.as_u64().context("invalid node ID")?)?;
                    let kind = node_type(&types, node, nodes)?;
                    ensure!(matches!(kind, 1 | 2 | 3 | 4 | 5), "unexpected content node");
                    let degree = graph.outdegree(node);
                    if degree > MAX_DEGREE {
                        results.push(json!({"node_id":node,"node_type":kind,"outdegree":degree,
                                            "truncated":true,"neighbors":[]}));
                        continue;
                    }
                    let mut neighbors = Vec::with_capacity(degree);
                    let mut counts = [0u64; 6];
                    for adjacent in RandomAccessLabeling::labels(&graph, node) {
                        let adjacent_kind = node_type(&types, adjacent, nodes)?;
                        counts[adjacent_kind as usize] += 1;
                        neighbors.push(json!({"node_id":adjacent,"node_type":adjacent_kind}));
                    }
                    ensure!(counts.iter().sum::<u64>() == degree as u64, "degree mismatch");
                    results.push(json!({"node_id":node,"node_type":kind,"outdegree":degree,
                                        "truncated":false,"neighbor_type_counts":counts,
                                        "neighbors":neighbors}));
                }
                output(json!({"event":"neighbors","results":results}))?;
            }
            "quit" => return Ok(()),
            op => bail!("unknown operation: {op}"),
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::node_type;

    #[test]
    fn packed_types_at_word_boundaries() {
        let mut words = [0u64; 4];
        for node in 0..64 {
            let kind = (node % 6) as u64;
            let bit = node * 3;
            words[bit / 64] |= kind << (bit % 64);
            if bit % 64 > 61 {
                words[bit / 64 + 1] |= kind >> (64 - bit % 64);
            }
        }
        let bytes: Vec<u8> = words.iter().flat_map(|word| word.to_le_bytes()).collect();
        for node in 0..64 {
            assert_eq!(node_type(&bytes, node, 64).unwrap(), (node % 6) as u8);
        }
        assert!(node_type(&bytes, 64, 64).is_err());
    }
}
