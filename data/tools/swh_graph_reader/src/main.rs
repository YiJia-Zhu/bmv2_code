//! Read-only JSON-lines adapter for the unlabelled SWH transposed graph.
//! SWHID permutations and reverse mappings are deliberately supplied by the
//! Python driver through bounded, verified S3 Range reads.

use anyhow::{bail, ensure, Context, Result};
use dsi_bitstream::prelude::BE;
use memmap2::{Mmap, MmapOptions};
use ph::fmph::GOFunction;
use ph::seeds::TwoToPowerBitsStatic;
use serde_json::{json, Value};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::time::Instant;
use webgraph::prelude::*;

type Mph = GOFunction<TwoToPowerBitsStatic<4>, TwoToPowerBitsStatic<1>>;

fn node_type(data: &[u8], node: usize, nodes: usize) -> Result<u8> {
    ensure!(node < nodes, "node ID out of bounds: {node}");
    let bit = node.checked_mul(3).context("node bit offset overflow")?;
    let word_offset = bit / 64 * 8;
    let shift = bit % 64;
    // SWH's packed Node2Type uses native usize words, produced on little-endian
    // x86_64. A 3-bit value may straddle two 64-bit words.
    let first = u64::from_le_bytes(data[word_offset..word_offset + 8].try_into()?);
    let mut value = first >> shift;
    if shift > 61 {
        let next = u64::from_le_bytes(data[word_offset + 8..word_offset + 16].try_into()?);
        value |= next << (64 - shift);
    }
    let value = (value & 7) as u8;
    ensure!(value < 6, "invalid node type {value} at node {node}");
    Ok(value)
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
    let root = PathBuf::from(std::env::args().nth(1).context("usage: reader GRAPH_DIR")?);
    let started = Instant::now();
    output(json!({"event":"loading_graph"}))?;
    let graph = BvGraph::with_basename(root.join("graph-transposed"))
        .endianness::<BE>()
        .flags(MemoryFlags::RANDOM_ACCESS)
        .load()
        .context("load graph-transposed.graph / .ef / .properties")?;
    let nodes = graph.num_nodes();
    output(json!({"event":"loading_mph", "nodes":nodes}))?;
    let mph: Mph = GOFunction::read(&mut BufReader::new(File::open(root.join("graph.fmphgo"))?))
        .context("load graph.fmphgo")?;
    ensure!(mph.len() == nodes, "MPHF key count differs from graph node count");
    let type_file = File::open(root.join("graph.node2type.bin"))?;
    let expected_type_bytes = nodes.checked_mul(3).context("type length overflow")?.div_ceil(64) * 8;
    ensure!(type_file.metadata()?.len() == expected_type_bytes as u64, "node2type size mismatch");
    // The file remains read-only and its length is fixed for this process.
    let types: Mmap = unsafe { MmapOptions::new().map(&type_file)? };
    output(json!({"event":"ready", "nodes":nodes, "arcs":graph.num_arcs(),
                  "load_seconds":started.elapsed().as_secs_f64()}))?;
    for line in io::stdin().lock().lines() {
        let value: Value = serde_json::from_str(&line?)?;
        match value["op"].as_str().context("missing operation")? {
            "hash" => {
                let mut results = Vec::new();
                for swhid in value["swhids"].as_array().context("missing swhids")? {
                    let swhid = swhid.as_str().context("invalid swhid")?;
                    ensure!(swhid.len() == 50 && swhid.starts_with("swh:1:dir:"), "invalid directory SWHID");
                    results.push(json!({"swhid":swhid,"position":mph.get(swhid.as_bytes())}));
                }
                output(json!({"event":"hashes","results":results}))?;
            }
            "neighbors" => {
                let mut results = Vec::new();
                for node in value["nodes"].as_array().context("missing nodes")? {
                    let node = usize::try_from(node.as_u64().context("invalid node ID")?)?;
                    let kind = node_type(&types, node, nodes)?;
                    ensure!(kind == 1, "input node {node} is not a directory");
                    let degree = graph.outdegree(node);
                    ensure!(degree <= 1_000_000, "sample neighbor limit exceeded at node {node}: {degree}");
                    let mut parents = Vec::new();
                    let mut revisions = Vec::new();
                    let mut type_counts = [0u64; 6];
                    for neighbor in RandomAccessLabeling::labels(&graph, node) {
                        let neighbor_kind = node_type(&types, neighbor, nodes)?;
                        type_counts[neighbor_kind as usize] += 1;
                        if neighbor_kind == 1 { parents.push(neighbor); }
                        if neighbor_kind == 4 { revisions.push(neighbor); }
                    }
                    ensure!(type_counts.iter().sum::<u64>() == degree as u64, "degree mismatch");
                    parents.sort_unstable();
                    parents.dedup();
                    revisions.sort_unstable();
                    revisions.dedup();
                    results.push(json!({"node_id":node,"node_type":kind,"outdegree":degree,
                                        "parent_nodes":parents,"revision_nodes":revisions,
                                        "neighbor_type_counts":type_counts}));
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
    fn types_across_word_boundaries() {
        let mut words = [0u64; 4];
        for node in 0..64 {
            let value = (node % 6) as u64;
            let bit = node * 3;
            words[bit / 64] |= value << (bit % 64);
            if bit % 64 > 61 { words[bit / 64 + 1] |= value >> (64 - bit % 64); }
        }
        let bytes: Vec<u8> = words.iter().flat_map(|w| w.to_le_bytes()).collect();
        for node in 0..64 {
            assert_eq!(node_type(&bytes, node, 64).unwrap(), (node % 6) as u8);
        }
        assert!(node_type(&bytes, 64, 64).is_err());
    }
}
