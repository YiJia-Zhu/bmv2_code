# Repository Guidelines

## Project Structure

- `bmv2_llm_compile_benchmark.py` generates P4 responses and measures local/PCL/DeepSeek/OpenAI-compatible model compile rates.
- `test_pcl_models.py` and `test.sh` provide PCL model/API smoke tests.
- `测试大概流程.md` documents the proposed benchmark and dataset design.
- `测试集划分与任务格式.md` defines the LLM task schema, leakage-safe splits, hidden oracle policy, and BMv2 pilot prompts.
- `data/raw/repositories/` stores immutable source snapshots and download provenance.
- `data/normalized/` stores JSONL records, compiler stderr, and auditable compile splits.
- `data/normalized/hf_p4llms/` stores the isolated normalized `p4llms/p4dataset` export.
- `data/tools/` contains the normalization, BMv2 prefilter, and failure-classification scripts.
- `data/pipelines/bigquery_gharchive_p4/` is the isolated Plan B repository collector. Treat `pipeline.json` as the canonical machine-readable status/phase record, `collector.py` as the canonical entry point, and `data/raw/gharchive_p4/` as its isolated output root. `data/tools/collect_gharchive_p4.py` is compatibility-only.
- `data/tools/recover_p4llms_dependencies.py` downloads missing GitHub include/support files into the isolated `data/raw/hf/p4dataset_supplemental/` overlay; `merge_bmv2_repair_results.py` and `build_bmv2_dataset_splits.py` create auditable repaired results and eligibility splits.
- `benchmark_runs/` and `training_smoke/runs/` are generated outputs and should not be treated as source.
- `training_smoke/` holds isolated Qwen/vLLM/LlamaFactory smoke-test scripts, configs, generated fine-tuning splits, and run outputs separate from the main data-collection tree.

## Current Collection Plans

- `TODO_code.md` is the root-level P4 code-collection plan, moved from `data/raw/swh_p4/TODO.md`. Read it for the latest written progress and next steps; detailed SWH run evidence remains under `data/raw/swh_p4/`. Check live manifests and checkpoints before assuming a running job has finished.
- `TODO_text.md` is the separate plan for P4 documentation knowledge after code collection. Its current scope is P4 specifications, official tutorials and tool documentation, papers, and targeted web sources. No bulk text collection has started.
- Keep code and documentation raw sources and training views separate. A discovered `.p4` path or static P4_16 candidate is not a confirmed P4_16 repository or a document count.

## Goal and Dataset Gate

The project compares BMv2 P4 compile-through rates across local and PCL models. Start with a reviewed 30-task pilot, validate the harness, then freeze a 300-task benchmark. Each task needs a stable ID, bilingual prompt variants, an author-verified `main.p4`, required-feature metadata, and source or generation provenance. Split by semantic family to prevent near-duplicate leakage, and keep pretraining, supervised, and benchmark data separate.

The Hugging Face `SFU-CONNECT/p4_dataset` snapshot is an additional, isolated
source. It must not be conflated with the separate `p4llms/p4dataset` pipeline
repository, even though both use GitHub P4 data and Stack-like path names.
Normalize it under `data/normalized/hf/` with `hf_`-prefixed artifacts;
never merge it into or overwrite the GitHub JSONL files. Retain both the raw
P4 text and the cleaned compile input, and record the local compiler result
separately from the dataset's claimed `compiles` flag.

The strict positive-data gate is `data/normalized/positive_compile_clean.jsonl`: every record must have `compile_status=compiled`, `returncode=0`, and use `p4c-bm2-ss` with P4_16/v1model. Intended error tests, target-specific programs, incomplete fragments, and source defects remain in separate splits. Never edit raw code to manufacture a 100% rate.

For the isolated `p4llms/p4dataset` export, use `hf_p4llms_bmv2_eligible.jsonl` as the BMv2-ready set (2,439 records, all return code 0), `hf_p4llms_bmv2_excluded.jsonl` for negative/target-specific/incomplete material, and `hf_p4llms_bmv2_repair_needed.jsonl` for unresolved dependencies or source defects. The supplemental overlay and its URL/SHA-256 manifest are separate from both the raw HF snapshot and normalized source JSONL.

LLM task construction must turn eligible source into reviewed natural-language specifications rather than exposing reference code. Split by repository and semantic `family_id`; keep the functional oracle/reference implementation hidden. Use `data/tasks/bmv2_pilot_prompts.jsonl` only as a harness smoke set, not as a final ranking set.

The generated `data/tasks/bmv2_full_auto_prompts.jsonl` contains 2,439 provenance-linked automatic prompts from the BMv2 eligible pool. Every row is marked `human_review=pending`; inspect it before spending API quota. It is a stress-test input, not the reviewed 300-task benchmark.

## Build and Test Commands

- `python3 -m py_compile bmv2_llm_compile_benchmark.py test_pcl_models.py data/tools/*.py training_smoke/tools/*.py` checks syntax.
- `python3 -m unittest data/pipelines/bigquery_gharchive_p4/tests/test_collector.py -q` runs the offline Plan B collector tests without network or BigQuery jobs.
- `python3 data/tools/normalize_dataset.py` refreshes normalized records.
- `python3 data/tools/compile_bmv2_candidates.py --auto-include --include-path /usr/include` runs the BMv2 prefilter.
- `python3 data/tools/classify_bmv2_results.py` regenerates the 100%-clean split and failure taxonomy.
- `python3 data/tools/recover_p4llms_dependencies.py --workers 48` recovers missing p4llms repository files without changing the HF snapshot.
- `python3 data/tools/build_bmv2_dataset_splits.py --classified-root data/normalized/hf_p4llms --output-root data/normalized/hf_p4llms` writes the final eligible/excluded/repair-needed sets.
- `./test_pcl_models.py --list-only` lists models; `./test_pcl_models.py` performs smoke requests.
- `./bmv2_llm_compile_benchmark.py --provider local --attempts 1` runs a local benchmark; use `--provider pcl` for PCL.
- `./bmv2_llm_compile_benchmark.py --provider deepseek --model deepseek-chat --prompts data/tasks/bmv2_pilot_prompts.jsonl` runs the DeepSeek provider using `.env` credentials.
- `./bmv2_llm_compile_benchmark.py --provider openai --model gpt-5.5 --prompts data/tasks/bmv2_pilot_prompts.jsonl` runs the OpenAI-compatible provider using `.env` credentials.

PCL tests require the configured tunnel. BMv2 tests require `p4c-bm2-ss` on `PATH`.

## Style and Testing

Use Python 3.8-compatible syntax, four-space indentation, `snake_case` names, and `UPPER_SNAKE_CASE` constants. Prefer the standard library and explicit error handling. There is no pytest suite; run syntax checks first, then a small smoke test and `p4c-bm2-ss --p4v 16` validation.

## Commits and Security

Use short imperative commits, for example `Add PCL SOCKS transport`. Pull requests should describe behavior changes, list validation commands, and include compile summaries when benchmark logic changes. Keep credentials only in `.env`; never print or commit keys. Do not commit generated compiler artifacts, benchmark runs, or tunnel details containing secrets.
