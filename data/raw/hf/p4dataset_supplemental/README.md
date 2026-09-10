# p4llms supplemental repository context

This directory is a derived, immutable overlay for `p4llms/p4dataset` BMv2
checks. The HF export contains mostly `.p4` files and omits some headers,
preprocessor files, and project support files. Files here were retrieved from
the corresponding GitHub repository at a `raw.githubusercontent.com` URL.

- `hf_p4llms_<owner>__<repository>/` mirrors the normalized source namespace.
- `download_manifest.jsonl` records the URL, repository/path, retrieval time,
  local path, byte count, and SHA-256 for every request across all recovery
  passes. Failed/404 requests remain in the manifest.
- `download_summary.json` records the most recent recovery pass.

The directory never overwrites `data/raw/hf/p4dataset/` or the normalized HF
JSONL sources. Recompile with:

```bash
python3 data/tools/compile_bmv2_candidates.py \
  --input data/normalized/hf_p4llms/hf_p4llms_missing_dependency_samples.jsonl \
  --context-input data/normalized/hf_p4llms/hf_p4llms_all_records.jsonl \
  --raw-root data/raw/hf/p4dataset \
  --supplemental-root data/raw/hf/p4dataset_supplemental \
  --auto-include --include-path /usr/local/share/p4c/p4include
```
