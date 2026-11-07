# P4 text seed collector

`collector.py` collects public seed documents for labeling, independently of
the Dolma 3 pool recall. Run it on `172.22.11.186` under
`/storage/xgr/zhuyijia/bmv2_code`. It creates a SQLite FIFO queue, records
each discovered link, stores content-addressed raw bytes, extracts text, and
exports a per-URL JSONL manifest. Robots rules, source host/path scope, byte
limits, depth, page budget, and per-host delay bound each run. Out-of-scope
links are recorded for review and never followed automatically.

```bash
python3 data/pipelines/p4_text_seeds/collector.py seed \
  --output data/raw/p4_text/seed_crawl --sources p4org eth_adv_net
python3 data/pipelines/p4_text_seeds/collector.py run \
  --output data/raw/p4_text/seed_crawl --max-pages 30 --max-depth 2
python3 data/pipelines/p4_text_seeds/collector.py reprocess \
  --output data/raw/p4_text/seed_crawl --max-pages 30
python3 data/pipelines/p4_text_seeds/collector.py export \
  --output data/raw/p4_text/seed_crawl
python3 data/pipelines/p4_text_seeds/prepare.py \
  --crawl-root data/raw/p4_text/seed_crawl \
  --output-root data/normalized/p4_text
```

`queue.jsonl` exports all URL states, `discovered_links.jsonl` exports in-scope
and review-only edges, and `documents.jsonl` contains fetched records with
content-addressed raw and extracted text paths. Links found at the depth
limit remain queued for a later run with a higher `--max-depth`. HTML
extraction removes common navigation/script elements; PDF uses `pdftotext`,
PPTX uses OOXML, and legacy PPT uses LibreOffice plus `pdftotext` (both must
be installed). Extraction errors leave the raw file and an error in the
manifest. A fetched page is a candidate seed, not an approved positive label.
`reprocess` upgrades extraction from saved bytes without another network
request. `run --run-source eth_adv_net --url-substring p4` selects matching
queued URLs while leaving other queue items for later FIFO runs.

`prepare.py` verifies every raw and extracted text SHA-256, creates one
`documents.jsonl` row per unique text, preserves each source URL in
`occurrences.jsonl`, and writes `review_queue.jsonl` with every label set to
`pending`. The review queue is regenerated on each run: put human decisions
in a separate `labels.jsonl` file, not in the queue. Empty extraction and
license status need review before classifier training or redistribution.
For Discourse topics, the collector also queues the public `.json` endpoint;
its extracted text keeps post IDs, authors, timestamps, and the reported
post count. The review queue favors JSON over the corresponding HTML, while
both raw formats remain in the occurrence manifest. A topic whose fetched
post count is lower than the reported stream length needs a follow-up API
request before treating its answers as complete.

`error` entries are retained for inspection. Requeue them only after
reviewing their cause; `fetching` entries are automatically requeued after
an interrupted run. Follow-up discovery for GitHub Wikis, LinkedIn,
search results, and Stack Overflow must be registered as explicit sources
before their links are fetched.
