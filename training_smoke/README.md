# Training Smoke Workspace

This folder isolates Qwen3/P4 training smoke tests, local vLLM launch scripts,
continuation benchmarks, PINC experiments, generated fine-tuning splits, and
run outputs from the main data-collection tree.

Layout:

- `scripts/`: entrypoint shell scripts for data prep, training, vLLM, and smoke evaluation.
- `tools/`: Python helpers used only by these training/evaluation smoke runs.
- `configs/`: LlamaFactory YAML configs.
- `data/`: generated fine-tuning JSONL and small manifests.
- `runs/`: generated benchmark outputs and LoRA/checkpoint artifacts.

Defaults keep source datasets in the repository root (`../data/...`) and write
generated smoke artifacts under this folder. Override the environment variables
documented in each script when running on another machine.

Common commands:

```bash
training_smoke/scripts/run_p4llms_qwen3_pipeline.sh status
training_smoke/scripts/run_p4llms_qwen3_pipeline.sh prepare
training_smoke/scripts/run_pinc_qwen3_sft.sh status
```

Do not treat `runs/` or generated JSONL files as source data.
