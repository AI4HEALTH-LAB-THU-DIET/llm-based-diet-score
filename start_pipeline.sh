#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

# -------- Configuration --------
MODEL_PATH="${MODEL_PATH:-/path/to/Qwen3.5-27B}"
SERVED_MODEL_NAME="${SERVED_MODEL_NAME:-openchat}"
HOST="${HOST:-your-vllm-host}"
PORT="${PORT:-8000}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-20176}"
GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.8}"
TENSOR_PARALLEL_SIZE="${TENSOR_PARALLEL_SIZE:-8}"
TOOL_CALL_PARSER="${TOOL_CALL_PARSER:-hermes}"

INPUT_FILE="${INPUT_FILE:-data/demo.csv}"
OUTPUT_FILE="${OUTPUT_FILE:-outputs/inference_output.csv}"
PROMPT_FILE="${PROMPT_FILE:-prompts/respiratory.txt}"
ROLE_TEXT="${ROLE_TEXT:-You are a nutrition expert for disease-specific dietary assessment.}"
BACKGROUND_TEXT="${BACKGROUND_TEXT:-}"

# -------- Usage --------
# 1. Start the vLLM server:
#    bash start_pipeline.sh serve
#
# 2. Run  inference:
#
# 3. Override settings when needed:
#    MODEL_PATH=/your/model \
#    PROMPT_FILE=prompts/cancer.txt \
#    OUTPUT_FILE=outputs/cancer_output.csv \
#    bash start_pipeline.sh infer

serve_vllm() {
  python -m vllm.entrypoints.openai.api_server \
    --model "${MODEL_PATH}" \
    --served-model-name "${SERVED_MODEL_NAME}" \
    --max_model_len "${MAX_MODEL_LEN}" \
    --tensor-parallel-size "${TENSOR_PARALLEL_SIZE}" \
    --port "${PORT}" \
    --host "${HOST}" \
    --enable-auto-tool-choice \
    --tool-call-parser "${TOOL_CALL_PARSER}" \
    --gpu_memory_utilization "${GPU_MEMORY_UTILIZATION}"
}

run_inference() {
  mkdir -p outputs

  python3 inference.py \
    --base-url "http://${HOST}:${PORT}/v1" \
    --model-name "${SERVED_MODEL_NAME}" \
    --input "${INPUT_FILE}" \
    --output "${OUTPUT_FILE}" \
    --prefix-file "${PROMPT_FILE}" \
    --role "${ROLE_TEXT}" \
    --background "${BACKGROUND_TEXT}"
}

case "${1:-}" in
  serve)
    serve_vllm
    ;;
  infer)
    run_inference
    ;;
  *)
    echo "Usage: bash start_pipeline.sh [serve|infer]"
    exit 1
    ;;
esac
