#!/usr/bin/env bash
set -euo pipefail

# This script only deploys the model as an OpenAI-compatible vLLM API server.
# Keep it running while inference jobs call BASE_URL=http://HOST:PORT/v1.

MODEL_PATH="${MODEL_PATH:-/path/to/model}"
SERVED_MODEL_NAME="${SERVED_MODEL_NAME:-openchat}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
TENSOR_PARALLEL_SIZE="${TENSOR_PARALLEL_SIZE:-1}"
MAX_MODEL_LEN="${MAX_MODEL_LEN:-8192}"
GPU_MEMORY_UTILIZATION="${GPU_MEMORY_UTILIZATION:-0.8}"

python -m vllm.entrypoints.openai.api_server \
  --model "${MODEL_PATH}" \
  --served-model-name "${SERVED_MODEL_NAME}" \
  --host "${HOST}" \
  --port "${PORT}" \
  --tensor-parallel-size "${TENSOR_PARALLEL_SIZE}" \
  --max_model_len "${MAX_MODEL_LEN}" \
  --gpu_memory_utilization "${GPU_MEMORY_UTILIZATION}"
