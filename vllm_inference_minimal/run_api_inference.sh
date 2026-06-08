#!/usr/bin/env bash
set -euo pipefail

# This script assumes a vLLM/OpenAI-compatible API server is already running.
# It reads JSONL input, loads the selected personal overall diet score prompt,
# calls /v1/chat/completions, and writes JSONL plus CSV results.
# If you see "localhost:8000 Connection refused", either start vLLM with
# deploy_vllm_model.sh or set BASE_URL to a remote/OpenAI API endpoint.
# If you see SSL EOF/network errors, try RETRIES=5 and check VPN/proxy/network.
#
# Default prompt:
#   prompts/overall_diet_score_en.txt
#
# Candidate prompt assignments:
#
# 1. English, score only:
#   PROMPT_FILE=prompts/overall_diet_score_en.txt
#   OUTPUT_FILE=data/result/overall_diet_score_en_result.jsonl
#   OUTPUT_CSV=data/result/overall_diet_score_en_result.csv
#   ERROR_FILE=data/result/overall_diet_score_en_errors.jsonl
#
# 2. Chinese, score only:
#   PROMPT_FILE=prompts/overall_diet_score_zh.txt
#   OUTPUT_FILE=data/result/overall_diet_score_zh_result.jsonl
#   OUTPUT_CSV=data/result/overall_diet_score_zh_result.csv
#   ERROR_FILE=data/result/overall_diet_score_zh_errors.jsonl
#
# 3. English, reasoning process + score:
#   PROMPT_FILE=prompts/overall_diet_score_en_reasoning.txt
#   OUTPUT_FILE=data/result/overall_diet_score_en_reasoning_result.jsonl
#   OUTPUT_CSV=data/result/overall_diet_score_en_reasoning_result.csv
#   ERROR_FILE=data/result/overall_diet_score_en_reasoning_errors.jsonl
#
# Example:
#   PROMPT_FILE=prompts/overall_diet_score_zh.txt \
#   OUTPUT_FILE=data/result/overall_diet_score_zh_result.jsonl \
#   OUTPUT_CSV=data/result/overall_diet_score_zh_result.csv \
#   ERROR_FILE=data/result/overall_diet_score_zh_errors.jsonl \
#   BASE_URL=http://localhost:8000/v1 \
#   MODEL_NAME=openchat \
#   bash run_api_inference.sh
#
# OpenAI official API example:
#   # Option A: pass variables on the same command.
#   API_PROVIDER=openai \
#   API_KEY='your_openai_api_key' \
#   PROMPT_FILE=prompts/overall_diet_score_en.txt \
#   OUTPUT_FILE=data/result/overall_diet_score_gpt54_result.jsonl \
#   OUTPUT_CSV=data/result/overall_diet_score_gpt54_result.csv \
#   bash run_api_inference.sh
#
#   # Option B: export variables first, then run the script.
#   export API_PROVIDER=openai
#   export API_KEY='your_openai_api_key'
#   bash run_api_inference.sh
#
# Note:
#   Running `API_PROVIDER=openai` alone in zsh does not export it to this bash script.

INPUT_FILE="${INPUT_FILE:-data/source_data/sample_input.jsonl}"
PROMPT_FILE="${PROMPT_FILE:-prompts/overall_diet_score_en.txt}"
OUTPUT_FILE="${OUTPUT_FILE:-data/result/overall_diet_score_api_result.jsonl}"
OUTPUT_CSV="${OUTPUT_CSV:-data/result/overall_diet_score_api_result.csv}"
ERROR_FILE="${ERROR_FILE:-data/result/overall_diet_score_api_errors.jsonl}"
API_PROVIDER="${API_PROVIDER:-vllm}"

if [[ "${API_PROVIDER}" == "openai" ]]; then
  BASE_URL="${BASE_URL:-https://api.openai.com/v1}"
  MODEL_NAME="${MODEL_NAME:-gpt-5.4}"
else
BASE_URL="${BASE_URL:-http://localhost:8000/v1}"
MODEL_NAME="${MODEL_NAME:-openchat}"
fi

API_KEY="${API_KEY:-}"
RETRIES="${RETRIES:-3}"

python3 overall_diet_score_api_generate.py \
  --input-file "${INPUT_FILE}" \
  --prompt-file "${PROMPT_FILE}" \
  --output-file "${OUTPUT_FILE}" \
  --output-csv "${OUTPUT_CSV}" \
  --error-file "${ERROR_FILE}" \
  --base-url "${BASE_URL}" \
  --model-name "${MODEL_NAME}" \
  --api-key "${API_KEY}" \
  --retries "${RETRIES}"
