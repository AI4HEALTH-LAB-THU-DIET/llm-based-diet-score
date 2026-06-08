# Personal Overall Diet Score vLLM Inference

This project generates a personal overall dietary health score from dietary information and personal health background.
It includes input data, prompt files, vLLM deployment, API inference, and result export.

## Directory

```text
.
├── overall_diet_score_api_generate.py
├── data
│   ├── result
│   └── source_data
│       └── sample_input.jsonl
├── deploy_vllm_model.sh
├── prompts
│   ├── overall_diet_score_en.txt
│   ├── overall_diet_score_zh.txt
│   └── overall_diet_score_en_reasoning.txt
├── requirements_api.txt
├── requirements_vllm.txt
└── run_api_inference.sh
```

## Input Data

Input files are placed in `data/source_data/`.

The expected format is JSONL, one participant per line:

```json
{"id": "2848581", "input": "[Basic information]\nSex: ..."}
```

The current 5-case example input file is:

```text
data/source_data/sample_input.jsonl
```

It contains five example participants, each with an `id` and a full dietary/health background text in `input`.

## Prompt

The prompt files are stored separately in `prompts/`:

```text
prompts/overall_diet_score_en.txt
prompts/overall_diet_score_zh.txt
prompts/overall_diet_score_en_reasoning.txt
```

The default prompt is `prompts/overall_diet_score_en.txt`. All prompt files use `{input}` as the participant text placeholder.

The English score-only prompt asks the model to return one overall score and 13 category-specific scores:

```json
{
  "overall_score": int,
  "cancer_score": int,
  "circulatory_score": int,
  "endocrine_score": int,
  "digestive_score": int,
  "musculo_score": int,
  "nervous_score": int,
  "mental_score": int,
  "respiratory_score": int,
  "genito_score": int,
  "blood_score": int,
  "skin_score": int,
  "ear_score": int,
  "eye_score": int
}
```

The Chinese score-only prompt asks the model to return Chinese field names:

```json
{
  "总体评分": int,
  "癌症评分": int,
  "循环系统评分": int,
  "内分泌评分": int,
  "消化系统评分": int,
  "肌肉骨骼评分": int,
  "神经系统评分": int,
  "精神和行为评分": int,
  "呼吸系统评分": int,
  "泌尿和生殖系统评分": int,
  "血液和免疫系统评分": int,
  "皮肤评分": int,
  "耳部评分": int,
  "眼部评分": int
}
```

The English reasoning prompt asks the model to return:

```json
{
  "reasoning_process": string,
  "overall_score": int,
  "cancer_score": int,
  "circulatory_score": int,
  "endocrine_score": int,
  "digestive_score": int,
  "musculo_score": int,
  "nervous_score": int,
  "mental_score": int,
  "respiratory_score": int,
  "genito_score": int,
  "blood_score": int,
  "skin_score": int,
  "ear_score": int,
  "eye_score": int
}
```

## Step 1: Deploy a vLLM Model

Use `deploy_vllm_model.sh` to start an OpenAI-compatible vLLM API server. Keep this process running while inference is being performed.

Install the vLLM server dependencies first:

```bash
python -m pip install -r requirements_vllm.txt
```

```bash
cd UKB20w_random5_cases/vllm_inference_minimal

MODEL_PATH=/path/to/model \
SERVED_MODEL_NAME=openchat \
HOST=0.0.0.0 \
PORT=8000 \
TENSOR_PARALLEL_SIZE=8 \
bash deploy_vllm_model.sh
```

Internally, it runs:

```bash
python -m vllm.entrypoints.openai.api_server ...
```

The served endpoint will be:

```text
http://HOST:PORT/v1/chat/completions
```

## Step 2: Run API Inference

Use `run_api_inference.sh` after the vLLM API server is running. The script reads `data/source_data/sample_input.jsonl`, loads the selected prompt, calls the chat completion API, and writes JSONL and CSV outputs.

If this machine only calls an existing API and does not deploy vLLM, install the API-only dependencies:

```bash
python -m pip install -r requirements_api.txt
```

```bash
cd UKB20w_random5_cases/vllm_inference_minimal

BASE_URL=http://localhost:8000/v1 \
MODEL_NAME=openchat \
bash run_api_inference.sh
```

Default output:

```text
data/result/overall_diet_score_api_result.jsonl
data/result/overall_diet_score_api_result.csv
```

The CSV contains three columns:

```text
id,information,raw_response
```

Switch prompt files at runtime with `PROMPT_FILE`:

```bash
PROMPT_FILE=prompts/overall_diet_score_zh.txt \
OUTPUT_FILE=data/result/overall_diet_score_zh_result.jsonl \
OUTPUT_CSV=data/result/overall_diet_score_zh_result.csv \
bash run_api_inference.sh

PROMPT_FILE=prompts/overall_diet_score_en_reasoning.txt \
OUTPUT_FILE=data/result/overall_diet_score_en_reasoning_result.jsonl \
OUTPUT_CSV=data/result/overall_diet_score_en_reasoning_result.csv \
bash run_api_inference.sh
```

The same script can call any OpenAI-compatible remote API by changing `BASE_URL`, `MODEL_NAME`, and `API_KEY`:

```bash
BASE_URL=http://localhost:8000/v1 \
MODEL_NAME=openchat \
API_KEY= \
bash run_api_inference.sh
```

For OpenAI's Chat Completions-compatible endpoint, use `API_PROVIDER=openai` or explicitly set `BASE_URL`:

```bash
API_PROVIDER=openai \
BASE_URL=https://api.openai.com/v1 \
MODEL_NAME=gpt-4o-mini \
API_KEY=your_api_key \
bash run_api_inference.sh
```

## Main Files

- `deploy_vllm_model.sh`: deploys the model as a vLLM OpenAI-compatible API server.
- `run_api_inference.sh`: calls an already-running vLLM/OpenAI-compatible API.
- `overall_diet_score_api_generate.py`: calls `/v1/chat/completions` through HTTP.
- `prompts/`: personal overall diet score prompts.
- `requirements_vllm.txt`: server-side vLLM environment versions, taken from the working server environment. Use this on a machine that deploys vLLM.
- `requirements_api.txt`: local API-only inference environment versions. Use this on a machine that only calls an existing vLLM/OpenAI-compatible API.

## Requirements

There are two requirements files because model serving and API calling have different environments:

```text
requirements_vllm.txt
requirements_api.txt
```

Use `requirements_vllm.txt` on the server that deploys the vLLM model. It contains pinned package versions from the server-side vLLM environment, including `vllm`, `torch`, `transformers`, `ray`, and related packages.

Use `requirements_api.txt` on a local or client machine that only runs `run_api_inference.sh`. It contains the packages needed by the API calling script, currently `requests` and `tqdm`, with versions taken from the local API-inference environment.
