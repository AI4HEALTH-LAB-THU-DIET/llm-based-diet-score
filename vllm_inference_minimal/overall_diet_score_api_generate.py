import argparse
import csv
import json
import os
import time
from typing import Dict, Iterable, List

import requests
from tqdm import tqdm


def read_jsonl(path: str) -> List[Dict[str, str]]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def append_jsonl(path: str, rows: Iterable[Dict[str, str]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv_from_jsonl(jsonl_path: str, csv_path: str) -> None:
    rows = read_jsonl(jsonl_path) if os.path.exists(jsonl_path) else []
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "information", "raw_response"])
        writer.writeheader()
        for row in rows:
            if row.get("error") or not row.get("raw_response"):
                continue
            writer.writerow({
                "id": row.get("id", ""),
                "information": row.get("input", ""),
                "raw_response": row.get("raw_response", ""),
            })


def successful_ids(path: str) -> set:
    if not os.path.exists(path):
        return set()
    return {
        str(row["id"])
        for row in read_jsonl(path)
        if row.get("id") and row.get("raw_response") and not row.get("error")
    }


def build_prompt(prompt_template: str, input_text: str) -> str:
    if "{input}" in prompt_template:
        return prompt_template.replace("{input}", input_text.strip())
    return f"{prompt_template.strip()}\n\nBelow is the dietary information for this individual:\n{input_text.strip()}"


def call_api(
    base_url: str,
    model_name: str,
    api_key: str,
    prompt: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
    retries: int,
) -> str:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    is_openai_api = "api.openai.com" in base_url
    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": "You are a nutrition expert for personal overall diet assessment."},
            {"role": "user", "content": prompt},
        ],
    }
    if is_openai_api:
        payload["max_completion_tokens"] = max_tokens
    else:
        payload["temperature"] = temperature
        payload["max_tokens"] = max_tokens

    url = f"{base_url.rstrip('/')}/chat/completions"
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=timeout,
            )
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries:
                raise
            wait_seconds = min(30, 2 ** (attempt - 1))
            print(f"[retry] request failed on attempt {attempt}/{retries}: {exc}. Waiting {wait_seconds}s.")
            time.sleep(wait_seconds)
    else:
        raise RuntimeError(f"Request failed after {retries} attempts: {last_error}")

    if not response.ok:
        raise RuntimeError(
            f"HTTP {response.status_code} from {url}: "
            f"{response.text}"
        )
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def main():
    parser = argparse.ArgumentParser(description="Personal overall diet score generation through a vLLM/OpenAI-compatible API.")
    parser.add_argument("--input-file", default="data/source_data/sample_input.jsonl")
    parser.add_argument("--prompt-file", default="prompts/overall_diet_score_en.txt")
    parser.add_argument("--output-file", default="data/result/overall_diet_score_api_result.jsonl")
    parser.add_argument("--output-csv", default="data/result/overall_diet_score_api_result.csv")
    parser.add_argument("--error-file", default="data/result/overall_diet_score_api_errors.jsonl")
    parser.add_argument("--base-url", default="http://localhost:8000/v1")
    parser.add_argument("--model-name", default="openchat")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--temperature", type=float, default=0.7)
    parser.add_argument("--max-tokens", type=int, default=1024)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()

    print(f"Input file: {args.input_file}")
    print(f"Prompt file: {args.prompt_file}")
    print(f"Output JSONL: {args.output_file}")
    print(f"Output CSV: {args.output_csv}")
    print(f"Error JSONL: {args.error_file}")
    print(f"Base URL: {args.base_url}")
    print(f"Model name: {args.model_name}")

    with open(args.prompt_file, "r", encoding="utf-8") as f:
        prompt_template = f.read()

    rows = read_jsonl(args.input_file)
    existing_ids = successful_ids(args.output_file)

    for row in tqdm(rows, desc="Overall diet score API inference"):
        row_id = str(row["id"])
        if row_id in existing_ids:
            continue
        prompt = build_prompt(prompt_template, row["input"])
        try:
            raw_response = call_api(
                args.base_url,
                args.model_name,
                args.api_key,
                prompt,
                args.temperature,
                args.max_tokens,
                args.timeout,
                args.retries,
            )
            error = ""
        except Exception as exc:
            error = str(exc)
            print(f"[failed] id={row_id} error={error}")
            append_jsonl(args.error_file, [{
                "id": row_id,
                "input": row["input"],
                "error": error,
            }])
            continue

        if not raw_response:
            error = "Empty response"
            print(f"[failed] id={row_id} error={error}")
            append_jsonl(args.error_file, [{
                "id": row_id,
                "input": row["input"],
                "error": error,
            }])
            continue

        append_jsonl(args.output_file, [{
            "id": row_id,
            "input": row["input"],
            "raw_response": raw_response,
        }])
        write_csv_from_jsonl(args.output_file, args.output_csv)

    write_csv_from_jsonl(args.output_file, args.output_csv)


if __name__ == "__main__":
    main()
