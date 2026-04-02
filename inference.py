import os
import re
import time
import json
import argparse
import requests
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== Fallback score regex ======
score_pattern = re.compile(r'\b(100|[1-9]?\d(\.\d+)?)\b')


def build_messages(role: str, prefix: str, text: str, background: str, suffix: str):
    """Build chat messages for an OpenAI-compatible endpoint."""
    prefix = prefix or ""
    background = background or ""
    suffix = suffix or ""
    return [
        {"role": "system", "content": f"{role}"},
        {"role": "user", "content": f"{prefix}{text.strip()}{background}{suffix}"},
    ]


def call_chat_completion(role: str, api_url: str, headers: dict, model_name: str, n_eid: str, text: str,
                         prefix: str, suffix: str, background: str = "", timeout_s: int = 180):
    """Call /v1/chat/completions once and return `(n_eid, content, error)`."""
    payload = {
        "model": model_name,
        "messages": build_messages(role, prefix, text, background, suffix),
        "temperature": 0.7,
        "max_tokens": 4096,
    }
    try:
        resp = requests.post(api_url, headers=headers, json=payload, timeout=timeout_s)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"] if data.get("choices") else None
        return n_eid, (content.strip() if content else None), None
    except Exception as e:
        return n_eid, None, str(e)


def parse_score_from_response(raw_response: str):
    """Prefer JSON parsing and fall back to regex extraction."""
    if not raw_response:
        return None
    try:
        parsed = json.loads(raw_response)
        score = parsed.get("overall diet score", None)
        if isinstance(score, int) and 0 <= score <= 100:
            return score
    except json.JSONDecodeError:
        pass
    except Exception:
        pass

    matches = score_pattern.findall(raw_response)
    if matches:
        try:
            score = int(round(float(matches[0][0])))
            if 0 <= score <= 100:
                return score
        except Exception:
            return None
    return None


def normalize_participant_id(val):
    """Normalize participant identifiers by removing trailing decimals."""
    try:
        return str(int(float(val)))
    except (ValueError, TypeError):
        return str(val).strip()


def read_table(path: str) -> pd.DataFrame:
    """Read a CSV or Excel table based on file extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(path)
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported input format: {path}")


def write_table(df: pd.DataFrame, path: str) -> None:
    """Write a DataFrame to CSV or Excel based on file extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        df.to_csv(path, index=False)
        return
    if ext in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
        return
    raise ValueError(f"Unsupported output format: {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Batch inference via a local OpenAI-compatible or vLLM endpoint."
    )

    # ====== Service configuration ======
    parser.add_argument("--base-url", default="http://localhost:8000/v1", help="For example: http://localhost:8000/v1")
    parser.add_argument("--api-key", default="", help="Leave empty when authentication is disabled.")
    parser.add_argument("--model-name", default="openchat", help="Model name served by the endpoint.")

    # ====== Inference settings ======
    parser.add_argument("--input", default="data/demo.csv", help="Input CSV or Excel file.")
    parser.add_argument("--output", required=True, help="Output CSV or Excel file.")
    parser.add_argument("--batch-size", type=int, default=64, help="Batch size.")
    parser.add_argument("--save-every", type=int, default=64, help="Save progress every N processed rows.")
    parser.add_argument("--use-concurrency", type=int, default=1, help="Use threaded requests: 1=yes, 0=no.")
    parser.add_argument("--max-workers", type=int, default=64, help="Maximum number of worker threads.")

    # ====== Prompt configuration ======
    parser.add_argument("--prefix", default=None, help="Prompt prefix passed directly as a string.")
    parser.add_argument("--prefix-file", default=None, help="Read the prompt prefix from a UTF-8 text file.")

    # ====== Optional prompt blocks ======
    parser.add_argument(
        "--suffix",
        default="\nBased on the information above, your answer is:",
        help="Optional suffix appended after the participant profile.",
    )
    parser.add_argument("--timeout", type=int, default=180, help="Per-request timeout in seconds.")
    parser.add_argument("--background", "--Background", dest="background", default="", help="Optional background knowledge block.")
    parser.add_argument("--role", "--Role", dest="role", required=True, help="System role prompt.")
    args = parser.parse_args()

    # ====== Read the prompt prefix ======
    prefix = None
    if args.prefix_file:
        with open(args.prefix_file, "r", encoding="utf-8") as f:
            prefix = f.read()
    elif args.prefix is not None:
        prefix = args.prefix
    else:
        raise SystemExit("You must provide a prompt through --prefix or --prefix-file.")

    role = args.role
    suffix = args.suffix
    background = args.background

    # ====== Base configuration ======
    base_url = args.base_url
    openai_api_key = args.api_key
    model_name = args.model_name

    API_URL = f"{base_url}/chat/completions"
    HEADERS = {"Content-Type": "application/json"}
    if openai_api_key:
        HEADERS["Authorization"] = f"Bearer {openai_api_key}"

    input_path = args.input
    output_path = args.output
    batch_size = args.batch_size
    save_every = args.save_every
    USE_CONCURRENCY = bool(args.use_concurrency)
    max_workers = args.max_workers

    # ====== Read the input table ======
    df = read_table(input_path)
    required_columns = {"Personal Identifier", "Combined Text"}
    missing_columns = required_columns.difference(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required input columns: {sorted(missing_columns)}")

    # ====== Resume from a previous output file if it exists ======
    if os.path.exists(output_path):
        existing_df = read_table(output_path)
        processed_n_eids = set(normalize_participant_id(x) for x in existing_df["Personal Identifier"])
        results = existing_df.to_dict("records")
        print(f"Loaded {len(processed_n_eids)} existing results. Processed rows will be skipped.")
    else:
        processed_n_eids = set()
        results = []

    # ====== Batch inference ======
    unprocessed_rows = [
        row for _, row in df.iterrows()
        if normalize_participant_id(row["Personal Identifier"]) not in processed_n_eids
    ]

    for i in tqdm(range(0, len(unprocessed_rows), batch_size), desc="Processing batches"):
        batch_rows = unprocessed_rows[i:i + batch_size]
        start_time = time.time()

        if USE_CONCURRENCY:
            futures = []
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                for row in batch_rows:
                    futures.append(ex.submit(
                        call_chat_completion,
                        role,
                        API_URL,
                        HEADERS,
                        model_name,
                        normalize_participant_id(row["Personal Identifier"]),
                        str(row["Combined Text"]),
                        prefix,
                        suffix,
                        background,
                        args.timeout,
                    ))

                for fut in as_completed(futures):
                    n_eid, raw_response, err = fut.result()
                    if err:
                        print(f"[Request failed] n_eid={n_eid} error={err}")
                    if raw_response is None:
                        print(f"[Empty response] n_eid={n_eid}")
                    results.append({
                        "Personal Identifier": n_eid,
                        "raw_response": raw_response,
                    })
        else:
            for row in batch_rows:
                n_eid, raw_response, err = call_chat_completion(
                    role,
                    API_URL,
                    HEADERS,
                    model_name,
                    normalize_participant_id(row["Personal Identifier"]),
                    str(row["Combined Text"]),
                    prefix,
                    suffix,
                    background,
                    args.timeout,
                )
                if err:
                    print(f"[Request failed] n_eid={n_eid} error={err}")
                if raw_response is None:
                    print(f"[Empty response] n_eid={n_eid}")
                results.append({
                    "Personal Identifier": normalize_participant_id(n_eid),
                    "raw_response": raw_response,
                })

        elapsed = time.time() - start_time
        print(f"[Batch time] {elapsed:.2f}s for {len(batch_rows)} rows")

        # ====== Periodic save ======
        save_step = max(1, save_every // max(1, batch_size))
        if (i // batch_size + 1) % save_step == 0:
            write_table(pd.DataFrame(results), output_path)
            print(f"Checkpoint saved to {output_path} ({len(results)} rows)")

    # ====== Final save ======
    write_table(pd.DataFrame(results), output_path)
    print(f"All results saved to: {output_path}")

if __name__ == "__main__":
    main()
