"""FSDP 테스트에 쓸 학습 코드 · 모델 · 데이터를 S3에 올린다. 표준 라이브러리와 aws CLI만 쓴다.

    python3 examples/fsdp/seed_assets.py --bucket <20-data bucket_name>
    python3 examples/fsdp/seed_assets.py --bucket x --dry-run   # 올리지 않고 준비만 한다

올라가는 곳 (Infra Mountpoint Role이 읽을 수 있는 prefix):
    code/_shared/train.py
    datasets/_shared/models/gpt2/...
    datasets/ds-tiny-shakespeare/v1/{train-000.jsonl, manifest.json}

한 번 올린 데이터셋 버전은 덮어쓰지 않는다. train.py와 모델은 다시 실행하면 갱신된다.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import urllib.request

MODEL_ID = "openai-community/gpt2"
MODEL_FILES = [
    "config.json",
    "generation_config.json",
    "merges.txt",
    "model.safetensors",  # 약 550MB
    "tokenizer.json",
    "tokenizer_config.json",
    "vocab.json",
]
MODEL_KEY = "datasets/_shared/models/gpt2"
TRAIN_KEY = "code/_shared/train.py"
DATA_URL = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"
DATASET_ID = "ds-tiny-shakespeare"
DATASET_VERSION = 1
DATASET_NAME = "Tiny Shakespeare"
DATASET_FORMAT = "text-jsonl"


def to_jsonl(text: str) -> str:
    """빈 줄로 나눈 문단 하나를 한 줄의 {"text": ...}로 만든다."""
    records = [p.strip() for p in text.split("\n\n") if p.strip()]
    return "".join(json.dumps({"text": r}, ensure_ascii=False) + "\n" for r in records)


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def build_manifest(data_dir: str, dataset_id: str, version: int, name: str, fmt: str) -> dict:
    files = []
    for fn in sorted(os.listdir(data_dir)):
        if fn == "manifest.json":
            continue
        path = os.path.join(data_dir, fn)
        files.append({"name": fn, "size_bytes": os.path.getsize(path), "sha256": sha256_of(path)})
    return {"dataset_id": dataset_id, "version": version, "name": name, "format": fmt, "files": files}


def download(url: str, path: str) -> None:
    print(f"download {url}", flush=True)
    with urllib.request.urlopen(url) as res, open(path, "wb") as f:
        while chunk := res.read(1 << 20):
            f.write(chunk)


def prepare(work: str) -> tuple[str, str]:
    model_dir = os.path.join(work, "model")
    data_dir = os.path.join(work, "data")
    os.makedirs(model_dir)
    os.makedirs(data_dir)
    for fn in MODEL_FILES:
        download(f"https://huggingface.co/{MODEL_ID}/resolve/main/{fn}", os.path.join(model_dir, fn))
    raw = os.path.join(work, "input.txt")
    download(DATA_URL, raw)
    with open(raw, encoding="utf-8") as f:
        records = to_jsonl(f.read())
    with open(os.path.join(data_dir, "train-000.jsonl"), "w", encoding="utf-8") as f:
        f.write(records)
    manifest = build_manifest(data_dir, DATASET_ID, DATASET_VERSION, DATASET_NAME, DATASET_FORMAT)
    with open(os.path.join(data_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    return model_dir, data_dir


def aws(*args: str) -> None:
    subprocess.run(["aws", *args], check=True)


def exists(bucket: str, key: str) -> bool:
    return subprocess.run(
        ["aws", "s3api", "head-object", "--bucket", bucket, "--key", key],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode == 0


def upload(bucket: str, model_dir: str, data_dir: str) -> None:
    train = os.path.join(os.path.dirname(os.path.abspath(__file__)), "train.py")
    aws("s3", "cp", train, f"s3://{bucket}/{TRAIN_KEY}")
    aws("s3", "sync", model_dir, f"s3://{bucket}/{MODEL_KEY}/")
    prefix = f"datasets/{DATASET_ID}/v{DATASET_VERSION}"
    if exists(bucket, f"{prefix}/manifest.json"):
        print(f"{prefix} already exists: keeping it")
        return
    # manifest를 마지막에 올린다. 목록은 manifest로 찾으므로 덜 올라간 버전이 보이지 않는다.
    aws("s3", "sync", data_dir, f"s3://{bucket}/{prefix}/", "--exclude", "manifest.json")
    aws("s3", "cp", os.path.join(data_dir, "manifest.json"), f"s3://{bucket}/{prefix}/manifest.json")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True, help="20-data 출력 bucket_name")
    p.add_argument("--dry-run", action="store_true", help="내려받기와 변환만 하고 올리지 않는다")
    args = p.parse_args()

    with tempfile.TemporaryDirectory() as work:
        model_dir, data_dir = prepare(work)
        with open(os.path.join(data_dir, "manifest.json"), encoding="utf-8") as f:
            print(f.read())
        if args.dry_run:
            print("dry run: nothing uploaded")
            return 0
        upload(args.bucket, model_dir, data_dir)
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
