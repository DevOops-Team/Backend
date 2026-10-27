"""2-GPU DDP 예시 학습 스크립트.

규칙 (인프라가 기대하는 것):
  - S3 코드를 갖지 않는다. 경로만 쓴다: 코드 $CODE_DIR, 데이터 $DATA_DIR, 체크포인트 $CKPT_DIR.
    세 경로는 Mountpoint for S3로 마운트된 작업 전용 디렉터리다.
  - 체크포인트는 새 파일을 처음부터 끝까지 한 번에 쓴다. Mountpoint는 rename·append를 지원하지 않으므로
    임시 파일 후 rename 방식은 쓰지 않는다. 시작 시 가장 최근 체크포인트에서 재개한다.
  - rank 0이 매 step DynamoDB($PROGRESS_TABLE, PK job_id)에 진행 상태를 덮어쓴다.
"""

import argparse
import glob
import io
import os
import re
import time
from decimal import Decimal

import boto3
import numpy as np
import torch
import torch.distributed as dist
import torch.nn as nn
from torch.nn.parallel import DistributedDataParallel as DDP

JOB_ID = os.environ["JOB_ID"]
CKPT_DIR = os.environ.get("CKPT_DIR", "/ckpt")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
TABLE = os.environ.get("PROGRESS_TABLE", "")
NODE = os.environ.get("NODE_NAME", "")
CKPT_RE = re.compile(r"ckpt-(\d+)\.pt$")
KEEP_CHECKPOINTS = 2


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--total-steps", type=int, default=5000)
    p.add_argument("--ckpt-every", type=int, default=1000)
    p.add_argument("--batch-size", type=int, default=256)
    p.add_argument("--lr", type=float, default=1e-3)
    return p.parse_args()


def load_data(device):
    """$DATA_DIR의 CSV(마지막 열이 레이블)를 읽는다. 없으면 합성 데이터를 만든다."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if files:
        arr = np.concatenate([np.loadtxt(f, delimiter=",", ndmin=2) for f in files])
        x, y = arr[:, :-1], arr[:, -1:]
    else:
        rng = np.random.default_rng(0)
        x = rng.normal(size=(100_000, 32))
        y = x @ rng.normal(size=(32, 1)) + 0.1 * rng.normal(size=(100_000, 1))
    return (torch.tensor(x, dtype=torch.float32, device=device),
            torch.tensor(y, dtype=torch.float32, device=device))


def latest_checkpoint():
    found = [(int(m.group(1)), p) for p in glob.glob(os.path.join(CKPT_DIR, "ckpt-*.pt"))
             if (m := CKPT_RE.search(p))]
    return max(found) if found else (0, None)


def save_checkpoint(step, model, opt):
    # 메모리에 직렬화한 뒤 새 파일에 순차로 한 번에 쓴다 (Mountpoint 규칙).
    buf = io.BytesIO()
    torch.save({"step": step, "model": model.module.state_dict(), "opt": opt.state_dict()}, buf)
    with open(os.path.join(CKPT_DIR, f"ckpt-{step}.pt"), "wb") as f:
        f.write(buf.getvalue())
    old = sorted(int(m.group(1)) for p in glob.glob(os.path.join(CKPT_DIR, "ckpt-*.pt"))
                 if (m := CKPT_RE.search(p)))
    for s in old[:-KEEP_CHECKPOINTS]:
        os.remove(os.path.join(CKPT_DIR, f"ckpt-{s}.pt"))


class ProgressWriter:
    """rank 0만 쓴다. DynamoDB 오류는 학습을 멈추지 않는다."""

    def __init__(self, enabled):
        self.table = boto3.resource("dynamodb").Table(TABLE) if enabled and TABLE else None

    def write(self, **fields):
        if not self.table:
            return
        fields["updated_at"] = int(time.time() * 1000)
        names = {f"#{k}": k for k in fields}
        # boto3는 float를 받지 않는다(DynamoDB N 타입은 Decimal).
        values = {f":{k}": (Decimal(str(v)) if isinstance(v, float) else v) for k, v in fields.items()}
        try:
            self.table.update_item(
                Key={"job_id": JOB_ID},
                UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in fields),
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
            )
        except Exception as e:  # noqa: BLE001
            print(f"progress write failed: {e}", flush=True)


def main():
    args = parse_args()
    dist.init_process_group("nccl")
    rank = dist.get_rank()
    device = torch.device("cuda", int(os.environ.get("LOCAL_RANK", 0)))
    torch.cuda.set_device(device)

    x, y = load_data(device)
    model = DDP(nn.Sequential(nn.Linear(x.shape[1], 256), nn.ReLU(), nn.Linear(256, 1)).to(device),
                device_ids=[device.index])
    opt = torch.optim.Adam(model.parameters(), lr=args.lr)
    loss_fn = nn.MSELoss()

    start, path = latest_checkpoint()
    if path:
        state = torch.load(path, map_location=device)
        model.module.load_state_dict(state["model"])
        opt.load_state_dict(state["opt"])
        print(f"[rank {rank}] resumed from {path} (step {start})", flush=True)

    progress = ProgressWriter(enabled=rank == 0)
    progress.write(status="RUNNING", step=start, total_steps=args.total_steps,
                   last_checkpoint_step=start, node_name=NODE)

    g = torch.Generator(device=device).manual_seed(1000 + rank)
    try:
        for step in range(start + 1, args.total_steps + 1):
            idx = torch.randint(0, x.shape[0], (args.batch_size,), device=device, generator=g)
            opt.zero_grad()
            loss = loss_fn(model(x[idx]), y[idx])
            loss.backward()
            opt.step()

            fields = {"step": step, "loss": float(loss.item())}
            if step % args.ckpt_every == 0:
                if rank == 0:
                    save_checkpoint(step, model, opt)
                    fields["last_checkpoint_step"] = step
                dist.barrier()
            progress.write(**fields)

        progress.write(status="COMPLETED")
    except Exception:
        progress.write(status="FAILED")
        raise
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
