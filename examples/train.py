"""DDP 예시 학습 스크립트 (GPU 노드 2대 × GPU 1개, torchrun으로 실행).

규칙 (인프라가 기대하는 것):
  - S3 코드를 갖지 않는다. 경로만 쓴다: 코드 $CODE_DIR, 데이터 $DATA_DIR, 체크포인트 $CKPT_DIR.
    세 경로는 Mountpoint for S3로 마운트된 작업 전용 디렉터리이고, 두 Pod가 같은 S3 경로를 본다.
  - 모델은 S3에 하나만 저장한다. DDP는 모든 rank의 가중치가 같으므로 rank 0만 체크포인트를 쓴다.
  - 체크포인트는 새 파일을 처음부터 끝까지 한 번에 쓴다. Mountpoint는 rename·append를 지원하지 않으므로
    임시 파일 후 rename 방식은 쓰지 않는다. 시작 시 모든 rank가 rank 0이 고른 최신 체크포인트에서 재개한다.
  - rank 0이 매 step DynamoDB($PROGRESS_TABLE, PK job_id)에 진행 상태를 덮어쓴다.

torchrun 없이 `python train.py`로 실행하면 프로세스 1개(world size 1)로 돈다.
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
    p.add_argument("--batch-size", type=int, default=256, help="rank당 batch 크기")
    p.add_argument("--lr", type=float, default=1e-3)
    return p.parse_args()


def init_distributed():
    """torchrun이 넣는 RANK·WORLD_SIZE·LOCAL_RANK를 쓴다. 없으면 단일 프로세스로 띄운다."""
    for k, v in {"RANK": "0", "WORLD_SIZE": "1", "LOCAL_RANK": "0",
                 "MASTER_ADDR": "127.0.0.1", "MASTER_PORT": "29500"}.items():
        os.environ.setdefault(k, v)
    if torch.cuda.is_available():
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)
        device = torch.device("cuda", local_rank)
        backend = "nccl"
    else:
        device = torch.device("cpu")
        backend = "gloo"
    dist.init_process_group(backend=backend)
    return dist.get_rank(), dist.get_world_size(), device


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


def checkpoint_steps():
    return sorted(int(m.group(1)) for p in glob.glob(os.path.join(CKPT_DIR, "ckpt-*.pt"))
                  if (m := CKPT_RE.search(p)))


def resume_step(rank, device):
    """rank 0이 고른 최신 체크포인트 step을 모든 rank에 알린다. rank마다 목록을 따로 보면 어긋날 수 있다."""
    steps = checkpoint_steps() if rank == 0 else []
    t = torch.tensor([steps[-1] if steps else 0], dtype=torch.long, device=device)
    dist.broadcast(t, src=0)
    return int(t.item())


def save_checkpoint(step, model, opt):
    # rank 0만 호출한다. 메모리에 직렬화한 뒤 새 파일에 순차로 한 번에 쓴다 (Mountpoint 규칙).
    buf = io.BytesIO()
    torch.save({"step": step, "model": model.state_dict(), "opt": opt.state_dict()}, buf)
    with open(os.path.join(CKPT_DIR, f"ckpt-{step}.pt"), "wb") as f:
        f.write(buf.getvalue())
    for s in checkpoint_steps()[:-KEEP_CHECKPOINTS]:
        os.remove(os.path.join(CKPT_DIR, f"ckpt-{s}.pt"))


def save_model(model):
    # 학습이 끝나면 가중치만 model.pt로 한 번 쓴다 (rank 0).
    buf = io.BytesIO()
    torch.save(model.state_dict(), buf)
    with open(os.path.join(CKPT_DIR, "model.pt"), "wb") as f:
        f.write(buf.getvalue())


class ProgressWriter:
    """DynamoDB 오류는 학습을 멈추지 않는다. rank 0이 아니면 아무것도 쓰지 않는다."""

    def __init__(self, enabled):
        self.table = None
        if not TABLE or not enabled:
            return
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        try:
            self.table = boto3.resource("dynamodb", region_name=region).Table(TABLE)
        except Exception as e:  # noqa: BLE001
            print(f"progress disabled: {e}", flush=True)

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
    rank, world, device = init_distributed()
    print(f"rank {rank}/{world} on {NODE or 'local'} ({device})", flush=True)

    x, y = load_data(device)
    net = nn.Sequential(nn.Linear(x.shape[1], 256), nn.ReLU(), nn.Linear(256, 1)).to(device)
    opt = torch.optim.Adam(net.parameters(), lr=args.lr)
    loss_fn = nn.MSELoss()

    # 모든 rank가 같은 파일을 읽어 가중치와 optimizer 상태(Adam moment)를 맞춘다.
    start = resume_step(rank, device)
    if start:
        state = torch.load(os.path.join(CKPT_DIR, f"ckpt-{start}.pt"), map_location=device)
        net.load_state_dict(state["model"])
        opt.load_state_dict(state["opt"])
        print(f"rank {rank}: resumed from ckpt-{start}.pt", flush=True)
    model = DDP(net, device_ids=[device.index] if device.type == "cuda" else None)

    progress = ProgressWriter(enabled=rank == 0)
    progress.write(status="RUNNING", step=start, total_steps=args.total_steps,
                   last_checkpoint_step=start, node_name=NODE)

    # rank마다 다른 batch를 뽑는다. DDP가 gradient를 평균하므로 실효 batch = batch_size × world.
    g = torch.Generator(device=device).manual_seed(1000 + start * world + rank)
    try:
        for step in range(start + 1, args.total_steps + 1):
            idx = torch.randint(0, x.shape[0], (args.batch_size,), device=device, generator=g)
            opt.zero_grad()
            loss = loss_fn(model(x[idx]), y[idx])
            loss.backward()
            opt.step()

            if rank == 0:
                fields = {"step": step, "loss": float(loss.item())}
                if step % args.ckpt_every == 0:
                    save_checkpoint(step, net, opt)
                    fields["last_checkpoint_step"] = step
                    print(f"step {step}: saved ckpt-{step}.pt (loss {fields['loss']:.4f})", flush=True)
                progress.write(**fields)

        if rank == 0:
            save_model(net)
        dist.barrier()
        progress.write(status="COMPLETED")
    except Exception:
        progress.write(status="FAILED")
        raise
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
