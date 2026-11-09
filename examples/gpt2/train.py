"""GPT-2 small(124M) DDP 파인튜닝 예시 (GPU 노드 2대 × GPU 1개, torchrun으로 실행).

Hugging Face transformers의 사전학습 가중치 "gpt2"를 받아 $DATA_DIR의 텍스트로 이어서 학습한다
(causal LM, 다음 토큰 예측). 컨테이너 시작 시 job.yaml이 transformers를 설치한다.

규칙 (인프라가 기대하는 것, ../train.py와 같다):
  - S3 코드를 갖지 않는다. 경로만 쓴다: 코드 $CODE_DIR, 데이터 $DATA_DIR, 체크포인트 $CKPT_DIR.
  - 모델은 S3에 하나만 저장한다. DDP는 모든 rank의 가중치가 같으므로 rank 0만 체크포인트를 쓴다.
  - 체크포인트는 메모리에 직렬화한 뒤 새 파일에 한 번에 쓴다 (Mountpoint는 rename·append 미지원).
    시작 시 모든 rank가 rank 0이 고른 최신 체크포인트에서 재개한다.
  - rank 0이 매 step $PROGRESS_FILE에 현재 상태(JSON)를 덮어쓴다. DynamoDB에는 sidecar가 쓴다.

T4(g4dn)는 bf16이 없어서 fp16 autocast + GradScaler를 쓴다.
torchrun 없이 `python train.py`로 실행하면 프로세스 1개(world size 1)로 돈다.
"""

import argparse
import glob
import io
import json
import math
import os
import re
import socket

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
from transformers import AutoModelForCausalLM, AutoTokenizer

JOB_ID = os.environ["JOB_ID"]
CKPT_DIR = os.environ.get("CKPT_DIR", "/ckpt")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
PROGRESS_FILE = os.environ.get("PROGRESS_FILE", "")
CKPT_RE = re.compile(r"ckpt-(\d+)\.pt$")
KEEP_CHECKPOINTS = 2


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model", default="gpt2", help="Hugging Face 모델 이름 (gpt2 = GPT-2 small 124M)")
    p.add_argument("--total-steps", type=int, default=2000)
    p.add_argument("--ckpt-every", type=int, default=250)
    p.add_argument("--batch-size", type=int, default=4, help="rank당 시퀀스 수 (T4 16GB 기준)")
    p.add_argument("--block-size", type=int, default=1024, help="시퀀스 길이 (GPT-2 최대 1024)")
    p.add_argument("--lr", type=float, default=3e-5)
    p.add_argument("--warmup-steps", type=int, default=100)
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


def load_tokens(tokenizer, block_size):
    """$DATA_DIR의 .txt를 이름순으로 이어 붙여 토큰 ID 하나의 긴 배열로 만든다. 모든 rank가 같은 결과를 얻는다."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.txt")))
    if not files:
        raise SystemExit(f"{DATA_DIR}에 .txt 파일이 없다 (make_data.py로 train.txt를 만들어 올린다)")
    text = "".join(open(f, encoding="utf-8").read() for f in files)
    # 전체를 한 번에 토크나이즈한다. 모델 최대 길이(1024) 경고는 block_size로 잘라 쓰므로 끈다.
    ids = tokenizer(text, return_attention_mask=False, verbose=False)["input_ids"]
    if len(ids) <= block_size:
        raise SystemExit(f"토큰 {len(ids)}개: block_size {block_size}보다 길어야 한다")
    return torch.tensor(ids, dtype=torch.long)


def lr_at(step, args):
    # warmup 후 cosine으로 lr의 10%까지 줄인다.
    if step <= args.warmup_steps:
        return args.lr * step / args.warmup_steps
    t = (step - args.warmup_steps) / max(1, args.total_steps - args.warmup_steps)
    return args.lr * (0.1 + 0.9 * 0.5 * (1 + math.cos(math.pi * t)))


def checkpoint_steps():
    return sorted(int(m.group(1)) for p in glob.glob(os.path.join(CKPT_DIR, "ckpt-*.pt"))
                  if (m := CKPT_RE.search(p)))


def resume_step(rank, device):
    """rank 0이 고른 최신 체크포인트 step을 모든 rank에 알린다. rank마다 목록을 따로 보면 어긋날 수 있다."""
    steps = checkpoint_steps() if rank == 0 else []
    t = torch.tensor([steps[-1] if steps else 0], dtype=torch.long, device=device)
    dist.broadcast(t, src=0)
    return int(t.item())


def save_checkpoint(step, model, opt, scaler):
    # rank 0만 호출한다. 메모리에 직렬화한 뒤 새 파일에 순차로 한 번에 쓴다 (Mountpoint 규칙).
    # GPT-2 small + Adam 상태라 약 1.5GB다.
    buf = io.BytesIO()
    torch.save({"step": step, "model": model.state_dict(), "opt": opt.state_dict(),
                "scaler": scaler.state_dict()}, buf)
    with open(os.path.join(CKPT_DIR, f"ckpt-{step}.pt"), "wb") as f:
        f.write(buf.getvalue())
    for s in checkpoint_steps()[:-KEEP_CHECKPOINTS]:
        os.remove(os.path.join(CKPT_DIR, f"ckpt-{s}.pt"))


def save_model(model):
    # 학습이 끝나면 가중치만 model.pt로 한 번 쓴다 (rank 0). 약 500MB.
    # 불러올 때: AutoModelForCausalLM.from_pretrained("gpt2").load_state_dict(torch.load("model.pt"))
    buf = io.BytesIO()
    torch.save(model.state_dict(), buf)
    with open(os.path.join(CKPT_DIR, "model.pt"), "wb") as f:
        f.write(buf.getvalue())


class ProgressFile:
    """현재 상태를 $PROGRESS_FILE에 덮어쓴다. rank 0이 아니거나 경로가 없으면 아무것도 하지 않는다.

    emptyDir(로컬 디스크)이라 임시 파일 후 rename으로 바꿔 sidecar가 반쯤 쓴 파일을 읽지 않게 한다.
    """

    def __init__(self, enabled):
        self.path = PROGRESS_FILE if enabled else ""
        self.state = {}

    def write(self, **fields):
        if not self.path:
            return
        self.state.update(fields)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f)
        os.replace(tmp, self.path)


def main():
    args = parse_args()
    rank, world, device = init_distributed()
    print(f"rank {rank}/{world} on {socket.gethostname()} ({device})", flush=True)
    use_amp = device.type == "cuda"

    tokenizer = AutoTokenizer.from_pretrained(args.model)
    tokens = load_tokens(tokenizer, args.block_size)
    if rank == 0:
        print(f"data: {tokens.numel():,} tokens", flush=True)

    net = AutoModelForCausalLM.from_pretrained(args.model).to(device)
    opt = torch.optim.AdamW(net.parameters(), lr=args.lr, weight_decay=0.1)
    scaler = torch.amp.GradScaler("cuda", enabled=use_amp)

    # 모든 rank가 같은 파일을 읽어 가중치·optimizer·scaler 상태를 맞춘다.
    start = resume_step(rank, device)
    if start:
        state = torch.load(os.path.join(CKPT_DIR, f"ckpt-{start}.pt"), map_location=device)
        net.load_state_dict(state["model"])
        opt.load_state_dict(state["opt"])
        scaler.load_state_dict(state["scaler"])
        print(f"rank {rank}: resumed from ckpt-{start}.pt", flush=True)
    model = DDP(net, device_ids=[device.index] if device.type == "cuda" else None)
    model.train()

    progress = ProgressFile(enabled=rank == 0)
    progress.write(status="RUNNING", step=start, total_steps=args.total_steps,
                   last_checkpoint_step=start)

    # rank마다 다른 위치의 시퀀스를 뽑는다. DDP가 gradient를 평균하므로 실효 batch = batch_size × world.
    g = torch.Generator().manual_seed(1000 + start * world + rank)
    try:
        for step in range(start + 1, args.total_steps + 1):
            for group in opt.param_groups:
                group["lr"] = lr_at(step, args)
            ix = torch.randint(0, tokens.numel() - args.block_size, (args.batch_size,), generator=g)
            x = torch.stack([tokens[i:i + args.block_size] for i in ix.tolist()]).to(device)

            opt.zero_grad(set_to_none=True)
            with torch.autocast(device.type, dtype=torch.float16, enabled=use_amp):
                loss = model(input_ids=x, labels=x).loss  # labels를 한 칸 밀어 다음 토큰을 맞히는 건 모델이 한다
            scaler.scale(loss).backward()
            scaler.unscale_(opt)
            torch.nn.utils.clip_grad_norm_(net.parameters(), 1.0)
            scaler.step(opt)
            scaler.update()

            if rank == 0:
                fields = {"step": step, "loss": float(loss.item())}
                if step % args.ckpt_every == 0:
                    save_checkpoint(step, net, opt, scaler)
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
