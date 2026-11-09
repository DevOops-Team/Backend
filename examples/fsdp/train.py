"""GPT-2 FSDP · DDP 학습 (연동 테스트용). 설정은 config.json 하나로 받는다 (Confluence `학습 파라미터 주입 방식`).

백엔드가 만든 JobSet의 torchrun이 Pod마다 이 파일을 실행한다.
  - TRAIN_CONFIG(/code/config.json)을 읽고 CONFIG_SHA256 · 키 · world_size를 확인한다.
  - DATA_DIR의 text-jsonl을 토큰화해 DistributedSampler로 rank마다 나눈다.
  - MODEL_DIR에서 모델을 읽어 training_mode에 따라 FSDP(GPT2Block 단위) 또는 DDP로 감싼다.
  - rank 0이 매 step PROGRESS_FILE에 상태를 쓰면 progress-reporter가 DynamoDB로 옮긴다.
  - checkpoint_every_steps마다, 그리고 마지막 step에 저장한다. FSDP는 rank마다 DCP 조각, DDP는 rank 0이 파일 하나.

임시 구성 (합의안과 다른 점):
  - 이미지 안이 아니라 S3의 /code/train.py다.
  - MODEL_DIR을 쓴다. 합의안은 모델 이름만 넘긴다.
  - Mountpoint가 rename을 지원하지 않아 DCP를 CKPT_LOCAL_DIR에 쓴 뒤 CKPT_DIR로 복사한다.
  - ckpt-sidecar가 없어 .approved · STOP을 처리하지 않고 재개하지 않는다. .done만 남긴다.
"""

from __future__ import annotations

import glob
import hashlib
import json
import math
import os
import shutil
import socket
from dataclasses import dataclass, fields

SCHEMA_VERSION = 1
CHECKPOINT_FORMATS = {"fsdp": "fsdp-dcp", "ddp": "ddp-pt"}
BLOCK_SIZE = 1024  # GPT-2 최대 시퀀스 길이. 모델 고정값
COPY_CHUNK = 16 << 20


@dataclass(frozen=True)
class DatasetRef:
    uri: str
    format: str
    manifest_sha256: str


@dataclass(frozen=True)
class TrainConfig:  # 기본값 없음: 백엔드가 모든 키를 채운다
    model: str
    training_mode: str
    checkpoint_format: str
    dataset: DatasetRef
    epochs: int
    max_steps: int | None
    batch_size_per_gpu: int
    learning_rate: float
    warmup_steps: int
    checkpoint_every_steps: int | None
    seed: int
    world_size: int


def load_config() -> tuple[TrainConfig, str]:
    with open(os.environ["TRAIN_CONFIG"], "rb") as f:
        raw = f.read()
    digest = hashlib.sha256(raw).hexdigest()
    expected = os.environ.get("CONFIG_SHA256")
    if expected and expected != digest:
        raise SystemExit(f"config hash mismatch: {digest} != {expected}")

    data = json.loads(raw)
    if data.pop("schema_version", None) != SCHEMA_VERSION:
        raise SystemExit(f"config schema_version must be {SCHEMA_VERSION}")
    names = {f.name for f in fields(TrainConfig)}
    if set(data) != names:  # 오타 · 누락을 조용히 넘기지 않는다
        raise SystemExit(f"config keys: missing {names - set(data)}, unknown {set(data) - names}")
    data["dataset"] = DatasetRef(**data["dataset"])
    cfg = TrainConfig(**data)
    want = CHECKPOINT_FORMATS.get(cfg.training_mode)
    if want is None:  # 다른 방식으로 돌면서 요청한 방식으로 보이는 일을 막는다
        raise SystemExit(f"this train.py does not implement {cfg.training_mode}")
    if cfg.checkpoint_format != want:
        raise SystemExit(f"{cfg.training_mode} needs checkpoint_format {want}, got {cfg.checkpoint_format}")
    if cfg.dataset.format != "text-jsonl":
        raise SystemExit(f"{cfg.model} does not support dataset format {cfg.dataset.format}")
    return cfg, digest


def total_steps(steps_per_epoch: int, epochs: int, max_steps: int | None) -> int:
    total = steps_per_epoch * epochs
    return min(total, max_steps) if max_steps is not None else total


def lr_at(step: int, base: float, warmup: int, total: int) -> float:
    # warmup 후 cosine으로 lr의 10%까지 줄인다.
    if step <= warmup:
        return base * step / warmup
    t = (step - warmup) / max(1, total - warmup)
    return base * (0.1 + 0.9 * 0.5 * (1 + math.cos(math.pi * t)))


def split_blocks(ids: list[int], size: int) -> list[list[int]]:
    n = len(ids) // size
    if n == 0:
        raise SystemExit(f"dataset too small: {len(ids)} tokens, need at least {size}")
    return [ids[i * size : (i + 1) * size] for i in range(n)]


def data_files(data_dir: str) -> list[str]:
    # 정렬: rank마다 순서가 다르면 DistributedSampler가 나눌 때 샘플이 겹치거나 빠진다
    paths = sorted(glob.glob(os.path.join(data_dir, "*.jsonl")))
    if not paths:
        raise SystemExit(f"no .jsonl files in {data_dir}")
    return paths


def step_name(step: int) -> str:
    return f"step-{step:08d}"


class ProgressFile:
    """현재 상태를 PROGRESS_FILE에 덮어쓴다. 경로가 없으면(rank 0 외) 아무것도 하지 않는다.

    emptyDir(로컬 디스크)이라 임시 파일 후 rename으로 바꿔 sidecar가 반쯤 쓴 파일을 읽지 않게 한다.
    """

    def __init__(self, path: str, generation: int):
        self.path = path
        self.state = {"generation": generation}

    def write(self, **values):
        if not self.path:
            return
        self.state.update(values)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f)
        os.replace(tmp, self.path)


def publish_checkpoint(local_dir: str, remote_dir: str) -> None:
    """내 rank가 쓴 DCP 파일을 Mountpoint(/ckpt)로 옮긴다. 새 파일에 순차로만 쓴다 (Mountpoint 규칙)."""
    os.makedirs(remote_dir, exist_ok=True)
    for name in sorted(os.listdir(local_dir)):
        with open(os.path.join(local_dir, name), "rb") as src, open(os.path.join(remote_dir, name), "wb") as dst:
            while chunk := src.read(COPY_CHUNK):
                dst.write(chunk)
    shutil.rmtree(local_dir)


def read_tokens(paths, tokenizer):
    ids = []
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                ids.extend(tokenizer(json.loads(line)["text"])["input_ids"])
                ids.append(tokenizer.eos_token_id)
    return ids


def save_fsdp(step, model, optimizer, cfg_hash, local_dir):
    # 모든 rank가 자기 조각을 쓴다. 모델 상태가 GPU마다 다르기 때문이다.
    import torch.distributed.checkpoint as dcp
    from torch.distributed.fsdp import FullyShardedDataParallel as FSDP
    from torch.distributed.fsdp import StateDictType

    with FSDP.state_dict_type(model, StateDictType.SHARDED_STATE_DICT):
        state = {
            "model": model.state_dict(),
            "optim": FSDP.optim_state_dict(model, optimizer),
            "step": step,
            "config_sha256": cfg_hash,
        }
    dcp.save(state, checkpoint_id=local_dir)


def save_ddp(step, model, optimizer, cfg_hash, local_dir):
    # 모든 rank의 상태가 같으므로 rank 0만 파일 하나를 쓴다.
    import torch

    state = {
        "model": model.module.state_dict(),
        "optim": optimizer.state_dict(),
        "step": step,
        "config_sha256": cfg_hash,
    }
    torch.save(state, os.path.join(local_dir, "checkpoint.pt"))


def save_checkpoint(mode, step, model, optimizer, cfg_hash, rank):
    # 로컬에 쓴 뒤 /ckpt로 복사한다. rank 0이 다 모인 뒤 .done을 마지막에 쓴다.
    import torch.distributed as dist

    name = step_name(step)
    remote_dir = os.path.join(os.environ["CKPT_DIR"], name)
    if mode == "fsdp" or rank == 0:
        local_dir = os.path.join(os.environ["CKPT_LOCAL_DIR"], name)
        os.makedirs(local_dir, exist_ok=True)
        (save_fsdp if mode == "fsdp" else save_ddp)(step, model, optimizer, cfg_hash, local_dir)
        publish_checkpoint(local_dir, remote_dir)
    dist.barrier()
    if rank == 0:
        with open(remote_dir + ".done", "wb"):
            pass


def wrap_model(mode, net, device):
    from torch.nn.parallel import DistributedDataParallel as DDP

    if mode == "ddp":
        return DDP(net.to(device), device_ids=[device.index])

    from torch.distributed.fsdp import FullyShardedDataParallel as FSDP
    from torch.distributed.fsdp.wrap import ModuleWrapPolicy
    from transformers.models.gpt2.modeling_gpt2 import GPT2Block

    # use_orig_params: optimizer와 체크포인트가 원래 파라미터 이름을 그대로 쓴다.
    return FSDP(net, auto_wrap_policy=ModuleWrapPolicy({GPT2Block}), device_id=device, use_orig_params=True)


def clip_gradients(mode, model, max_norm):
    import torch

    if mode == "ddp":
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm)
    else:
        model.clip_grad_norm_(max_norm)  # FSDP는 조각을 모아 전체 노름을 계산한다


def main():
    import torch
    import torch.distributed as dist
    from torch.utils.data import DataLoader, DistributedSampler, TensorDataset
    from transformers import AutoModelForCausalLM, AutoTokenizer

    cfg, cfg_hash = load_config()
    dist.init_process_group("nccl")
    rank, world = dist.get_rank(), dist.get_world_size()
    progress = ProgressFile(os.environ.get("PROGRESS_FILE", "") if rank == 0 else "", int(os.environ["GENERATION"]))
    try:
        if world != cfg.world_size:
            raise SystemExit(f"WORLD_SIZE {world} != config.world_size {cfg.world_size}")
        local_rank = int(os.environ["LOCAL_RANK"])
        torch.cuda.set_device(local_rank)
        device = torch.device("cuda", local_rank)
        torch.manual_seed(cfg.seed)
        print(f"rank {rank}/{world} on {socket.gethostname()}: {cfg.training_mode}", flush=True)
        progress.write(status="RUNNING", step=0, total_steps=0, last_checkpoint_step=0)

        model_dir = os.environ["MODEL_DIR"]
        tokenizer = AutoTokenizer.from_pretrained(model_dir, local_files_only=True)
        blocks = split_blocks(read_tokens(data_files(os.environ["DATA_DIR"]), tokenizer), BLOCK_SIZE)
        dataset = TensorDataset(torch.tensor(blocks, dtype=torch.long))
        sampler = DistributedSampler(dataset, num_replicas=world, rank=rank, seed=cfg.seed, drop_last=True)
        loader = DataLoader(dataset, batch_size=cfg.batch_size_per_gpu, sampler=sampler, drop_last=True)
        steps_per_epoch = len(loader)
        total = total_steps(steps_per_epoch, cfg.epochs, cfg.max_steps)
        ckpt_every = cfg.checkpoint_every_steps or steps_per_epoch  # null이면 epoch마다
        progress.write(total_steps=total)
        if rank == 0:
            print(f"{len(blocks)} blocks, {steps_per_epoch} steps/epoch, {total} steps", flush=True)

        net = AutoModelForCausalLM.from_pretrained(model_dir, local_files_only=True)
        net.config.use_cache = False
        model = wrap_model(cfg.training_mode, net, device)
        model.train()
        optimizer = torch.optim.AdamW(model.parameters(), lr=cfg.learning_rate, weight_decay=0.1)

        step = 0
        epoch = 0
        while step < total:
            sampler.set_epoch(epoch)
            for (batch,) in loader:
                step += 1
                for group in optimizer.param_groups:
                    group["lr"] = lr_at(step, cfg.learning_rate, cfg.warmup_steps, total)
                x = batch.to(device)
                optimizer.zero_grad(set_to_none=True)
                loss = model(input_ids=x, labels=x).loss  # labels를 한 칸 밀어 다음 토큰을 맞히는 건 모델이 한다
                loss.backward()
                clip_gradients(cfg.training_mode, model, 1.0)
                optimizer.step()

                reduced = loss.detach().clone()  # 화면에는 전체 rank 평균을 보여 준다
                dist.all_reduce(reduced, op=dist.ReduceOp.SUM)
                values = {"step": step, "loss": float(reduced.item()) / world}
                if step % ckpt_every == 0 or step == total:  # 마지막 step은 항상 저장
                    save_checkpoint(cfg.training_mode, step, model, optimizer, cfg_hash, rank)
                    values["last_checkpoint_step"] = step
                    if rank == 0:
                        print(f"step {step}: saved {step_name(step)} (loss {values['loss']:.4f})", flush=True)
                progress.write(**values)
                if step >= total:
                    break
            epoch += 1

        dist.barrier()
        progress.write(status="COMPLETED")
    except BaseException:
        progress.write(status="FAILED")
        raise
    finally:
        dist.destroy_process_group()


if __name__ == "__main__":
    main()
