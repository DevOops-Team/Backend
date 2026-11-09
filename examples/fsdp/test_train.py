"""train.py와 seed_assets.py에서 GPU 없이 확인할 수 있는 부분의 테스트.

실행: python3 -m unittest discover -s examples/fsdp
torch가 없어도 돈다. FSDP 동작 자체는 EKS에서 실제로 돌려 봐야 확인된다.
"""

import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

import seed_assets
import train

VALID = {
    "schema_version": 1,
    "model": "gpt2",
    "training_mode": "fsdp",
    "checkpoint_format": "fsdp-dcp",
    "dataset": {"uri": "s3://b/datasets/ds/v1/", "format": "text-jsonl", "manifest_sha256": "aa"},
    "epochs": 3,
    "max_steps": None,
    "batch_size_per_gpu": 4,
    "learning_rate": 3e-05,
    "warmup_steps": 100,
    "checkpoint_every_steps": 250,
    "seed": 42,
    "world_size": 2,
}


class LoadConfig(unittest.TestCase):
    def write(self, data):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "config.json")
        raw = json.dumps(data).encode()
        with open(path, "wb") as f:
            f.write(raw)
        return path, hashlib.sha256(raw).hexdigest()

    def env(self, path, sha):
        old = dict(os.environ)
        self.addCleanup(lambda: (os.environ.clear(), os.environ.update(old)))
        os.environ["TRAIN_CONFIG"] = path
        os.environ["CONFIG_SHA256"] = sha

    def test_reads_every_key(self):
        path, sha = self.write(VALID)
        self.env(path, sha)
        cfg, digest = train.load_config()
        self.assertEqual(cfg.training_mode, "fsdp")
        self.assertEqual(cfg.dataset.format, "text-jsonl")
        self.assertEqual(digest, sha)

    def test_rejects_hash_mismatch(self):
        path, _ = self.write(VALID)
        self.env(path, "0" * 64)
        with self.assertRaisesRegex(SystemExit, "hash mismatch"):
            train.load_config()

    def test_rejects_missing_and_unknown_keys(self):
        data = {k: v for k, v in VALID.items() if k != "seed"}
        data["sed"] = 1
        path, sha = self.write(data)
        self.env(path, sha)
        with self.assertRaisesRegex(SystemExit, "seed"):
            train.load_config()

    def test_rejects_wrong_schema(self):
        path, sha = self.write(dict(VALID, schema_version=9))
        self.env(path, sha)
        with self.assertRaisesRegex(SystemExit, "schema_version"):
            train.load_config()

    def test_accepts_ddp_with_its_own_format(self):
        path, sha = self.write(dict(VALID, training_mode="ddp", checkpoint_format="ddp-pt"))
        self.env(path, sha)
        cfg, _ = train.load_config()
        self.assertEqual(cfg.training_mode, "ddp")

    def test_rejects_unknown_mode(self):
        path, sha = self.write(dict(VALID, training_mode="horovod"))
        self.env(path, sha)
        with self.assertRaisesRegex(SystemExit, "horovod"):
            train.load_config()

    def test_rejects_ddp_with_fsdp_format(self):
        path, sha = self.write(dict(VALID, training_mode="ddp"))
        self.env(path, sha)
        with self.assertRaisesRegex(SystemExit, "ddp-pt"):
            train.load_config()

    def test_rejects_wrong_checkpoint_format(self):
        path, sha = self.write(dict(VALID, checkpoint_format="x"))
        self.env(path, sha)
        with self.assertRaisesRegex(SystemExit, "fsdp-dcp"):
            train.load_config()


class TotalSteps(unittest.TestCase):
    def test_epochs_decide_and_max_steps_caps(self):
        self.assertEqual(train.total_steps(41, 10, None), 410)
        self.assertEqual(train.total_steps(41, 10, 100), 100)


class LearningRate(unittest.TestCase):
    def test_warms_up_then_decays_to_ten_percent(self):
        self.assertAlmostEqual(train.lr_at(50, 1.0, 100, 1000), 0.5)
        self.assertAlmostEqual(train.lr_at(100, 1.0, 100, 1000), 1.0)
        self.assertAlmostEqual(train.lr_at(1000, 1.0, 100, 1000), 0.1)

    def test_short_run_without_warmup_room_still_works(self):
        self.assertGreater(train.lr_at(5, 1.0, 100, 5), 0)


class Blocks(unittest.TestCase):
    def test_cuts_tokens_into_whole_blocks(self):
        self.assertEqual(train.split_blocks(list(range(10)), 4), [[0, 1, 2, 3], [4, 5, 6, 7]])

    def test_too_little_data_is_an_error(self):
        with self.assertRaisesRegex(SystemExit, "too small"):
            train.split_blocks([1, 2], 4)


class DataFiles(unittest.TestCase):
    def test_lists_jsonl_sorted_so_every_rank_agrees(self):
        d = tempfile.mkdtemp()
        for name in ("b.jsonl", "a.jsonl", "manifest.json"):
            Path(d, name).write_text("{}\n")
        self.assertEqual([os.path.basename(p) for p in train.data_files(d)], ["a.jsonl", "b.jsonl"])

    def test_no_files_is_an_error(self):
        with self.assertRaisesRegex(SystemExit, "no .jsonl"):
            train.data_files(tempfile.mkdtemp())


class ProgressFile(unittest.TestCase):
    def test_keeps_earlier_fields_and_leaves_no_temp_file(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "progress.json")
        p = train.ProgressFile(path, generation=1)
        p.write(status="RUNNING", step=0, total_steps=10)
        p.write(step=3, loss=2.5)
        self.assertEqual(
            json.loads(Path(path).read_text()),
            {"generation": 1, "status": "RUNNING", "step": 3, "total_steps": 10, "loss": 2.5},
        )
        self.assertEqual(os.listdir(d), ["progress.json"])

    def test_without_path_writes_nothing(self):
        train.ProgressFile("", generation=1).write(step=1)


class PublishCheckpoint(unittest.TestCase):
    def test_copies_files_and_frees_local_disk(self):
        local, remote = tempfile.mkdtemp(), tempfile.mkdtemp()
        step_dir = Path(local, "step-00000250")
        step_dir.mkdir()
        (step_dir / "__0_0.distcp").write_bytes(b"shard")
        (step_dir / ".metadata").write_bytes(b"meta")
        train.publish_checkpoint(str(step_dir), os.path.join(remote, "step-00000250"))
        got = sorted(os.listdir(os.path.join(remote, "step-00000250")))
        self.assertEqual(got, [".metadata", "__0_0.distcp"])
        self.assertEqual(Path(remote, "step-00000250", "__0_0.distcp").read_bytes(), b"shard")
        self.assertFalse(step_dir.exists())


class StepName(unittest.TestCase):
    def test_uses_eight_digits(self):
        self.assertEqual(train.step_name(250), "step-00000250")


class Seed(unittest.TestCase):
    def test_paragraphs_become_jsonl_records(self):
        lines = seed_assets.to_jsonl("First\nline\n\nSecond\n\n\n").splitlines()
        self.assertEqual([json.loads(line) for line in lines], [{"text": "First\nline"}, {"text": "Second"}])

    def test_manifest_lists_sizes_and_hashes(self):
        d = tempfile.mkdtemp()
        Path(d, "train-000.jsonl").write_bytes(b"abc")
        m = seed_assets.build_manifest(d, "ds-x", 1, "X", "text-jsonl")
        self.assertEqual(m["dataset_id"], "ds-x")
        self.assertEqual(m["format"], "text-jsonl")
        self.assertEqual(m["files"], [{"name": "train-000.jsonl", "size_bytes": 3, "sha256": hashlib.sha256(b"abc").hexdigest()}])

    def test_manifest_excludes_itself(self):
        d = tempfile.mkdtemp()
        Path(d, "train-000.jsonl").write_bytes(b"abc")
        Path(d, "manifest.json").write_bytes(b"{}")
        self.assertEqual(len(seed_assets.build_manifest(d, "ds-x", 1, "X", "text-jsonl")["files"]), 1)


if __name__ == "__main__":
    unittest.main()
