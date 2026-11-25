package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

// bucketObjects is an in-memory bucket that keeps what Put writes.
type bucketObjects struct{ fakeObjects }

func (b bucketObjects) Put(_ context.Context, key string, body io.Reader) error {
	data, err := io.ReadAll(body)
	b.fakeObjects[key] = string(data)
	return err
}

type submitted struct{ js *unstructured.Unstructured }

func (s *submitted) Submit(_ context.Context, js *unstructured.Unstructured) error {
	s.js = js
	return nil
}
func (s *submitted) List(context.Context) ([]model.JobStatus, error)           { return nil, nil }
func (s *submitted) Delete(context.Context, string) error                      { return nil }
func (s *submitted) Stop(context.Context, string, time.Time) ([]string, error) { return nil, nil }

type noFencing struct{}

func (noFencing) Create(context.Context, string, string) error          { return nil }
func (noFencing) Restart(context.Context, string, int, time.Time) error { return nil }

type capturedLifecycle struct{ got *[]model.SubmittedEvent }

func (c capturedLifecycle) Submitted(_ context.Context, e model.SubmittedEvent) error {
	*c.got = append(*c.got, e)
	return nil
}

type noLifecycle struct{}

func (noLifecycle) Submitted(context.Context, model.SubmittedEvent) error { return nil }

type noRecord struct{}

func (noRecord) Put(context.Context, model.JobEvent) error { return nil }

// parts is an UploadSource over fixed uploads.
type parts []Upload

func (p *parts) Next() (Upload, error) {
	if len(*p) == 0 {
		return Upload{}, io.EOF
	}
	u := (*p)[0]
	*p = (*p)[1:]
	return u, nil
}

const (
	fixedPrefix = "datasets/smollm2-wikitext2/v1/"
	fixedTrain  = "print('train')\n"
)

func hexSum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func fixedManifest(t *testing.T, edit func(*model.DatasetManifest)) string {
	m := model.DatasetManifest{SchemaVersion: 1, Model: "SmolLM2-135M", Format: "text-jsonl", ModelBytes: 269060552, DataBytes: 10926306,
		Files: []model.ManifestEntry{
			{Name: "code/train.py", SHA256: hexSum(fixedTrain), SizeBytes: int64(len(fixedTrain))},
			{Name: "data/wikitext-2-train.jsonl", SHA256: "aa", SizeBytes: 10926306},
			{Name: "model/config.json", SHA256: "bb", SizeBytes: 704},
		}}
	if edit != nil {
		edit(&m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func submitService(t *testing.T, objs fakeObjects, sched *submitted) *JobService {
	cfg, err := settings.Load("")
	if err != nil {
		t.Fatal(err)
	}
	return NewJobService(Config{Namespace: "training", Region: "us-east-2", Bucket: "ckpt",
		Settings: cfg.WithImages("ghcr.io/x/trainer:1", "ghcr.io/x/checkpoint:1"),
		NewID:    func() string { return "a1b2c3d4" }},
		Deps{Objects: bucketObjects{objs}, Scheduler: sched, Fencing: noFencing{}, Lifecycle: noLifecycle{}, Record: noRecord{}})
}

func fixedSpec() model.SubmitSpec {
	return model.SubmitSpec{GPUs: 2, Hyperparameters: map[string]any{"max_steps": float64(600)}, ExpID: "exp-0001",
		Dataset: &model.SpecDataset{ID: "smollm2-wikitext2", Version: 1}}
}

func TestSubmitWithFixedDatasetUploadsNothingAndPointsThePodsAtIt(t *testing.T) {
	manifest := fixedManifest(t, nil)
	objs := fakeObjects{fixedPrefix + "manifest.json": manifest, fixedPrefix + "code/train.py": fixedTrain}
	sched := &submitted{}
	created, err := submitService(t, objs, sched).Submit(context.Background(), fixedSpec(), &parts{})
	if err != nil {
		t.Fatal(err)
	}

	for key := range objs {
		if strings.HasPrefix(key, "datasets/") && !strings.HasPrefix(key, fixedPrefix) {
			t.Errorf("wrote a dataset object %s; a fixed dataset is used in place", key)
		}
	}
	if objs[fixedPrefix+"manifest.json"] != manifest {
		t.Error("the fixed dataset's manifest was rewritten")
	}
	if objs["code/a1b2c3d4/train.py"] != fixedTrain {
		t.Error("train.py was not copied to code/<job id>/ for the file viewer")
	}

	var cfg model.TrainConfig
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Model != "SmolLM2-135M" || cfg.Dataset.URI != "s3://ckpt/"+fixedPrefix || cfg.Dataset.ManifestSHA256 != hexSum(manifest) {
		t.Errorf("config model %q dataset %+v", cfg.Model, cfg.Dataset)
	}
	if cfg.MaxSteps == nil || *cfg.MaxSteps != 600 {
		t.Errorf("max_steps = %v, want 600", cfg.MaxSteps)
	}
	js, _ := json.Marshal(sched.js.Object)
	for _, want := range []string{`"s3://ckpt/` + fixedPrefix + `"`, `"smollm2-wikitext2/v1"`, `"exp-0001"`, hexSum(manifest)} {
		if !strings.Contains(string(js), want) {
			t.Errorf("JobSet lacks %s", want)
		}
	}
}

func TestSubmitWithFixedDatasetRejects(t *testing.T) {
	good := fixedManifest(t, nil)
	cases := []struct {
		name  string
		spec  func(*model.SubmitSpec)
		objs  fakeObjects
		parts parts
	}{
		{name: "a job ID, whose upload goes with the job", spec: func(s *model.SubmitSpec) { s.Dataset.ID = "0da414c1" }},
		{name: "version 0", spec: func(s *model.SubmitSpec) { s.Dataset.Version = 0 }},
		{name: "file parts with it", parts: parts{{Field: "data", Path: "x.jsonl", Body: strings.NewReader("{}")}},
			objs: fakeObjects{fixedPrefix + "manifest.json": good, fixedPrefix + "code/train.py": fixedTrain}},
		{name: "a dataset that is not there", objs: fakeObjects{}},
		{name: "a manifest without train.py",
			objs: fakeObjects{fixedPrefix + "manifest.json": fixedManifest(t, func(m *model.DatasetManifest) { m.Files = m.Files[1:] })}},
		{name: "another data format",
			objs: fakeObjects{fixedPrefix + "manifest.json": fixedManifest(t, func(m *model.DatasetManifest) { m.Format = "parquet" }),
				fixedPrefix + "code/train.py": fixedTrain}},
		{name: "a train.py that differs from the manifest",
			objs: fakeObjects{fixedPrefix + "manifest.json": good, fixedPrefix + "code/train.py": "changed"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := fixedSpec()
			if c.spec != nil {
				c.spec(&spec)
			}
			objs := c.objs
			if objs == nil {
				objs = fakeObjects{}
			}
			sched := &submitted{}
			_, err := submitService(t, objs, sched).Submit(context.Background(), spec, &c.parts)
			if kindOf(err) != KindInvalid {
				t.Fatalf("err = %v, want invalid", err)
			}
			if sched.js != nil {
				t.Error("a JobSet was submitted")
			}
			for key := range objs {
				if strings.HasPrefix(key, "code/") {
					t.Errorf("wrote %s before rejecting", key)
				}
			}
		})
	}
}

func TestSubmitWithUploadStillStoresItAsTheJobsOwnDataset(t *testing.T) {
	objs := fakeObjects{}
	sched := &submitted{}
	up := parts{
		{Field: "model", Path: "gpt2/config.json", Body: strings.NewReader("{}")},
		{Field: "data", Path: "train.jsonl", Body: strings.NewReader(`{"text":"a"}`)},
		{Field: "train", Path: "my_train.py", Body: strings.NewReader(fixedTrain)},
	}
	created, err := submitService(t, objs, sched).Submit(context.Background(), model.SubmitSpec{GPUs: 1}, &up)
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "datasets/a1b2c3d4/v1/"
	var m model.DatasetManifest
	if err := json.Unmarshal([]byte(objs[prefix+"manifest.json"]), &m); err != nil {
		t.Fatal(err)
	}
	if m.Model != "gpt2" || len(m.Files) != 3 || objs[prefix+"code/train.py"] != fixedTrain || objs["code/a1b2c3d4/train.py"] != fixedTrain {
		t.Errorf("manifest %+v", m)
	}
	var cfg model.TrainConfig
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Dataset.URI != "s3://ckpt/"+prefix || cfg.Dataset.ManifestSHA256 != hexSum(objs[prefix+"manifest.json"]) {
		t.Errorf("config dataset %+v", cfg.Dataset)
	}
	if js, _ := json.Marshal(sched.js.Object); !strings.Contains(string(js), `"a1b2c3d4/v1"`) {
		t.Error("JobSet dataset annotation is not the job's own upload")
	}
}

// safetensorsFile is a model file with only a header: the backend never reads past it (GPU-261).
func safetensorsFile(t *testing.T, shapes map[string][]int64) string {
	tensors := map[string]any{"__metadata__": map[string]string{"format": "pt"}}
	for name, shape := range shapes {
		tensors[name] = map[string]any{"dtype": "BF16", "shape": shape, "data_offsets": []int{0, 0}}
	}
	header, err := json.Marshal(tensors)
	if err != nil {
		t.Fatal(err)
	}
	return string(binary.LittleEndian.AppendUint64(nil, uint64(len(header)))) + string(header) + "weights…"
}

// tinyQwen has one decoder layer: q 64×64 · k 16×64 · v 16×64 · o 64×64 · gate · up 128×64 · down 64×128.
var tinyQwen = map[string][]int64{
	"model.embed_tokens.weight":              {1000, 64},
	"model.layers.0.self_attn.q_proj.weight": {64, 64},
	"model.layers.0.self_attn.q_proj.bias":   {64},
	"model.layers.0.self_attn.k_proj.weight": {16, 64},
	"model.layers.0.self_attn.v_proj.weight": {16, 64},
	"model.layers.0.self_attn.o_proj.weight": {64, 64},
	"model.layers.0.mlp.gate_proj.weight":    {128, 64},
	"model.layers.0.mlp.up_proj.weight":      {128, 64},
	"model.layers.0.mlp.down_proj.weight":    {64, 128},
	"lm_head.weight":                         {1000, 64},
}

func qloraObjects(t *testing.T) fakeObjects {
	weights := safetensorsFile(t, tinyQwen)
	manifest := fixedManifest(t, func(m *model.DatasetManifest) {
		m.Files = append(m.Files, model.ManifestEntry{Name: "model/model.safetensors", SHA256: "cc", SizeBytes: int64(len(weights))})
	})
	return fakeObjects{fixedPrefix + "manifest.json": manifest, fixedPrefix + "code/train.py": fixedTrain,
		fixedPrefix + "model/model.safetensors": weights}
}

func TestSubmitQLoRAWritesTheAdapterIntoConfigAndSizesTheCheckpointByIt(t *testing.T) {
	sched := &submitted{}
	spec := fixedSpec()
	spec.Finetune = "qlora"
	created, err := submitService(t, qloraObjects(t), sched).Submit(context.Background(), spec, &parts{})
	if err != nil {
		t.Fatal(err)
	}
	var cfg model.TrainConfig
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SchemaVersion != 3 || cfg.Finetune.Method != "qlora" || cfg.Finetune.Lora == nil || cfg.Finetune.Lora.R != 16 ||
		len(cfg.Finetune.Lora.TargetModules) != 7 {
		t.Errorf("config schema %d finetune %+v, want 3 · qlora with the default LoRA", cfg.SchemaVersion, cfg.Finetune)
	}
	// 어댑터 16 · (128 + 80 + 80 + 128 + 192 · 3) = 15,872개 × 12 B ÷ 2 pods → 1 ms 남짓, 하한 300 s
	if cfg.CheckpointPolicy.EstimatedSaveSeconds != 0 || cfg.CheckpointPolicy.IntervalSeconds != 300 {
		t.Errorf("policy %+v, want the adapter's tiny save and the 300 s floor", cfg.CheckpointPolicy)
	}
	if got := sched.js.GetAnnotations()[model.FinetuneAnnotation]; got != "qlora" {
		t.Errorf("JobSet annotation %s = %q, want qlora", model.FinetuneAnnotation, got)
	}
}

func TestSubmitFullStaysTheDefaultAndCountsEveryParameter(t *testing.T) {
	created, err := submitService(t, qloraObjects(t), &submitted{}).Submit(context.Background(), fixedSpec(), &parts{})
	if err != nil {
		t.Fatal(err)
	}
	var cfg struct {
		Finetune map[string]any `json:"finetune"`
	}
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Finetune["method"] != "full" || cfg.Finetune["lora"] != nil {
		t.Errorf("finetune = %v, want full with lora null (train.py rejects missing keys)", cfg.Finetune)
	}
}

func TestSubmitQLoRARejects(t *testing.T) {
	ddp := fixedSpec()
	ddp.Finetune, ddp.Strategy = "qlora", "ddp"
	unknown := fixedSpec()
	unknown.Finetune = "lora"
	noSafetensors := fixedSpec()
	noSafetensors.Finetune = "qlora"
	for name, c := range map[string]struct {
		spec model.SubmitSpec
		objs fakeObjects
	}{
		"ddp":            {ddp, qloraObjects(t)},
		"unknown method": {unknown, qloraObjects(t)},
		"no safetensors": {noSafetensors, fakeObjects{fixedPrefix + "manifest.json": fixedManifest(t, nil), fixedPrefix + "code/train.py": fixedTrain}},
	} {
		t.Run(name, func(t *testing.T) {
			sched := &submitted{}
			_, err := submitService(t, c.objs, sched).Submit(context.Background(), c.spec, &parts{})
			var e *Error
			if !errors.As(err, &e) || e.Kind != KindInvalid || !strings.Contains(err.Error(), "finetune") {
				t.Fatalf("err = %v, want an invalid finetune field", err)
			}
			if sched.js != nil {
				t.Error("a JobSet was submitted")
			}
		})
	}
}

// digestFencing remembers the jobset.json hash the submission recorded with the fencing item.
type digestFencing struct {
	noFencing
	got *string
}

func (f digestFencing) Create(_ context.Context, _ string, digest string) error {
	*f.got = digest
	return nil
}

// The fencing item records the hash of the jobset.json bytes put in S3, the same value job.submitted carries
// (GPU-271), so the recovery controller can tell the submitted original from a later change.
func TestSubmitRecordsTheHashOfTheStoredJobsetJSON(t *testing.T) {
	cfg, err := settings.Load("")
	if err != nil {
		t.Fatal(err)
	}
	objs := fakeObjects{fixedPrefix + "manifest.json": fixedManifest(t, nil), fixedPrefix + "code/train.py": fixedTrain}
	var digest string
	var events []model.SubmittedEvent
	svc := NewJobService(Config{Namespace: "training", Region: "us-east-2", Bucket: "ckpt",
		Settings: cfg.WithImages("ghcr.io/x/trainer:1", "ghcr.io/x/checkpoint:1"),
		NewID:    func() string { return "a1b2c3d4" }},
		Deps{Objects: bucketObjects{objs}, Scheduler: &submitted{}, Fencing: digestFencing{got: &digest},
			Lifecycle: capturedLifecycle{&events}, Record: noRecord{}})
	if _, err := svc.Submit(context.Background(), fixedSpec(), &parts{}); err != nil {
		t.Fatal(err)
	}
	stored, ok := objs["code/a1b2c3d4/jobset.json"]
	if !ok || digest == "" || digest != hexSum(stored) {
		t.Errorf("recorded %q, stored jobset.json hashes to %q", digest, hexSum(stored))
	}
	// job.submitted carries the same hash and where the original is (GPU-271)
	if len(events) != 1 || events[0].JobsetSHA256 != digest || events[0].JobsetURI != "s3://ckpt/code/a1b2c3d4/jobset.json" {
		t.Errorf("job.submitted = %+v, want jobset_sha256 %s and the S3 URI", events, digest)
	}
}
