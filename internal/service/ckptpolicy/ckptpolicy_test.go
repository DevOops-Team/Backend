package ckptpolicy

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/DevOops-Team/Backend/internal/settings"
)

var defaults = settings.Checkpoint{
	BytesPerParam:       12,
	WriteBytesPerSecond: 100e6,
	MTBFSeconds:         21600,
	MinIntervalSeconds:  300,
	WarmupMeasureSteps:  20,
}

var qlora = &settings.Lora{R: 16, Alpha: 32, Dropout: 0.05,
	TargetModules: []string{"q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"}}

func TestComputeUsesYoungDalyOnTheSavedParameters(t *testing.T) {
	// Qwen2.5-1.5B Full · FSDP 4장: 1.54e9 × 12 B ÷ 4 pods ÷ 100 MB/s ≈ 46.3 s, τ = √(2 · 46.3 · 21600) ≈ 1414 s
	p := Compute(1_543_714_304, 4, defaults)
	if p.EstimatedSaveSeconds != 46.3 || p.IntervalSeconds != 1414 {
		t.Errorf("policy = %+v, want save 46.3 s · interval 1414 s", p)
	}
	if p.MTBFSeconds != 21600 || p.WarmupMeasureSteps != 20 {
		t.Errorf("policy = %+v, want the inputs carried for train.py and the logs", p)
	}
}

func TestComputeSplitsTheSaveAcrossWriters(t *testing.T) {
	// SmolLM2-135M: FSDP 2장은 둘이 나눠 쓰고(8.1 s → 590 s), DDP는 rank 0 혼자 전체를 쓴다(16.1 s → 835 s)
	if p := Compute(134_515_008, Writers("fsdp", 2), defaults); p.EstimatedSaveSeconds != 8.1 || p.IntervalSeconds != 590 {
		t.Errorf("fsdp 2: %+v, want 8.1 s · 590 s", p)
	}
	if p := Compute(134_515_008, Writers("ddp", 2), defaults); p.EstimatedSaveSeconds != 16.1 || p.IntervalSeconds != 835 {
		t.Errorf("ddp 2: %+v, want 16.1 s · 835 s", p)
	}
}

func TestComputeKeepsTheMinimumInterval(t *testing.T) {
	// 14B QLoRA 어댑터 6,880만 × 12 B ÷ 4 ≈ 2.1 s → τ ≈ 299 s, 하한 300 s (모델 폴더 29.6 GB로 계산하면 6,180 s였다)
	if p := Compute(68_812_800, 4, defaults); p.EstimatedSaveSeconds != 2.1 || p.IntervalSeconds != 300 {
		t.Errorf("policy = %+v, want 2.1 s and the 300 s floor", p)
	}
}

func TestComputeFollowsALowerMTBF(t *testing.T) {
	// 검증 때 M · 하한을 낮춰 중간 저장을 여러 번 본다 (설계 2절): 16.1 s, M 600 → √(2·16.1·600) ≈ 139 s
	c := defaults
	c.MTBFSeconds, c.MinIntervalSeconds = 600, 60
	if p := Compute(134_515_008, 1, c); p.IntervalSeconds != 139 {
		t.Errorf("interval = %v, want 139", p.IntervalSeconds)
	}
}

// qwen14B is the shape of Qwen2.5-14B's linear layers (48 layers, hidden 5120, kv 1024, MLP 13824) plus
// tensors LoRA does not touch.
func qwen14B() map[string][]int64 {
	shapes := map[string][]int64{
		"model.embed_tokens.weight": {152064, 5120},
		"lm_head.weight":            {152064, 5120},
		"model.norm.weight":         {5120},
	}
	for i := range 48 {
		p := fmt.Sprintf("model.layers.%d.", i)
		shapes[p+"self_attn.q_proj.weight"] = []int64{5120, 5120}
		shapes[p+"self_attn.q_proj.bias"] = []int64{5120}
		shapes[p+"self_attn.k_proj.weight"] = []int64{1024, 5120}
		shapes[p+"self_attn.v_proj.weight"] = []int64{1024, 5120}
		shapes[p+"self_attn.o_proj.weight"] = []int64{5120, 5120}
		shapes[p+"mlp.gate_proj.weight"] = []int64{13824, 5120}
		shapes[p+"mlp.up_proj.weight"] = []int64{13824, 5120}
		shapes[p+"mlp.down_proj.weight"] = []int64{5120, 13824}
		shapes[p+"input_layernorm.weight"] = []int64{5120}
	}
	return shapes
}

func TestSavedParamsCountsTheLoRAAdapter(t *testing.T) {
	// 층당 16 · (10240 + 6144 + 6144 + 10240 + 18944 · 3) = 1,433,600, × 48 = 68,812,800 (개인 문서 Q2 「6,900만」)
	if n := SavedParams(qwen14B(), qlora); n != 68_812_800 {
		t.Errorf("lora params = %d, want 68812800", n)
	}
	only := &settings.Lora{R: 8, TargetModules: []string{"q_proj"}}
	if n := SavedParams(qwen14B(), only); n != 48*8*(5120+5120) {
		t.Errorf("q_proj only = %d, want %d (bias and other layers do not count)", n, 48*8*10240)
	}
}

func TestSavedParamsCountsEveryParameterForFull(t *testing.T) {
	shapes := map[string][]int64{"a.weight": {3, 4}, "a.bias": {4}, "scalar": {}}
	if n := SavedParams(shapes, nil); n != 12+4+1 {
		t.Errorf("full params = %d, want 17", n)
	}
}

func TestReadsASafetensorsHeader(t *testing.T) {
	header := []byte(`{"__metadata__":{"format":"pt"},"w":{"dtype":"BF16","shape":[2,3],"data_offsets":[0,12]}}`)
	file := binary.LittleEndian.AppendUint64(nil, uint64(len(header)))
	file = append(file, header...)
	n, err := HeaderLength(file[:8])
	if err != nil || n != int64(len(header)) {
		t.Fatalf("length = %d, %v", n, err)
	}
	shapes, err := Shapes(file[8 : 8+n])
	if err != nil || len(shapes) != 1 || len(shapes["w"]) != 2 || shapes["w"][1] != 3 {
		t.Fatalf("shapes = %v, %v", shapes, err)
	}
	for _, bad := range [][]byte{nil, []byte("1234567"), binary.LittleEndian.AppendUint64(nil, 0),
		binary.LittleEndian.AppendUint64(nil, MaxHeaderBytes+1)} {
		if _, err := HeaderLength(bad); err == nil {
			t.Errorf("HeaderLength(%v) accepted", bad)
		}
	}
	if _, err := Shapes([]byte("not json")); err == nil {
		t.Error("Shapes accepted a broken header")
	}
}
