package ckptpolicy

import (
	"testing"

	"github.com/DevOops-Team/Backend/internal/settings"
)

var defaults = settings.Checkpoint{
	SizeFactor:          3,
	WriteBytesPerSecond: 100e6,
	MTBFSeconds:         21600,
	MinIntervalSeconds:  300,
	WarmupMeasureSteps:  20,
}

func TestComputeUsesYoungDaly(t *testing.T) {
	// GPT-2 small folder ≈ 550 MB → C = 550e6 · 3 / 100e6 = 16.5 s, τ = √(2 · 16.5 · 21600) ≈ 844 s
	p := Compute(550_000_000, defaults)
	if p.EstimatedSaveSeconds != 16.5 {
		t.Errorf("save = %v, want 16.5", p.EstimatedSaveSeconds)
	}
	if p.IntervalSeconds != 844 {
		t.Errorf("interval = %v, want 844", p.IntervalSeconds)
	}
	if p.MTBFSeconds != 21600 || p.WarmupMeasureSteps != 20 {
		t.Errorf("policy = %+v, want the inputs carried for train.py and the logs", p)
	}
}

func TestComputeGrowsWithModelSize(t *testing.T) {
	// 2 GB → C = 60 s, τ = √(2 · 60 · 21600) ≈ 1610 s: a bigger save is worth doing less often
	if p := Compute(2_000_000_000, defaults); p.IntervalSeconds != 1610 {
		t.Errorf("interval = %v, want 1610", p.IntervalSeconds)
	}
}

func TestComputeKeepsTheMinimumInterval(t *testing.T) {
	// 10 MB → τ ≈ 114 s, below the 300 s floor
	if p := Compute(10_000_000, defaults); p.IntervalSeconds != 300 {
		t.Errorf("interval = %v, want the 300 s floor", p.IntervalSeconds)
	}
}

func TestComputeFollowsALowerMTBF(t *testing.T) {
	// 검증 때 Gitops에서 M · 하한을 낮춰 중간 저장을 여러 번 본다 (설계 2절)
	c := defaults
	c.MTBFSeconds, c.MinIntervalSeconds = 600, 60
	if p := Compute(550_000_000, c); p.IntervalSeconds != 141 {
		t.Errorf("interval = %v, want √(2·16.5·600) ≈ 141", p.IntervalSeconds)
	}
}
