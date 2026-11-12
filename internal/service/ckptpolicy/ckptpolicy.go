// Package ckptpolicy computes how often a job saves a checkpoint (design §5).
//
// Young/Daly: the interval that minimises (time spent saving + work lost to a failure) is
// τ = √(2 · C · M), C = one save's time, M = mean time between failures. The backend only knows the
// model size at submission, not the step time, so it returns a time interval; train.py measures its
// first steps and turns τ into a step interval (n = ⌊T/τ⌋ saves over the estimated run time T).
package ckptpolicy

import (
	"math"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

// Compute returns the policy written into config.json for a model folder of modelBytes.
func Compute(modelBytes int64, c settings.Checkpoint) model.CheckpointPolicy {
	save := float64(modelBytes) * c.SizeFactor / c.WriteBytesPerSecond
	interval := math.Sqrt(2 * save * c.MTBFSeconds)
	if interval < c.MinIntervalSeconds {
		interval = c.MinIntervalSeconds
	}
	return model.CheckpointPolicy{
		IntervalSeconds:      math.Round(interval),
		EstimatedSaveSeconds: math.Round(save*10) / 10,
		MTBFSeconds:          c.MTBFSeconds,
		WarmupMeasureSteps:   c.WarmupMeasureSteps,
	}
}
