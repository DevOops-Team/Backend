// Package ckptpolicy computes how often a job saves a checkpoint (design §5).
//
// Young/Daly: the interval that minimises (time spent saving + work lost to a failure) is
// τ = √(2 · C · M), C = one save's time, M = mean time between failures. The backend only knows the
// model at submission, not the step time, so it returns a time interval; train.py measures its
// first steps and turns τ into a step interval (n = ⌊T/τ⌋ saves over the estimated run time T).
//
// C comes from what a checkpoint actually holds (GPU-261), not from the model folder size:
//
//	saved parameters = every parameter (full) | the LoRA adapter Σ r·(in+out) (qlora)
//	checkpoint bytes = saved parameters × bytes_per_param (fp32 weight 4 + Adam moments 8 = 12)
//	C                = checkpoint bytes ÷ writers ÷ write_bytes_per_second
//
// writers is the number of pods writing their part at once: every GPU for FSDP (sharded DCP), one for
// DDP (rank 0 writes the whole state).
package ckptpolicy

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

// MaxHeaderBytes caps a safetensors header read (the JSON before the tensor data). Real headers are KBs
// to a few MBs; the format itself limits them to 100 MB.
const MaxHeaderBytes = 100 << 20

// Compute returns the policy written into config.json for a checkpoint of savedParams parameters that
// writers pods write at the same time.
func Compute(savedParams int64, writers int, c settings.Checkpoint) model.CheckpointPolicy {
	if writers < 1 {
		writers = 1
	}
	save := float64(savedParams) * c.BytesPerParam / float64(writers) / c.WriteBytesPerSecond
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

// Writers is how many pods write a checkpoint at once: all of them for FSDP, rank 0 alone for DDP.
func Writers(strategy string, gpus int) int {
	if strategy == "fsdp" {
		return gpus
	}
	return 1
}

// HeaderLength reads the 8-byte little-endian length at the start of a safetensors file.
func HeaderLength(prefix []byte) (int64, error) {
	if len(prefix) < 8 {
		return 0, errors.New("safetensors: shorter than its 8-byte header length")
	}
	n := binary.LittleEndian.Uint64(prefix[:8])
	if n == 0 || n > MaxHeaderBytes {
		return 0, fmt.Errorf("safetensors: header length %d out of range", n)
	}
	return int64(n), nil
}

// Shapes parses a safetensors header (the JSON after the length) into tensor name → shape.
func Shapes(header []byte) (map[string][]int64, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(header, &raw); err != nil {
		return nil, fmt.Errorf("safetensors header: %w", err)
	}
	shapes := make(map[string][]int64, len(raw))
	for name, v := range raw {
		if name == "__metadata__" {
			continue
		}
		var t struct {
			Shape []int64 `json:"shape"`
		}
		if err := json.Unmarshal(v, &t); err != nil {
			return nil, fmt.Errorf("safetensors tensor %s: %w", name, err)
		}
		shapes[name] = t.Shape
	}
	return shapes, nil
}

// SavedParams counts the parameters a checkpoint saves: every parameter without lora, otherwise the LoRA
// adapter — r·(in + out) for each 2-D weight "<…>.<target>.weight" of a target module (peft adds lora_A
// r×in and lora_B out×r there). Linear weights are [out, in].
func SavedParams(shapes map[string][]int64, lora *settings.Lora) int64 {
	var n int64
	for name, shape := range shapes {
		if lora == nil {
			p := int64(1)
			for _, d := range shape {
				p *= d
			}
			n += p
			continue
		}
		if len(shape) != 2 {
			continue
		}
		for _, t := range lora.TargetModules {
			if strings.HasSuffix(name, "."+t+".weight") {
				n += int64(lora.R) * (shape[0] + shape[1])
				break
			}
		}
	}
	return n
}
