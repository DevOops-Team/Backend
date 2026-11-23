package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service/ckptpolicy"
	"github.com/DevOops-Team/Backend/internal/settings"
)

// savedParams is how many parameters the job's checkpoint holds (GPU-261): every parameter for full, the
// LoRA adapter for qlora. The tensor shapes come from the headers of the model's safetensors files, which
// are already in S3 (an upload has just been stored, a fixed dataset was put there before). Only each
// header is read (a ranged GET), never the weights.
//
// When the shapes cannot be read (no safetensors file, a broken header), full falls back to the folder
// size at 2 bytes a parameter (bf16 · fp16 checkpoints) with a warning; qlora needs the shapes to size
// the adapter, so it is rejected.
func (s *JobService) savedParams(ctx context.Context, ds datasetInput, finetune string, lora *settings.Lora) (int64, error) {
	shapes, err := s.tensorShapes(ctx, ds)
	if err == nil {
		n := ckptpolicy.SavedParams(shapes, lora)
		if n > 0 {
			return n, nil
		}
		if lora != nil {
			return 0, invalidFields([]model.FieldError{{Field: "finetune", Message: fmt.Sprintf(
				"QLoRA 대상 층(%s)이 모델에 없습니다", strings.Join(lora.TargetModules, ", "))}})
		}
		err = fmt.Errorf("no tensors")
	}
	if lora != nil {
		return 0, invalidFields([]model.FieldError{{Field: "finetune", Message: "QLoRA는 safetensors 모델 폴더만 쓸 수 있습니다 " +
			"(텐서 모양으로 어댑터 크기를 계산): " + err.Error()}})
	}
	slog.Warn("checkpoint size from the model folder size", "dataset", ds.prefix, "finetune", finetune, "err", err)
	return ds.modelBytes / 2, nil
}

// tensorShapes reads every model/*.safetensors header of the dataset.
func (s *JobService) tensorShapes(ctx context.Context, ds datasetInput) (map[string][]int64, error) {
	shapes := map[string][]int64{}
	found := false
	for _, name := range ds.modelFiles {
		if !strings.HasSuffix(name, ".safetensors") {
			continue
		}
		found = true
		key := ds.prefix + name
		head, _, err := s.objects.Get(ctx, key, 8)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		n, err := ckptpolicy.HeaderLength(head)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		raw, _, err := s.objects.Get(ctx, key, 8+n)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if int64(len(raw)) < 8+n {
			return nil, fmt.Errorf("%s: header cut short", name)
		}
		part, err := ckptpolicy.Shapes(raw[8 : 8+n])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for k, v := range part {
			shapes[k] = v
		}
	}
	if !found {
		return nil, fmt.Errorf("no model/*.safetensors")
	}
	return shapes, nil
}
