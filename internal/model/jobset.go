package model

import (
	"encoding/json"
	"strconv"
	"strings"
)

// JobStatusFromMeta reads what Submit wrote into a JobSet's labels and annotations: the job's name, model,
// strategy, GPUs, queue, dataset and changed hyperparameters. The repository adds the live state of a
// JobSet on the cluster; the service uses it alone for a stopped job whose JobSet is gone (jobset.json).
// The result is Pending · QUEUED.
func JobStatusFromMeta(id, jobsetName string, labels, ann map[string]string) JobStatus {
	st := JobStatus{
		ID:           id,
		Name:         jobsetName,
		Model:        ann[ModelAnnotation],
		Strategy:     ann[StrategyAnnotation],
		Finetune:     ann[FinetuneAnnotation],
		Generation:   atoi(labels[GenerationLabel]),
		GPUs:         atoi(ann[WorldSizeAnnotation]),
		JobsetName:   jobsetName,
		Queue:        labels[QueueLabel],
		Dataset:      parseDataset(ann[DatasetAnnotation]),
		ConfigSHA256: ann[ConfigSHA256Annotation],
		Phase:        PhasePending,
		Status:       StatusQueued,
	}
	if name := ann[NameAnnotation]; name != "" {
		st.Name = name
	}
	if raw := ann[HyperparametersAnnotation]; raw != "" {
		_ = json.Unmarshal([]byte(raw), &st.Hyperparameters) // only informational
	}
	return st
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s) // 0 for JobSets that lack the value
	return n
}

// parseDataset reads the devoops/dataset annotation, "<id>/v<version>".
func parseDataset(s string) *DatasetRef {
	id, v, ok := strings.Cut(s, "/v")
	version, err := strconv.Atoi(v)
	if !ok || err != nil {
		return nil
	}
	return &DatasetRef{ID: id, Version: version}
}
