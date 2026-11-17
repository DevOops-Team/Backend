package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// The trained model is the final approved checkpoint of a finished job (ADR-30). The ckpt-sidecar uploads
// it to jobs/<id>/gen-NNNN/step-NNNNNNNN/ and writes manifest.json last (Data checkpoint/ckpt/storage.py);
// the collector records its manifest_key in data.checkpoints. The manifest is the list of files a
// download may sign, so a request can never reach another key. Downloads are presigned S3 URLs: the
// browser fetches the files from S3 directly (GPU-190).

// checkpointManifest is the part of the sidecar's manifest.json the backend reads.
type checkpointManifest struct {
	Files []struct {
		Name      string `json:"name"`
		SizeBytes int64  `json:"size_bytes"`
		SHA256    string `json:"sha256"`
	} `json:"files"`
}

// ModelURLTTL is how long a signed download link works. It is signed when the user clicks, so it only has
// to outlive the start of the download.
const ModelURLTTL = 15 * time.Minute

// trainedModel is a finished job's final checkpoint with the S3 prefix of its files.
type trainedModel struct {
	model.TrainedModel
	prefix string
}

// Model returns the final checkpoint of a finished job: 409 while it has not succeeded, 404 when no
// checkpoint was approved.
func (s *JobService) Model(ctx context.Context, id string) (model.TrainedModel, error) {
	m, err := s.trainedModel(ctx, id)
	return m.TrainedModel, err
}

// ModelFileURL signs a download of one file of the trained model; only names in its manifest are allowed.
func (s *JobService) ModelFileURL(ctx context.Context, id, name string) (string, error) {
	if !safeName(name) {
		return "", invalid("invalid file name")
	}
	m, err := s.trainedModel(ctx, id)
	if err != nil {
		return "", err
	}
	if !slices.ContainsFunc(m.Files, func(f model.ModelFile) bool { return f.Name == name }) {
		return "", notFound("file not in the trained model")
	}
	url, err := s.objects.Presign(ctx, m.prefix+name, name, ModelURLTTL)
	if err != nil {
		return "", upstream("sign model download", err)
	}
	return url, nil
}

func (s *JobService) trainedModel(ctx context.Context, id string) (trainedModel, error) {
	if !validJobID.MatchString(id) {
		return trainedModel{}, invalid("invalid job id")
	}
	jobs, err := s.List(ctx)
	if err != nil {
		return trainedModel{}, err
	}
	i := slices.IndexFunc(jobs, func(j model.Job) bool { return j.ID == id })
	if i < 0 {
		return trainedModel{}, notFound("job not found")
	}
	if jobs[i].Status != model.StatusSucceeded {
		return trainedModel{}, &Error{Kind: KindConflict, Code: "NOT_FINISHED",
			Msg: "training has not finished (status " + string(jobs[i].Status) + ")"}
	}

	final, err := s.records.FinalCheckpoint(ctx, id)
	if err != nil {
		return trainedModel{}, upstream("read final checkpoint", err)
	}
	if final == nil {
		return trainedModel{}, notFound("no approved checkpoint")
	}
	prefix, ok := strings.CutSuffix(final.ManifestKey, manifestName)
	if !ok || !strings.HasPrefix(prefix, "jobs/"+id+"/") {
		return trainedModel{}, upstream("read final checkpoint", fmt.Errorf("manifest key %q is not under jobs/%s/", final.ManifestKey, id))
	}

	data, _, err := s.objects.Get(ctx, final.ManifestKey, maxMetadataBytes)
	if errors.Is(err, model.ErrNotFound) {
		return trainedModel{}, notFound("checkpoint manifest not found")
	}
	if err != nil {
		return trainedModel{}, upstream("get checkpoint manifest", err)
	}
	var man checkpointManifest
	if err := json.Unmarshal(data, &man); err != nil {
		return trainedModel{}, upstream("parse checkpoint manifest", err)
	}
	files := make([]model.ModelFile, 0, len(man.Files))
	for _, f := range man.Files {
		files = append(files, model.ModelFile{Name: f.Name, Size: f.SizeBytes, SHA256: f.SHA256})
	}
	return trainedModel{
		TrainedModel: model.TrainedModel{JobID: id, Generation: final.Generation, Step: final.Step, Format: final.Format,
			WorldSize: final.WorldSize, SizeBytes: final.SizeBytes, ApprovedAt: final.ApprovedAt, Files: files},
		prefix: prefix,
	}, nil
}
