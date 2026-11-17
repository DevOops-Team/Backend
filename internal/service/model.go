package service

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/DevOops-Team/Backend/internal/model"
)

// The trained model is the final approved checkpoint of a finished job (ADR-30). The ckpt-sidecar uploads
// it to jobs/<id>/gen-NNNN/step-NNNNNNNN/ and writes manifest.json last (Data checkpoint/ckpt/storage.py);
// the collector records its manifest_key in data.checkpoints. The manifest is the list of files a
// download may read, so a request can never reach another key.

// checkpointManifest is the part of the sidecar's manifest.json the backend reads.
type checkpointManifest struct {
	Files []struct {
		Name      string `json:"name"`
		SizeBytes int64  `json:"size_bytes"`
		SHA256    string `json:"sha256"`
	} `json:"files"`
}

// ModelArchive is the whole trained model as one zip. Write streams it; the files are stored, not
// compressed, because weights do not compress.
type ModelArchive struct {
	Name  string // file name for Content-Disposition
	Write func(w io.Writer) error
}

// trainedModel is a finished job's final checkpoint with the S3 prefix of its files.
type trainedModel struct {
	model.TrainedModel
	jobName  string
	prefix   string
	manifest []byte
}

// Model returns the final checkpoint of a finished job: 409 while it has not succeeded, 404 when no
// checkpoint was approved.
func (s *JobService) Model(ctx context.Context, id string) (model.TrainedModel, error) {
	m, err := s.trainedModel(ctx, id)
	return m.TrainedModel, err
}

// ModelFile opens one file of the trained model; only names in its manifest are allowed.
func (s *JobService) ModelFile(ctx context.Context, id, name string) (model.ObjectStream, error) {
	if !safeName(name) {
		return model.ObjectStream{}, invalid("invalid file name")
	}
	m, err := s.trainedModel(ctx, id)
	if err != nil {
		return model.ObjectStream{}, err
	}
	if !slices.ContainsFunc(m.Files, func(f model.ModelFile) bool { return f.Name == name }) {
		return model.ObjectStream{}, notFound("file not in the trained model")
	}
	body, size, err := s.objects.Open(ctx, m.prefix+name)
	if errors.Is(err, model.ErrNotFound) {
		return model.ObjectStream{}, notFound("model file missing in storage")
	}
	if err != nil {
		return model.ObjectStream{}, upstream("open model file", err)
	}
	return model.ObjectStream{Name: name, Body: body, Size: size}, nil
}

// ModelArchive checks the job before anything is written, then returns the zip writer. Entries sit in
// one folder named after the job and step, with the manifest (sha256 per file) next to them.
func (s *JobService) ModelArchive(ctx context.Context, id string) (ModelArchive, error) {
	m, err := s.trainedModel(ctx, id)
	if err != nil {
		return ModelArchive{}, err
	}
	base := m.jobName
	if !validName.MatchString(base) {
		base = id
	}
	dir := fmt.Sprintf("%s-step-%d", base, m.Step)
	write := func(w io.Writer) error {
		zw := zip.NewWriter(w)
		entry := func(name string) (io.Writer, error) {
			return zw.CreateHeader(&zip.FileHeader{Name: dir + "/" + name, Method: zip.Store, Modified: m.ApprovedAt})
		}
		ew, err := entry(manifestName)
		if err != nil {
			return err
		}
		if _, err := ew.Write(m.manifest); err != nil {
			return err
		}
		for _, f := range m.Files {
			if err := s.copyObject(ctx, m.prefix+f.Name, entry, f.Name); err != nil {
				return err
			}
		}
		return zw.Close()
	}
	return ModelArchive{Name: dir + ".zip", Write: write}, nil
}

func (s *JobService) copyObject(ctx context.Context, key string, entry func(string) (io.Writer, error), name string) error {
	body, _, err := s.objects.Open(ctx, key)
	if err != nil {
		return fmt.Errorf("open %s: %w", key, err)
	}
	defer body.Close()
	w, err := entry(name)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, body); err != nil {
		return fmt.Errorf("copy %s: %w", key, err)
	}
	return nil
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
		jobName:  jobs[i].Name,
		prefix:   prefix,
		manifest: data,
	}, nil
}
