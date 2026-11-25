package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Browser → S3 direct upload (GPU-278). devoops.site goes through the Cloudflare proxy, which refuses request
// bodies over 100MB, so the browser PUTs every file straight to S3 with presigned URLs and then submits only
// the spec. Each URL signs the file's size and SHA-256, so S3 itself refuses anything else: the backend never
// reads the files back, it only compares HeadObject size · checksum with the manifest at submission.

const (
	// UploadURLTTL bounds a presigned PUT. The URL also dies with the backend's Roles Anywhere session (1 hour),
	// whichever comes first; S3 checks it when each PUT starts, so a long upload that started in time finishes.
	UploadURLTTL = time.Hour
	// maxPutBytes is S3's limit for one PUT (a whole file or one part).
	maxPutBytes = 5 << 30
	// Multipart part size the browser may choose: S3 needs at least 5 MiB for every part but the last.
	minPartBytes = 5 << 20
	maxPartBytes = 512 << 20
	maxParts     = 10000 // S3 limit
	// uploadStateName holds the S3 multipart upload ids next to the manifest. ckpt-init downloads only the
	// manifest's files, so the pods never see it.
	uploadStateName = "upload.json"
)

// uploadState is datasets/<id>/v1/upload.json: the multipart files of an upload.
type uploadState struct {
	PartBytes int64                    `json:"part_bytes"`
	Multipart map[string]multipartFile `json:"multipart"` // by dataset file name (model/…)
}

type multipartFile struct {
	UploadID string   `json:"upload_id"`
	Parts    []string `json:"parts"` // SHA-256 hex per part
}

// partSize is the size of part i (0-based) of a size-byte file cut into partBytes parts.
func partSize(size, partBytes int64, i int) int64 {
	return min(partBytes, size-int64(i)*partBytes)
}

// validUploadID is a job id (cmd/api newID: 8 hex). A fixed dataset id has a '-', so the two never meet.
var (
	validUploadID = regexp.MustCompile(`^[0-9a-f]{8}$`)
	validSHA256   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// entryName maps an upload path (multipart part name or POST /api/uploads path, without its field) to the
// dataset file name, and returns the model folder for model files.
func entryName(field, path string) (name, folder string, err error) {
	switch field {
	case "model":
		folder, rest, ok := splitModelPath(path)
		if !ok {
			return "", "", invalid("model file path must be <folder>/<file>: " + path)
		}
		return "model/" + rest, folder, nil
	case "data":
		if !safeName(path) || !strings.HasSuffix(path, ".jsonl") {
			return "", "", invalid("training data must be .jsonl files: " + path)
		}
		return "data/" + path, "", nil
	case "train":
		if !safeName(path) || !strings.HasSuffix(path, ".py") {
			return "", "", invalid("training code must be one .py file: " + path)
		}
		return trainName, "", nil
	default:
		return "", "", invalid("unknown upload field: " + field)
	}
}

// CreateUpload checks the file list the way a multipart submission is checked, writes the dataset manifest
// datasets/<id>/v1/manifest.json from it and returns one presigned PUT per file. Nothing is submitted yet.
func (s *JobService) CreateUpload(ctx context.Context, req model.UploadRequest) (model.UploadTicket, error) {
	lim := s.cfg.Settings.Limits
	if len(req.Files) > lim.MaxFiles {
		return model.UploadTicket{}, tooLarge(fmt.Sprintf("too many files: at most %d", lim.MaxFiles))
	}
	var (
		folder                string
		entries               []model.ManifestEntry
		modelBytes, dataBytes int64
		trainBytes            int64
		seen                  = map[string]bool{}
	)
	for _, f := range req.Files {
		field, path, _ := strings.Cut(f.Path, "/")
		name, dir, err := entryName(field, path)
		if err != nil {
			return model.UploadTicket{}, err
		}
		if field == "model" {
			if folder == "" {
				folder = dir
			} else if dir != folder {
				return model.UploadTicket{}, invalid("model files come from more than one folder: " + folder + ", " + dir)
			}
		}
		if field == "train" && seen[trainName] {
			return model.UploadTicket{}, invalid("only one training code file (train.py) can be uploaded")
		}
		if seen[name] {
			return model.UploadTicket{}, invalid("duplicate file: " + name)
		}
		seen[name] = true
		if f.Size < 0 || (len(f.Parts) == 0 && f.Size > maxPutBytes) {
			return model.UploadTicket{}, invalid(fmt.Sprintf("%s: size must be 0 ~ %d bytes for one PUT (send parts)", f.Path, int64(maxPutBytes)))
		}
		if !validSHA256.MatchString(f.SHA256) {
			return model.UploadTicket{}, invalid(f.Path + ": sha256 must be 64 lowercase hex characters")
		}
		if len(f.Parts) > 0 {
			if req.PartBytes < minPartBytes || req.PartBytes > maxPartBytes {
				return model.UploadTicket{}, invalid(fmt.Sprintf("partBytes must be %d ~ %d for a multipart file", int64(minPartBytes), int64(maxPartBytes)))
			}
			n := int((f.Size + req.PartBytes - 1) / req.PartBytes)
			if f.Size <= req.PartBytes || len(f.Parts) != n || n > maxParts {
				return model.UploadTicket{}, invalid(fmt.Sprintf("%s: %d bytes in %d-byte parts is %d parts (a file of one part is a single PUT)", f.Path, f.Size, req.PartBytes, n))
			}
			for _, sum := range f.Parts {
				if !validSHA256.MatchString(sum) {
					return model.UploadTicket{}, invalid(f.Path + ": part sha256 must be 64 lowercase hex characters")
				}
			}
		}
		switch field {
		case "model":
			modelBytes += f.Size
		case "data":
			dataBytes += f.Size
		case "train":
			trainBytes = f.Size
		}
		entries = append(entries, model.ManifestEntry{Name: name, SHA256: f.SHA256, SizeBytes: f.Size})
	}
	if modelBytes > lim.MaxModelBytes {
		return model.UploadTicket{}, tooLarge(fmt.Sprintf("model upload is over the limit (%d bytes)", lim.MaxModelBytes))
	}
	if dataBytes > lim.MaxDataBytes {
		return model.UploadTicket{}, tooLarge(fmt.Sprintf("data upload is over the limit (%d bytes)", lim.MaxDataBytes))
	}
	if trainBytes > MaxTrainBytes {
		return model.UploadTicket{}, tooLarge(fmt.Sprintf("train upload is over the limit (%d bytes)", MaxTrainBytes))
	}
	var fields []model.FieldError
	if !seen[modelConfigName] {
		fields = append(fields, model.FieldError{Field: "model", Message: "Hugging Face 모델 폴더(config.json 포함)가 필요합니다"})
	}
	if !slices.ContainsFunc(entries, func(e model.ManifestEntry) bool { return strings.HasPrefix(e.Name, "data/") }) {
		fields = append(fields, model.FieldError{Field: "data", Message: ".jsonl 학습 데이터가 1개 이상 필요합니다"})
	}
	if !seen[trainName] {
		fields = append(fields, model.FieldError{Field: "train", Message: "학습 코드(train.py)가 필요합니다"})
	} else if trainBytes == 0 {
		fields = append(fields, model.FieldError{Field: "train", Message: "학습 코드(train.py)가 비어 있습니다"})
	}
	if len(fields) > 0 {
		return model.UploadTicket{}, invalidFields(fields)
	}
	sort.Slice(entries, func(a, b int) bool { return entries[a].Name < entries[b].Name })

	id := s.cfg.NewID()
	prefix := datasetPrefix(id, datasetVersion)
	now := s.cfg.Now().UTC()

	state := uploadState{PartBytes: req.PartBytes, Multipart: map[string]multipartFile{}}
	for _, f := range req.Files {
		if len(f.Parts) == 0 {
			continue
		}
		field, path, _ := strings.Cut(f.Path, "/")
		name, _, _ := entryName(field, path)
		uploadID, err := s.objects.CreateMultipart(ctx, prefix+name)
		if err != nil {
			return model.UploadTicket{}, upstream("start multipart "+name, err)
		}
		state.Multipart[name] = multipartFile{UploadID: uploadID, Parts: f.Parts}
	}
	if len(state.Multipart) > 0 {
		b, err := json.Marshal(state)
		if err != nil {
			return model.UploadTicket{}, fmt.Errorf("encode upload state: %w", err)
		}
		if err := s.objects.Put(ctx, prefix+uploadStateName, bytes.NewReader(b)); err != nil {
			return model.UploadTicket{}, upstream("upload "+uploadStateName, err)
		}
	}
	manifestBytes, err := json.MarshalIndent(model.DatasetManifest{
		SchemaVersion: manifestSchema, JobID: id, Model: folder, Format: s.cfg.Settings.Training.DatasetFormat,
		Files: entries, ModelBytes: modelBytes, DataBytes: dataBytes, CreatedAt: now.Format(time.RFC3339),
	}, "", "  ")
	if err != nil {
		return model.UploadTicket{}, fmt.Errorf("encode manifest: %w", err)
	}
	if err := s.objects.Put(ctx, prefix+manifestName, bytes.NewReader(manifestBytes)); err != nil {
		return model.UploadTicket{}, upstream("upload "+manifestName, err)
	}

	byName := map[string]model.ManifestEntry{}
	for _, e := range entries {
		byName[e.Name] = e
	}
	targets := make([]model.UploadTarget, 0, len(req.Files))
	for _, f := range req.Files {
		field, path, _ := strings.Cut(f.Path, "/")
		name, _, _ := entryName(field, path)
		e := byName[name]
		if mp, ok := state.Multipart[name]; ok {
			t := model.UploadTarget{Path: f.Path}
			for i, sum := range mp.Parts {
				size := partSize(e.SizeBytes, req.PartBytes, i)
				url, headers, err := s.objects.PresignUploadPart(ctx, prefix+name, mp.UploadID, int32(i+1), size, sum, UploadURLTTL)
				if err != nil {
					return model.UploadTicket{}, upstream("presign "+name, err)
				}
				t.Parts = append(t.Parts, model.UploadPartTarget{Number: int32(i + 1), Size: size, URL: url, Headers: headers})
			}
			targets = append(targets, t)
			continue
		}
		url, headers, err := s.objects.PresignPut(ctx, prefix+name, e.SizeBytes, e.SHA256, UploadURLTTL)
		if err != nil {
			return model.UploadTicket{}, upstream("presign "+name, err)
		}
		targets = append(targets, model.UploadTarget{Path: f.Path, URL: url, Headers: headers})
	}
	slog.Info("upload created", "id", id, "model", folder, "files", len(entries), "multipart", len(state.Multipart),
		"modelBytes", modelBytes, "dataBytes", dataBytes)
	return model.UploadTicket{ID: id, ExpiresAt: now.Add(UploadURLTTL), Files: targets}, nil
}

// uploadedDataset is the dataset of a finished POST /api/uploads upload. Every manifest file must be in S3
// with its size and SHA-256 (S3 checked the bytes against the signed checksum when they were PUT).
func (s *JobService) uploadedDataset(ctx context.Context, id string, src UploadSource) (datasetInput, error) {
	if _, err := src.Next(); !errors.Is(err, io.EOF) {
		if err != nil {
			return datasetInput{}, invalid("read upload: " + err.Error())
		}
		return datasetInput{}, invalid("spec.upload를 쓰면 파일 파트(model · data · train)를 보내지 않습니다")
	}
	// A job's inputs are written before the JobSet; a second submission of the same upload must not touch them.
	if _, _, err := s.objects.Get(ctx, s.jobsetKey(id), 1); err == nil {
		return datasetInput{}, &Error{Kind: KindConflict, Msg: "upload " + id + " is already submitted"}
	} else if !errors.Is(err, model.ErrNotFound) {
		return datasetInput{}, upstream("check submission", err)
	}
	prefix := datasetPrefix(id, datasetVersion)
	bad := func(msg string) error {
		return invalidFields([]model.FieldError{{Field: "upload", Message: msg}})
	}
	raw, _, err := s.objects.Get(ctx, prefix+manifestName, maxMetadataBytes)
	if errors.Is(err, model.ErrNotFound) {
		return datasetInput{}, bad("업로드가 없습니다 (POST /api/uploads로 먼저 만드세요)")
	}
	if err != nil {
		return datasetInput{}, upstream("read upload manifest", err)
	}
	var m model.DatasetManifest
	if err := json.Unmarshal(raw, &m); err != nil || m.JobID != id {
		return datasetInput{}, bad("업로드 manifest.json을 읽을 수 없습니다")
	}
	var state uploadState
	if b, _, err := s.objects.Get(ctx, prefix+uploadStateName, maxMetadataBytes); err == nil {
		if err := json.Unmarshal(b, &state); err != nil {
			return datasetInput{}, bad("업로드 upload.json을 읽을 수 없습니다")
		}
	} else if !errors.Is(err, model.ErrNotFound) {
		return datasetInput{}, upstream("read upload state", err)
	}
	for _, e := range m.Files {
		if mp, ok := state.Multipart[e.Name]; ok {
			if err := s.completeMultipart(ctx, prefix+e.Name, e, mp, state.PartBytes); err != nil {
				return datasetInput{}, err
			}
			continue
		}
		h, err := s.objects.Head(ctx, prefix+e.Name)
		if errors.Is(err, model.ErrNotFound) {
			return datasetInput{}, bad(e.Name + "이(가) 아직 올라가지 않았습니다")
		}
		if err != nil {
			return datasetInput{}, upstream("check "+e.Name, err)
		}
		if h.Size != e.SizeBytes || h.SHA256 != e.SHA256 {
			return datasetInput{}, bad(e.Name + "이(가) 업로드 목록과 다릅니다 (크기 · sha256)")
		}
	}
	train, _, err := s.objects.Get(ctx, prefix+trainName, MaxTrainBytes)
	if err != nil {
		return datasetInput{}, upstream("read uploaded train.py", err)
	}
	return datasetInput{id: id, version: datasetVersion, prefix: prefix, modelName: m.Model,
		modelBytes: m.ModelBytes, dataBytes: m.DataBytes, modelFiles: modelFiles(m.Files), train: train,
		manifestSHA: sha256Hex(raw)}, nil
}

// completeMultipart finishes a multipart file once every part is in S3 with its signed checksum. A file already
// completed (an earlier submission failed later on) only has its size checked: its parts were checked then.
//
// ListParts comes first: before completion the object does not exist, and HeadObject of a missing key is 403, not
// 404, for the backend role (no s3:ListBucket on datasets/). Only when the upload is gone is the object read.
func (s *JobService) completeMultipart(ctx context.Context, key string, e model.ManifestEntry, mp multipartFile, partBytes int64) error {
	bad := func(msg string) error {
		return invalidFields([]model.FieldError{{Field: "upload", Message: e.Name + msg}})
	}
	parts, err := s.objects.ListParts(ctx, key, mp.UploadID)
	if errors.Is(err, model.ErrNotFound) {
		h, herr := s.objects.Head(ctx, key)
		if herr == nil {
			if h.Size != e.SizeBytes {
				return bad("의 크기가 업로드 목록과 다릅니다")
			}
			return nil // completed by an earlier submission
		}
		return bad("의 multipart 업로드가 없습니다 (만료 · 취소)")
	}
	if err != nil {
		return upstream("list parts "+e.Name, err)
	}
	byNumber := map[int32]model.UploadedPart{}
	for _, p := range parts {
		byNumber[p.Number] = p
	}
	done := make([]model.UploadedPart, 0, len(mp.Parts))
	for i, sum := range mp.Parts {
		p, ok := byNumber[int32(i+1)]
		if !ok {
			return bad(fmt.Sprintf("이(가) 아직 다 올라가지 않았습니다 (조각 %d/%d)", len(byNumber), len(mp.Parts)))
		}
		if p.SHA256 != sum || p.Size != partSize(e.SizeBytes, partBytes, i) {
			return bad(fmt.Sprintf("의 조각 %d이(가) 업로드 목록과 다릅니다 (크기 · sha256)", i+1))
		}
		done = append(done, p)
	}
	if err := s.objects.CompleteMultipart(ctx, key, mp.UploadID, done); err != nil {
		return upstream("complete multipart "+e.Name, err)
	}
	return nil
}
