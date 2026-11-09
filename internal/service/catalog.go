package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

const (
	manifestName    = "manifest.json"
	datasetsPrefix  = "datasets/"
	seededVersion   = "seed" // 임시 구성: 데이터셋 등록 API 없이 시드 스크립트가 올린다.
	datasetIDPrefix = datasetsPrefix + "ds-"
)

// datasetManifest is datasets/<id>/v<version>/manifest.json. Name is optional: the id is shown without it.
type datasetManifest struct {
	DatasetID string `json:"dataset_id"`
	Version   int    `json:"version"`
	Name      string `json:"name"`
	Format    string `json:"format"`
	Files     []struct {
		Name      string `json:"name"`
		SizeBytes int64  `json:"size_bytes"`
		SHA256    string `json:"sha256"`
	} `json:"files"`
}

func datasetPrefix(id string, version int) string {
	return fmt.Sprintf("%s%s/v%d/", datasetsPrefix, id, version)
}

// Models lists the model profiles with the files stored in S3.
func (s *JobService) Models(ctx context.Context) ([]model.Model, error) {
	names := make([]string, 0, len(s.cfg.Settings.ModelProfiles))
	for name := range s.cfg.Settings.ModelProfiles {
		names = append(names, name)
	}
	sort.Strings(names)

	models := make([]model.Model, 0, len(names))
	for _, name := range names {
		p := s.cfg.Settings.ModelProfiles[name]
		prefix := modelPrefix(p)
		objs, err := s.objects.List(ctx, prefix)
		if err != nil {
			return nil, upstream("list model files", err)
		}
		files := make([]model.StoredFile, 0, len(objs))
		for _, o := range objs {
			files = append(files, model.StoredFile{Name: strings.TrimPrefix(o.Key, prefix), Size: o.Size})
		}
		minGPUs := make(map[string]int, len(p.Strategies))
		for _, st := range p.Strategies {
			minGPUs[st] = s.cfg.Settings.Strategies[st].MinGPUs
		}
		models = append(models, model.Model{
			Name: name, DisplayName: p.DisplayName, Strategies: p.Strategies, DefaultStrategy: p.DefaultStrategy,
			StrategyMinGPUs: minGPUs,
			GPUs:            model.GPURange{Min: p.GPUs.Min, Max: p.GPUs.Max, Default: p.GPUs.Default, Options: p.GPUs.Options},
			DatasetFormats:  p.DatasetFormats, Hyperparameters: hyperparameterDefs(p),
			URI: fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, prefix), Files: files,
		})
	}
	return models, nil
}

// ModelFile returns the start of one stored file of a model.
func (s *JobService) ModelFile(ctx context.Context, name, file string) (model.FileContent, error) {
	if !safeName(file) {
		return model.FileContent{}, invalid("invalid file path")
	}
	p, ok := s.cfg.Settings.ModelProfiles[name]
	if !ok {
		return model.FileContent{}, notFound("model not found")
	}
	return s.preview(ctx, modelPrefix(p)+file)
}

func modelPrefix(p settings.Profile) string {
	return datasetsPrefix + strings.TrimSuffix(p.ModelSubPath, "/") + "/"
}

func hyperparameterDefs(p settings.Profile) []model.HyperparameterDef {
	out := make([]model.HyperparameterDef, 0, len(p.Hyperparameters))
	for _, h := range p.Hyperparameters {
		var def any
		switch h.Key {
		case "epochs":
			def = p.Defaults.Epochs
		case "batch_size_per_gpu":
			def = p.Defaults.BatchSizePerGPU
		case "learning_rate":
			def = p.Defaults.LearningRate
		}
		out = append(out, model.HyperparameterDef{
			Key: h.Key, Level: h.Level, Type: h.Type, Default: def, Min: h.Min, ExclusiveMin: h.ExclusiveMin, Max: h.Max,
		})
	}
	return out
}

// Datasets lists every dataset that has a manifest in S3, without its versions.
func (s *JobService) Datasets(ctx context.Context) ([]model.Dataset, error) {
	all, err := s.loadDatasets(ctx)
	if err != nil {
		return nil, err
	}
	for i := range all {
		all[i].Versions = nil
	}
	return all, nil
}

// Dataset returns one dataset with its versions and files.
func (s *JobService) Dataset(ctx context.Context, id string) (model.Dataset, error) {
	if !validDataID.MatchString(id) {
		return model.Dataset{}, invalid("invalid dataset id")
	}
	all, err := s.loadDatasets(ctx)
	if err != nil {
		return model.Dataset{}, err
	}
	for _, d := range all {
		if d.ID == id {
			return d, nil
		}
	}
	return model.Dataset{}, notFound("dataset not found")
}

// DatasetFile returns the start of one file of a dataset version.
func (s *JobService) DatasetFile(ctx context.Context, id string, version int, name string) (model.FileContent, error) {
	if !validDataID.MatchString(id) || version < 1 || !safeName(name) {
		return model.FileContent{}, invalid("invalid file path")
	}
	return s.preview(ctx, datasetPrefix(id, version)+name)
}

func (s *JobService) loadDatasets(ctx context.Context) ([]model.Dataset, error) {
	objs, err := s.objects.List(ctx, datasetIDPrefix)
	if err != nil {
		return nil, upstream("list datasets", err)
	}
	byID := map[string]*model.Dataset{}
	for _, o := range objs {
		m := manifestKey.FindStringSubmatch(o.Key)
		if m == nil {
			continue
		}
		version, err := strconv.Atoi(m[2])
		if err != nil {
			continue
		}
		manifest, sha, err := s.readManifest(ctx, m[1], version)
		if err != nil {
			return nil, err
		}
		d := byID[m[1]]
		if d == nil {
			d = &model.Dataset{ID: m[1], Name: m[1]}
			byID[m[1]] = d
		}
		if version >= d.LatestVersion {
			d.LatestVersion, d.Format = version, manifest.Format
			if manifest.Name != "" {
				d.Name = manifest.Name
			}
		}
		d.Versions = append(d.Versions, s.versionOf(m[1], version, manifest, sha))
	}

	out := make([]model.Dataset, 0, len(byID))
	for _, d := range byID {
		sort.Slice(d.Versions, func(a, b int) bool { return d.Versions[a].Version < d.Versions[b].Version })
		out = append(out, *d)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (s *JobService) versionOf(id string, version int, m datasetManifest, sha string) model.DatasetVersion {
	v := model.DatasetVersion{
		Version: version, Source: seededVersion, FileCount: len(m.Files), ManifestSHA256: sha,
		URI:   fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, datasetPrefix(id, version)),
		Files: make([]model.StoredFile, 0, len(m.Files)),
	}
	for _, f := range m.Files {
		v.TotalBytes += f.SizeBytes
		v.Files = append(v.Files, model.StoredFile{Name: f.Name, Size: f.SizeBytes, SHA256: f.SHA256})
	}
	return v
}

// readManifest returns the parsed manifest and the sha256 of its bytes (config.json dataset.manifest_sha256).
func (s *JobService) readManifest(ctx context.Context, id string, version int) (datasetManifest, string, error) {
	key := datasetPrefix(id, version) + manifestName
	data, size, err := s.objects.Get(ctx, key, maxMetadataBytes)
	if errors.Is(err, model.ErrNotFound) {
		return datasetManifest{}, "", notFound(fmt.Sprintf("dataset %s v%d not found", id, version))
	}
	if err != nil {
		return datasetManifest{}, "", upstream("read "+key, err)
	}
	if int64(len(data)) < size {
		return datasetManifest{}, "", invalid(key + " is too large")
	}
	var m datasetManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return datasetManifest{}, "", upstream("parse "+key, err)
	}
	return m, sha256Hex(data), nil
}
