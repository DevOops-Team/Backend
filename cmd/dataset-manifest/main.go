// Command dataset-manifest writes manifest.json for a fixed dataset (GPU-200), the same shape the backend
// writes for an upload, so a job can be submitted with spec.dataset instead of file parts.
//
// The folder must already have the layout of datasets/<id>/v<version>/:
//
//	model/<Hugging Face model files, config.json included>
//	data/<file>.jsonl ...
//	code/train.py
//
// Usage (then copy the folder, manifest included, to s3://<checkpoint bucket>/datasets/<id>/v<version>/):
//
//	go run ./cmd/dataset-manifest -dir ./smollm2-wikitext2 -model SmolLM2-135M
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

func main() {
	dir := flag.String("dir", "", "dataset folder (model/ · data/ · code/train.py)")
	modelName := flag.String("model", "", "model folder name shown to people, e.g. SmolLM2-135M")
	flag.Parse()
	if *dir == "" || *modelName == "" {
		flag.Usage()
		os.Exit(2)
	}
	m, err := build(*dir, *modelName, time.Now())
	if err != nil {
		fmt.Fprintln(os.Stderr, "dataset-manifest:", err)
		os.Exit(1)
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(*dir, "manifest.json"), b, 0o644)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "dataset-manifest:", err)
		os.Exit(1)
	}
	fmt.Printf("manifest.json: %d files, model %d bytes, data %d bytes\n", len(m.Files), m.ModelBytes, m.DataBytes)
}

func build(dir, modelName string, now time.Time) (model.DatasetManifest, error) {
	m := model.DatasetManifest{SchemaVersion: 1, Model: modelName, Format: "text-jsonl", Files: []model.ManifestEntry{},
		CreatedAt: now.UTC().Format(time.RFC3339)}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		switch {
		case name == "manifest.json":
			return nil
		case strings.HasPrefix(name, "model/"):
		case strings.HasPrefix(name, "data/") && strings.HasSuffix(name, ".jsonl") && !strings.Contains(name[len("data/"):], "/"):
		case name == "code/train.py":
		default:
			return fmt.Errorf("%s: only model/…, data/*.jsonl and code/train.py belong in a dataset", name)
		}
		sum, size, err := hashFile(path)
		if err != nil {
			return err
		}
		m.Files = append(m.Files, model.ManifestEntry{Name: name, SHA256: sum, SizeBytes: size})
		if strings.HasPrefix(name, "model/") {
			m.ModelBytes += size
		} else if strings.HasPrefix(name, "data/") {
			m.DataBytes += size
		}
		return nil
	})
	if err != nil {
		return model.DatasetManifest{}, err
	}
	has := map[string]bool{}
	for _, f := range m.Files {
		has[f.Name] = true
		if strings.HasPrefix(f.Name, "data/") {
			has["data/"] = true
		}
	}
	for _, need := range []string{"model/config.json", "data/", "code/train.py"} {
		if !has[need] {
			return model.DatasetManifest{}, errors.New("missing " + need)
		}
	}
	sort.Slice(m.Files, func(a, b int) bool { return m.Files[a].Name < m.Files[b].Name })
	return m, nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}
