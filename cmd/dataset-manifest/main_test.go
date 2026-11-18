package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, dir string, files map[string]string) {
	for name, body := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBuildListsEveryFileLikeAnUpload(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, map[string]string{"model/config.json": "{}", "model/tok/vocab.json": "[]", "data/a.jsonl": "x\n",
		"code/train.py": "print()\n", "manifest.json": "old"})
	m, err := build(dir, "SmolLM2-135M", time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range m.Files {
		names = append(names, f.Name)
	}
	if got := strings.Join(names, ","); got != "code/train.py,data/a.jsonl,model/config.json,model/tok/vocab.json" {
		t.Errorf("files %s", got)
	}
	if m.ModelBytes != 4 || m.DataBytes != 2 || m.Format != "text-jsonl" || m.Model != "SmolLM2-135M" {
		t.Errorf("manifest %+v", m)
	}
}

func TestBuildRejectsAnIncompleteOrStrayFolder(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"no train.py":  {"model/config.json": "{}", "data/a.jsonl": "x"},
		"no data":      {"model/config.json": "{}", "code/train.py": "x"},
		"a stray file": {"model/config.json": "{}", "data/a.jsonl": "x", "code/train.py": "x", "README.md": "x"},
	} {
		dir := t.TempDir()
		write(t, dir, files)
		if _, err := build(dir, "m", time.Unix(0, 0)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
