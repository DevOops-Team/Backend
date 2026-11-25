package model

import "time"

// UploadRequest is POST /api/uploads (GPU-278): the files the browser will PUT straight to S3, so a
// submission never goes through the Cloudflare proxy (100MB request limit). Paths are the multipart part
// names of POST /api/jobs: model/<folder>/<path>, data/<file>.jsonl, train/<file>.py.
type UploadRequest struct {
	Files []UploadFile `json:"files"`
}

type UploadFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex; S3 checks the uploaded bytes against it (signed x-amz-checksum-sha256)
}

// UploadTicket is the answer: one presigned PUT per file. Headers must be sent with the PUT as given (they
// are signed). Submit with spec.upload.id once every PUT is done.
type UploadTicket struct {
	ID        string         `json:"id"`
	ExpiresAt time.Time      `json:"expiresAt"`
	Files     []UploadTarget `json:"files"`
}

type UploadTarget struct {
	Path    string            `json:"path"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// ObjectHead is what HeadObject tells about an uploaded object.
type ObjectHead struct {
	Size   int64
	SHA256 string // hex, from the object's full-object SHA-256 checksum; empty when it has none
}
