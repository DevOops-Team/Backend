package model

import "time"

// UploadRequest is POST /api/uploads (GPU-278): the files the browser will PUT straight to S3, so a
// submission never goes through the Cloudflare proxy (100MB request limit). Paths are the multipart part
// names of POST /api/jobs: model/<folder>/<path>, data/<file>.jsonl, train/<file>.py.
type UploadRequest struct {
	// PartBytes is the multipart part size the browser hashed with. Required when a file has Parts.
	PartBytes int64        `json:"partBytes,omitempty"`
	Files     []UploadFile `json:"files"`
}

type UploadFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex, the whole file (manifest; ckpt-init checks it on download)
	// Parts is set for a multipart upload: each part's SHA-256 (hex), part i = bytes [i·PartBytes, (i+1)·PartBytes).
	// S3 checks every part against its signed checksum. Without Parts the file is one PUT checked as a whole.
	Parts []string `json:"parts,omitempty"`
}

// UploadTicket is the answer: one presigned PUT per file. Headers must be sent with the PUT as given (they
// are signed). Submit with spec.upload.id once every PUT is done.
type UploadTicket struct {
	ID        string         `json:"id"`
	ExpiresAt time.Time      `json:"expiresAt"`
	Files     []UploadTarget `json:"files"`
}

// UploadTarget is one file: a single PUT (URL · Headers) or, for a multipart file, one PUT per part (Parts).
// The parts may go in any order and in parallel; the backend completes the upload at submission.
type UploadTarget struct {
	Path    string             `json:"path"`
	URL     string             `json:"url,omitempty"`
	Headers map[string]string  `json:"headers,omitempty"`
	Parts   []UploadPartTarget `json:"parts,omitempty"`
}

type UploadPartTarget struct {
	Number  int32             `json:"number"` // 1-based
	Size    int64             `json:"size"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// UploadedPart is a part S3 has (ListParts).
type UploadedPart struct {
	Number int32
	ETag   string
	Size   int64
	SHA256 string // hex
}

// ObjectHead is what HeadObject tells about an uploaded object.
type ObjectHead struct {
	Size   int64
	SHA256 string // hex, from the object's full-object SHA-256 checksum; empty when it has none
}
