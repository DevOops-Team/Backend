package model

import "errors"

// ObjectInfo is one stored object (S3).
type ObjectInfo struct {
	Key  string
	Size int64
}

var (
	// ErrNotFound is returned by the object repository when the key does not exist.
	ErrNotFound = errors.New("object not found")
	// ErrAlreadyExists is returned by the scheduler when the JobSet name is taken.
	ErrAlreadyExists = errors.New("already exists")
	// ErrGenerationMoved is returned by the fencing repository when the job's generation is not the one the
	// caller expected, or a recovery owns it (GPU-240).
	ErrGenerationMoved = errors.New("generation moved")
)
