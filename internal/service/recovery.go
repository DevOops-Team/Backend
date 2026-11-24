package service

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"

	"github.com/DevOops-Team/Backend/internal/model"
)

// JobLookup finds a job the way the screens see it (JobService.Get).
type JobLookup interface {
	Get(ctx context.Context, id string) (model.Job, error)
}

// JobSetUIDs reads JobSets on the management cluster.
type JobSetUIDs interface {
	// JobSetUID returns the UID of the named JobSet, or model.ErrNotFound.
	JobSetUID(ctx context.Context, name string) (string, error)
}

// JobsetDigests reads the jobset.json hash recorded at submission (the fencing item).
type JobsetDigests interface {
	// JobsetSHA256 returns model.ErrNotFound when the job has no fencing item and "" when it has no hash.
	JobsetSHA256(ctx context.Context, jobID string) (string, error)
}

// PlacementSource tells where a generation of a job runs, from a trusted placement record.
type PlacementSource interface {
	// Placement returns model.ErrPlacementUnavailable when it has no such record.
	Placement(ctx context.Context, jobID string, generation int) (model.Placement, error)
}

// NoPlacement is the placement source until one is connected: the backend sees the management cluster
// only, and the worker nodes of a job are on the EKS workers. Every lookup is unavailable (503), as the
// contract asks instead of guessing nodes.
type NoPlacement struct{}

func (NoPlacement) Placement(context.Context, string, int) (model.Placement, error) {
	return model.Placement{}, model.ErrPlacementUnavailable
}

var (
	sha256HexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	clusterPattern   = regexp.MustCompile(`^eks-[a-z0-9]+$`)
)

// RecoveryService answers the recovery controller's lookup of a failed job (GPU-119, K8s
// recovery-controller/docs/backend-metadata-api.md v1). It only reads: the job (management cluster and
// data database), the generation's management JobSet, the fencing item's jobset.json hash and the
// placement source. Nothing it returns is made up: a missing fact is an error, never an empty or
// guessed field.
type RecoveryService struct {
	bucket    string
	jobs      JobLookup
	jobsets   JobSetUIDs
	digests   JobsetDigests
	placement PlacementSource
}

func NewRecoveryService(bucket string, jobs JobLookup, jobsets JobSetUIDs, digests JobsetDigests, placement PlacementSource) *RecoveryService {
	if placement == nil {
		placement = NoPlacement{}
	}
	return &RecoveryService{bucket: bucket, jobs: jobs, jobsets: jobsets, digests: digests, placement: placement}
}

// Metadata returns the recovery metadata of generation gen of job id.
//
//   - 404: no such job, or the generation has no JobSet on the management cluster
//   - 422: bad id or generation, or permanent metadata problems (no fencing item, no recorded hash)
//   - 503: a dependency failed or the placement is not known (yet); the controller retries
func (s *RecoveryService) Metadata(ctx context.Context, id string, gen int) (model.RecoveryMetadata, error) {
	if !validJobID.MatchString(id) {
		return model.RecoveryMetadata{}, unprocessable("INVALID_REQUEST", "invalid job id")
	}
	if gen < 1 {
		return model.RecoveryMetadata{}, unprocessable("INVALID_REQUEST", "generation must be an integer of at least 1")
	}

	job, err := s.jobs.Get(ctx, id)
	if err != nil {
		return model.RecoveryMetadata{}, transient(err)
	}
	if gen > job.Generation {
		return model.RecoveryMetadata{}, &Error{Kind: KindNotFound, Code: "GENERATION_NOT_FOUND",
			Msg: fmt.Sprintf("job %s is at generation %d, not %d", id, job.Generation, gen)}
	}
	if job.GPUs < 1 {
		return model.RecoveryMetadata{}, unprocessable("METADATA_INVALID", "the job's GPU count is not known")
	}

	name := model.JobSetName(id, gen)
	uid, err := s.jobsets.JobSetUID(ctx, name)
	if errors.Is(err, model.ErrNotFound) {
		return model.RecoveryMetadata{}, &Error{Kind: KindNotFound, Code: "GENERATION_NOT_FOUND",
			Msg: "JobSet " + name + " is not on the management cluster"}
	}
	if err != nil {
		return model.RecoveryMetadata{}, unavailable("UPSTREAM", "read JobSet "+name+" failed: "+err.Error(), err)
	}
	if uid == "" {
		return model.RecoveryMetadata{}, unavailable("UPSTREAM", "JobSet "+name+" has no UID", nil)
	}

	digest, err := s.digests.JobsetSHA256(ctx, id)
	if errors.Is(err, model.ErrNotFound) {
		return model.RecoveryMetadata{}, unprocessable("FENCING_ITEM_MISSING", "job "+id+" has no fencing item")
	}
	if err != nil {
		return model.RecoveryMetadata{}, unavailable("UPSTREAM", "read fencing item failed: "+err.Error(), err)
	}
	if digest == "" {
		return model.RecoveryMetadata{}, unprocessable("JOBSET_DIGEST_MISSING",
			"job "+id+" has no jobset.json hash recorded at submission (submitted before GPU-119)")
	}
	if !sha256HexPattern.MatchString(digest) {
		return model.RecoveryMetadata{}, unprocessable("METADATA_INVALID", "the recorded jobset.json hash is not 64 lowercase hex digits")
	}

	p, err := s.placement.Placement(ctx, id, gen)
	if errors.Is(err, model.ErrPlacementUnavailable) {
		return model.RecoveryMetadata{}, unavailable("PLACEMENT_UNAVAILABLE",
			"no trusted placement of "+name+" is available yet", err)
	}
	if err != nil {
		return model.RecoveryMetadata{}, unavailable("UPSTREAM", "read placement failed: "+err.Error(), err)
	}
	if msg := placementProblem(p, gen, job); msg != "" {
		return model.RecoveryMetadata{}, unavailable("PLACEMENT_UNAVAILABLE", "placement of "+name+": "+msg, nil)
	}

	return model.RecoveryMetadata{
		SchemaVersion:           model.RecoverySchemaVersion,
		JobID:                   id,
		Generation:              gen,
		RecoveryEligible:        recoveryEligible(job.Status),
		JobsetURI:               fmt.Sprintf("s3://%s/code/%s/jobset.json", s.bucket, id),
		JobsetSHA256:            digest,
		GPUCount:                job.GPUs,
		Cluster:                 p.Cluster,
		ActiveNodes:             slices.Clone(p.Nodes),
		PlacementGeneration:     p.Generation,
		JobsetName:              name,
		JobsetUID:               uid,
		ObservedAt:              p.ObservedAt,
		AllowRestartFromScratch: true,
	}, nil
}

// recoveryEligible is false for a job the user stopped (or is stopping) and one that finished. A failed
// job stays eligible: the controller decides from the fault.
func recoveryEligible(st model.Status) bool {
	switch st {
	case model.StatusCancelled, model.StatusStopping, model.StatusSucceeded:
		return false
	}
	return true
}

// placementProblem checks a placement before it is returned: one distinct, non-empty node per GPU, a
// worker cluster, the requested generation exactly (the controller refuses any other: an earlier
// generation's nodes are not where the failed one ran), and an observation time. A placement that fails is
// treated as not available rather than passed on.
func placementProblem(p model.Placement, gen int, job model.Job) string {
	if !clusterPattern.MatchString(p.Cluster) {
		return "cluster " + strconv.Quote(p.Cluster) + " is not a worker cluster"
	}
	if job.Generation == gen && job.Cluster != "" && job.Cluster != p.Cluster {
		return "cluster " + p.Cluster + " differs from the cluster MultiKueue reports (" + job.Cluster + ")"
	}
	if len(p.Nodes) != job.GPUs {
		return fmt.Sprintf("%d nodes for %d GPUs", len(p.Nodes), job.GPUs)
	}
	seen := map[string]bool{}
	for _, n := range p.Nodes {
		if n == "" || seen[n] {
			return "empty or repeated node name"
		}
		seen[n] = true
	}
	if p.Generation != gen {
		return fmt.Sprintf("placement generation %d is not the requested generation %d", p.Generation, gen)
	}
	if p.ObservedAt <= 0 {
		return "no observation time"
	}
	return ""
}

func unprocessable(code, msg string) error {
	return &Error{Kind: KindUnprocessable, Code: code, Msg: msg}
}

func unavailable(code, msg string, err error) error {
	return &Error{Kind: KindUnavailable, Code: code, Msg: msg, Err: err}
}

// transient passes a not-found error on and turns any other failure into 503, which the controller
// retries (the screens answer 502 for the same upstream failures).
func transient(err error) error {
	var se *Error
	if errors.As(err, &se) && se.Kind == KindNotFound {
		return err
	}
	return unavailable("UPSTREAM", err.Error(), err)
}
