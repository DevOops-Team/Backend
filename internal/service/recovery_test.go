package service

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/DevOops-Team/Backend/internal/model"
)

const recDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type recJobs struct {
	job model.Job
	err error
}

func (f recJobs) Get(context.Context, string) (model.Job, error) { return f.job, f.err }

type recJobSets map[string]string // name → UID

func (f recJobSets) JobSetUID(_ context.Context, name string) (string, error) {
	uid, ok := f[name]
	if !ok {
		return "", model.ErrNotFound
	}
	return uid, nil
}

type recDigests struct {
	digest string
	err    error
}

func (f recDigests) JobsetSHA256(context.Context, string) (string, error) { return f.digest, f.err }

type recPlacement struct {
	p   model.Placement
	err error
}

func (f recPlacement) Placement(context.Context, string, int) (model.Placement, error) {
	return f.p, f.err
}

func runningJob() model.Job {
	return model.Job{JobStatus: model.JobStatus{ID: "abc123", Status: model.StatusRunning, Generation: 1, GPUs: 2, Cluster: "eks-a"}}
}

func goodPlacement() model.Placement {
	return model.Placement{Cluster: "eks-a", Nodes: []string{"ip-10-0-1-1", "ip-10-0-1-2"}, Generation: 1, ObservedAt: 1800000000}
}

type recCase struct {
	jobs      *recJobs // nil = a running job
	jobsets   recJobSets
	digests   recDigests
	placement PlacementSource
}

func (c recCase) service() *RecoveryService {
	if c.jobsets == nil {
		c.jobsets = recJobSets{"job-abc123-g0001": "uid-1"}
	}
	if c.digests == (recDigests{}) {
		c.digests = recDigests{digest: recDigest}
	}
	if c.placement == nil {
		c.placement = recPlacement{p: goodPlacement()}
	}
	if c.jobs == nil {
		c.jobs = &recJobs{job: runningJob()}
	}
	return NewRecoveryService("ckpt-bucket", c.jobs, c.jobsets, c.digests, c.placement)
}

func TestRecoveryMetadataOfARunningGeneration(t *testing.T) {
	got, err := recCase{}.service().Metadata(t.Context(), "abc123", 1)
	if err != nil {
		t.Fatal(err)
	}
	want := model.RecoveryMetadata{
		SchemaVersion: 1, JobID: "abc123", Generation: 1, RecoveryEligible: true,
		JobsetURI: "s3://ckpt-bucket/code/abc123/jobset.json", JobsetSHA256: recDigest,
		GPUCount: 2, Cluster: "eks-a", ActiveNodes: []string{"ip-10-0-1-1", "ip-10-0-1-2"}, PlacementGeneration: 1,
		JobsetName: "job-abc123-g0001", JobsetUID: "uid-1", ObservedAt: 1800000000, AllowRestartFromScratch: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("metadata =\n%+v\nwant\n%+v", got, want)
	}
}

func TestRecoveryEligibilityFollowsTheJobStatus(t *testing.T) {
	for st, want := range map[model.Status]bool{
		model.StatusRunning: true, model.StatusFailed: true, model.StatusRecovering: true, model.StatusAdmitted: true,
		model.StatusCancelled: false, model.StatusStopping: false, model.StatusSucceeded: false,
	} {
		job := runningJob()
		job.Status = st
		got, err := recCase{jobs: &recJobs{job: job}}.service().Metadata(t.Context(), "abc123", 1)
		if err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if got.RecoveryEligible != want {
			t.Errorf("%s: recovery_eligible = %v, want %v", st, got.RecoveryEligible, want)
		}
	}
}

// An older generation (the job moved on) is answered from its own JobSet; the MultiKueue cluster of the
// current generation is not compared with it.
func TestRecoveryMetadataOfAnOlderGeneration(t *testing.T) {
	job := runningJob()
	job.Generation, job.Cluster = 2, "eks-b"
	c := recCase{jobs: &recJobs{job: job}, jobsets: recJobSets{"job-abc123-g0001": "uid-1", "job-abc123-g0002": "uid-2"}}
	got, err := c.service().Metadata(t.Context(), "abc123", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.JobsetName != "job-abc123-g0001" || got.JobsetUID != "uid-1" || got.Cluster != "eks-a" {
		t.Errorf("metadata = %+v", got)
	}
}

func TestRecoveryMetadataErrors(t *testing.T) {
	dbDown := errors.New("connection refused")
	for name, tc := range map[string]struct {
		c    recCase
		id   string
		gen  int
		kind Kind
		code string
	}{
		"bad id":               {id: "ABC/1", gen: 1, kind: KindUnprocessable, code: "INVALID_REQUEST"},
		"generation 0":         {gen: 0, kind: KindUnprocessable, code: "INVALID_REQUEST"},
		"no job":               {c: recCase{jobs: &recJobs{err: notFound("job not found")}}, gen: 1, kind: KindNotFound, code: "NOT_FOUND"},
		"job lookup fails":     {c: recCase{jobs: &recJobs{err: upstream("list jobsets", dbDown)}}, gen: 1, kind: KindUnavailable, code: "UPSTREAM"},
		"generation ahead":     {gen: 2, kind: KindNotFound, code: "GENERATION_NOT_FOUND"},
		"no jobset":            {c: recCase{jobsets: recJobSets{}}, gen: 1, kind: KindNotFound, code: "GENERATION_NOT_FOUND"},
		"no fencing item":      {c: recCase{digests: recDigests{err: model.ErrNotFound}}, gen: 1, kind: KindUnprocessable, code: "FENCING_ITEM_MISSING"},
		"malformed hash":       {c: recCase{digests: recDigests{digest: "ABC"}}, gen: 1, kind: KindUnprocessable, code: "METADATA_INVALID"},
		"fencing read fails":   {c: recCase{digests: recDigests{err: dbDown}}, gen: 1, kind: KindUnavailable, code: "UPSTREAM"},
		"no placement source":  {c: recCase{placement: NoPlacement{}}, gen: 1, kind: KindUnavailable, code: "PLACEMENT_UNAVAILABLE"},
		"placement read fails": {c: recCase{placement: recPlacement{err: dbDown}}, gen: 1, kind: KindUnavailable, code: "UPSTREAM"},
	} {
		id := tc.id
		if id == "" {
			id = "abc123"
		}
		_, err := tc.c.service().Metadata(t.Context(), id, tc.gen)
		var se *Error
		if !errors.As(err, &se) || se.Kind != tc.kind || se.ErrorCode() != tc.code {
			t.Errorf("%s: err = %v (%+v), want %s %s", name, err, se, tc.kind, tc.code)
		}
	}
}

// A job submitted before GPU-119 has a fencing item without the hash: 422, never a hash made from the
// object as it is now.
func TestRecoveryMetadataWithoutARecordedHash(t *testing.T) {
	svc := NewRecoveryService("b", recJobs{job: runningJob()}, recJobSets{"job-abc123-g0001": "uid-1"},
		recDigests{digest: ""}, recPlacement{p: goodPlacement()})
	_, err := svc.Metadata(t.Context(), "abc123", 1)
	var se *Error
	if !errors.As(err, &se) || se.Kind != KindUnprocessable || se.Code != "JOBSET_DIGEST_MISSING" {
		t.Errorf("err = %v, want 422 JOBSET_DIGEST_MISSING", err)
	}
}

// A placement that does not hold together is not passed on: 503 like a missing one.
func TestRecoveryMetadataRefusesAnIncompletePlacement(t *testing.T) {
	for name, mutate := range map[string]func(*model.Placement){
		"one node for 2 GPUs": func(p *model.Placement) { p.Nodes = p.Nodes[:1] },
		"repeated node":       func(p *model.Placement) { p.Nodes = []string{"n1", "n1"} },
		"empty node":          func(p *model.Placement) { p.Nodes = []string{"n1", ""} },
		"no cluster":          func(p *model.Placement) { p.Cluster = "" },
		"other cluster":       func(p *model.Placement) { p.Cluster = "eks-b" }, // MultiKueue says eks-a
		"later generation":    func(p *model.Placement) { p.Generation = 2 },
		"no generation":       func(p *model.Placement) { p.Generation = 0 },
		"no observation time": func(p *model.Placement) { p.ObservedAt = 0 },
	} {
		p := goodPlacement()
		mutate(&p)
		_, err := recCase{placement: recPlacement{p: p}}.service().Metadata(t.Context(), "abc123", 1)
		var se *Error
		if !errors.As(err, &se) || se.Kind != KindUnavailable || se.Code != "PLACEMENT_UNAVAILABLE" {
			t.Errorf("%s: err = %v, want 503 PLACEMENT_UNAVAILABLE", name, err)
		}
	}
}
