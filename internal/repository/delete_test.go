package repository

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/DevOops-Team/Backend/internal/model"
)

// versionedBucket keeps "key#version" entries and answers ListObjectVersions in pages of MaxKeys.
type versionedBucket struct {
	s3Reader
	versions []string // key#version, sorted
	markers  []string
	calls    int
}

func split(e string) (string, string) {
	for i := len(e) - 1; i >= 0; i-- {
		if e[i] == '#' {
			return e[:i], e[i+1:]
		}
	}
	return e, ""
}

func (b *versionedBucket) ListObjectVersions(_ context.Context, in *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	var all []string
	for _, e := range append(slices.Clone(b.versions), b.markers...) {
		if k, _ := split(e); len(k) >= len(*in.Prefix) && k[:len(*in.Prefix)] == *in.Prefix {
			all = append(all, e)
		}
	}
	slices.Sort(all)
	start := 0
	if in.KeyMarker != nil {
		start = slices.Index(all, aws.ToString(in.KeyMarker)+"#"+aws.ToString(in.VersionIdMarker)) + 1
	}
	end := min(start+int(aws.ToInt32(in.MaxKeys)), len(all))
	out := &s3.ListObjectVersionsOutput{IsTruncated: aws.Bool(end < len(all))}
	for _, e := range all[start:end] {
		k, v := split(e)
		if slices.Contains(b.markers, e) {
			out.DeleteMarkers = append(out.DeleteMarkers, types.DeleteMarkerEntry{Key: aws.String(k), VersionId: aws.String(v)})
		} else {
			out.Versions = append(out.Versions, types.ObjectVersion{Key: aws.String(k), VersionId: aws.String(v)})
		}
	}
	if end < len(all) {
		k, v := split(all[end-1])
		out.NextKeyMarker, out.NextVersionIdMarker = aws.String(k), aws.String(v)
	}
	return out, nil
}

func (b *versionedBucket) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	b.calls++
	for _, id := range in.Delete.Objects {
		e := aws.ToString(id.Key) + "#" + aws.ToString(id.VersionId)
		b.versions = slices.DeleteFunc(b.versions, func(x string) bool { return x == e })
		b.markers = slices.DeleteFunc(b.markers, func(x string) bool { return x == e })
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func TestDeleteAllRemovesEveryVersionAndDeleteMarkerUnderThePrefixOnly(t *testing.T) {
	b := &versionedBucket{
		versions: []string{"jobs/aa/gen-0001/step-1/x#v1", "jobs/aa/gen-0001/step-1/x#v2", "jobs/ab/y#v1"},
		markers:  []string{"jobs/aa/gen-0001/step-1/x#m1"},
	}
	for i := range 2500 { // more than two pages
		b.versions = append(b.versions, fmt.Sprintf("jobs/aa/gen-0002/f%04d#v1", i))
	}
	o := &Objects{client: b, bucket: "bucket"}
	if err := o.DeleteAll(t.Context(), "jobs/aa/"); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(b.versions, []string{"jobs/ab/y#v1"}) || len(b.markers) != 0 {
		t.Errorf("left versions %v markers %v", b.versions, b.markers)
	}
	if b.calls < 3 {
		t.Errorf("DeleteObjects calls = %d, want one per page", b.calls)
	}
}

type failingDelete struct{ versionedBucket }

func (f *failingDelete) DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return &s3.DeleteObjectsOutput{Errors: []types.Error{{Key: aws.String("jobs/aa/x"), Code: aws.String("AccessDenied")}}}, nil
}

func TestDeleteAllReportsPerKeyErrors(t *testing.T) {
	b := &failingDelete{versionedBucket{versions: []string{"jobs/aa/x#v1"}}}
	if err := (&Objects{client: b, bucket: "bucket"}).DeleteAll(t.Context(), "jobs/aa/"); err == nil {
		t.Fatal("want an error when S3 refuses a key")
	}
}

func jobset(name, id string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(model.JobSetAPIVersion)
	u.SetKind(model.JobSetKind)
	u.SetNamespace("training")
	u.SetName(name)
	u.SetLabels(map[string]string{model.JobIDLabel: id})
	return u
}

func TestKubeDeleteRemovesEveryGenerationOfTheJobOnly(t *testing.T) {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{JobSetGVR: "JobSetList", WorkloadGVR: "WorkloadList"},
		jobset("job-aa-g0001", "aa"), jobset("job-aa-g0002", "aa"), jobset("job-bb-g0001", "bb"))
	k := &Kube{dyn: dyn, namespace: "training"}
	if err := k.Delete(t.Context(), "aa"); err != nil {
		t.Fatal(err)
	}
	l, err := dyn.Resource(JobSetGVR).Namespace("training").List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(l.Items) != 1 || l.Items[0].GetName() != "job-bb-g0001" {
		t.Errorf("left %v", l.Items)
	}
	if err := k.Delete(t.Context(), "zz"); err != nil {
		t.Errorf("deleting a job with no JobSet: %v", err)
	}
}
