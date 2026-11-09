package model

import "fmt"

// Kubernetes names shared by the service (writes them into the JobSet) and the repository (reads them back).
const (
	JobSetAPIVersion = "jobset.x-k8s.io/v1alpha2"
	JobSetKind       = "JobSet"

	QueueLabel         = "kueue.x-k8s.io/queue-name"
	PriorityClassLabel = "kueue.x-k8s.io/priority-class"
	JobIDLabel         = "devoops/job-id"
	GenerationLabel    = "devoops/generation"
	GPUGroupLabel      = "devoops/gpu-group"
	GPUGroupValue      = "train"

	StrategyAnnotation     = "devoops/strategy"
	WorldSizeAnnotation    = "devoops/world-size"
	ModelAnnotation        = "devoops/model"
	DatasetAnnotation      = "devoops/dataset" // "<id>/v<version>"
	ConfigSHA256Annotation = "devoops/config-sha256"

	// 임시 구성: 서비스 DB가 없어서 작업 이름과 사용자가 바꾼 값도 JobSet에 남긴다.
	NameAnnotation            = "devoops/name"
	HyperparametersAnnotation = "devoops/hyperparameters"
)

// JobSetName returns the JobSet name of one generation of a job, e.g. job-3f9a1c2e-g0001.
// The same generation has the same name, so a duplicate submission fails with AlreadyExists.
func JobSetName(jobID string, generation int) string {
	return fmt.Sprintf("job-%s-g%04d", jobID, generation)
}
