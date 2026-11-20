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
	ExpIDAnnotation        = "devoops/exp-id" // 실험 ID (잡 투입기, GPU-172). jobset.json을 따라 재배치 세대에도 남는다

	// 서비스 DB가 없어서 작업 이름과 사용자가 바꾼 값도 JobSet에 남긴다.
	NameAnnotation            = "devoops/name"
	HyperparametersAnnotation = "devoops/hyperparameters"

	// 사용자 중단 (GPU-204): Backend가 JobSet을 지우기 전에 단다. 감시기(Data)가 삭제를 보고 이 주석이 있으면
	// job.stopped 대신 job.cancelled(payload reason · requested_at)를 보낸다.
	StopReasonAnnotation      = "devoops/stop-reason"
	StopRequestedAtAnnotation = "devoops/stop-requested-at" // RFC 3339 UTC
	StopReasonUser            = "user"

	// 사용자 다시 시작 (GPU-240 · GPU-234): Backend가 중단된 작업을 세대 +1로 다시 제출할 때 새 JobSet에 단다.
	// 감시기(Data)가 이 주석을 보면 복구 재제출본처럼 job.submitted(새 세대) + job.resumed를 보내고, payload의
	// restart_reason으로 측정(v_incidents)이 장애 복구와 구분한다.
	RestartReasonAnnotation      = "devoops/restart-reason"
	RestartRequestedAtAnnotation = "devoops/restart-requested-at" // RFC 3339 UTC
	RestartReasonUser            = "user"

	// RecoveryIDAnnotation is set by the relocator on the generations it re-submits (recovery_render.py).
	RecoveryIDAnnotation = "devoops/recovery-id"
)

// JobSetName returns the JobSet name of one generation of a job, e.g. job-3f9a1c2e-g0001.
// The same generation has the same name, so a duplicate submission fails with AlreadyExists.
func JobSetName(jobID string, generation int) string {
	return fmt.Sprintf("job-%s-g%04d", jobID, generation)
}
