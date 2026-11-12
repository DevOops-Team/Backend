# 학습 작업 입력 형식 (이전 방식)

> 더 이상 쓰지 않는다. `POST /api/jobs`는 multipart가 아니라 JSON이다. 정본은 `api/openapi.yaml`과 Confluence `서비스 API 명세`다.

프론트(웹 제출 화면)가 백엔드 `POST /api/jobs`로 보내는 입력의 정본이다. 백엔드 검증(`internal/service`, `internal/service/jobspec`)과 프론트 폼(`Frontend/src/components/UploadForm.tsx`)이 이 문서를 따른다. 바꾸면 이 문서부터 고친다. 응답 JSON 스키마 등 API 전체는 Swagger(`/swagger/`, `api/openapi.yaml`)에 있다.

- 관련 티켓: GPU-76 (프론트 입력 형식 확정)
- JobSet 템플릿 자체(GPU-61, 데이터 파트)가 확정되면 2절의 "사용자가 쓰는 것"을 그 템플릿에 맞춘다.

## 1. 요청

`POST /api/jobs`, `multipart/form-data`

| 필드 | 개수 | 내용 | 저장 위치(S3) | Pod 안 경로 |
|---|---|---|---|---|
| `job` | 정확히 1 | JobSet 매니페스트 (`job.yaml`) | `code/<id>/job.yaml` (원본 그대로) | `/code/job.yaml` |
| `train` | 정확히 1 | 학습 코드 | `code/<id>/train.py` (올린 파일명과 상관없이 이 이름) | `$CODE_DIR/train.py` |
| `data` | 1개 이상 | 학습 데이터 파일 | `datasets/<id>/<파일명>` | `$DATA_DIR/<파일명>` |

- 요청 전체 크기는 **최대 2GiB**다.
- 데이터 파일명은 base name만 쓴다. 폴더 구조는 유지되지 않고, 같은 이름을 두 번 올리면 나중 것이 남는다.
- `<id>`는 백엔드가 만드는 8자리 hex 작업 ID다.

### 응답

| 코드 | 언제 | 본문 |
|---|---|---|
| `201` | 제출 성공 | `{"id": "<id>", "name": "job-<id>"}` |
| `400` | 입력 오류 (아래 표). S3 업로드 전에 거른다 | `{"error": "<메시지>"}` |
| `502` | S3 업로드나 관리 클러스터 JobSet 생성 실패 | `{"error": "<메시지>"}` |

| 400 메시지 | 원인 |
|---|---|
| `expected multipart form: …` | multipart가 아니거나 2GiB 초과 |
| `job (job.yaml) is required` / `train (train.py) is required` / `at least one data file is required` | 필드 누락 |
| `job.yaml is not valid YAML` / `job.yaml is not a YAML object` | YAML 파싱 실패 |
| `job.yaml must be jobset.x-k8s.io/v1alpha2 JobSet, got …` | apiVersion·kind가 다름 |
| `job.yaml must define spec.replicatedJobs` | `replicatedJobs`가 없거나 비어 있음 |
| `spec.replicatedJobs[i]: missing template.spec.template (pod template)` / `pod spec has no containers` | Pod 템플릿·컨테이너 없음 |
| `invalid data filename: …` | 데이터 파일명이 `.`, `..` 등 |

프론트는 이 메시지를 그대로 보여 준다. 세 필드가 다 있어야 제출 버튼이 켜지고, 파일 선택 창은 `job`은 `.yaml/.yml`, `train`은 `.py`만 보여 준다(확장자는 백엔드가 검사하지 않는다).

## 2. `job.yaml`

`jobset.x-k8s.io/v1alpha2` `JobSet` 하나. 예시: `examples/job.yaml`(DDP 2노드), `examples/gpt2/job.yaml`.

### 사용자가 쓰는 것

- `spec.replicatedJobs[]`: 각 항목에 Job 템플릿(`template.spec.template.spec.containers` 1개 이상)
- 컨테이너 이미지, `command`/`args`(보통 `torchrun … ${CODE_DIR}/train.py`), `resources` (GPU는 `nvidia.com/gpu: 1` — 노드 1대에 GPU 1개, Pod 1개)
- 필요하면 `parallelism`/`completions`/`completionMode: Indexed`, `spec.failurePolicy.maxRestarts`, 추가 env·label·affinity·toleration

### 백엔드가 채우는 것 (쓰지 않아도 된다)

| 항목 | 동작 |
|---|---|
| `metadata.name`, `metadata.namespace` | **덮어쓴다**: `job-<id>`, `training` |
| JobSet 라벨 `kueue.x-k8s.io/queue-name`, `devoops/job-id` | 추가(같은 키면 덮어씀) |
| `spec.failurePolicy.maxRestarts` | 없을 때만 3 |
| Pod 라벨 `devoops/gpu-group: train`, zone affinity, hostname anti-affinity, `nvidia.com/gpu` toleration | 사용자 값 뒤에 추가 |
| 볼륨 `s3-code`·`s3-datasets`·`s3-checkpoints`·`progress`, 모든 컨테이너의 마운트 `/code`·`/data`·`/ckpt`·`/progress` | 추가. **같은 이름의 볼륨이나 같은 경로의 마운트를 쓰면 안 된다** (Kubernetes가 거부) |
| env `JOB_ID`, `PROGRESS_FILE`, `CODE_DIR`, `DATA_DIR`, `CKPT_DIR`, `AWS_REGION`, `AWS_DEFAULT_REGION` | 같은 이름이 있으면 **덮어쓴다** |
| `initContainers`에 `progress-reporter` sidecar | 추가 |

자세한 이유는 [README `jobspec.Prepare`](../README.md#1-작업-제출-post-apijobs) 표를 본다.

## 3. `train.py`

컨테이너 안에서 아래 env만 보고 동작해야 한다. S3·DynamoDB SDK는 쓰지 않는다.

| env | 값 | 용도 |
|---|---|---|
| `CODE_DIR` | `/code` | `train.py`, `job.yaml` (읽기 전용) |
| `DATA_DIR` | `/data` | 올린 데이터 파일 (읽기 전용) |
| `CKPT_DIR` | `/ckpt` | 체크포인트 (쓰기). Mountpoint for S3라 **rename·append 불가** → 메모리에 직렬화 후 새 파일에 한 번에 쓴다 |
| `PROGRESS_FILE` | `/progress/progress.json` | 진행 상태 (아래) |
| `JOB_ID` | `<id>` | 로그 등 |

### 진행 상태 파일

DDP라면 rank 0만 쓴다. 임시 파일에 쓰고 `os.replace`로 바꾼다(emptyDir라 가능). sidecar가 2초마다 읽어 바뀌었으면 DynamoDB `devoops-job-progress`에 그대로 덮어쓴다.

```json
{"status": "RUNNING", "step": 1200, "total_steps": 5000, "loss": 0.0123, "last_checkpoint_step": 1000}
```

| 키 | 타입 | 프론트 표시 |
|---|---|---|
| `status` | `RUNNING` \| `COMPLETED` \| `FAILED` | 상태 |
| `step`, `total_steps` | 정수 | 진행 바 |
| `loss` | 실수 | loss |
| `last_checkpoint_step` | 정수 | 마지막 체크포인트 |

`node_name`과 `updated_at`은 sidecar가 붙인다. 파일이 없으면 프론트에 `진행 기록 없음`이 표시될 뿐 학습에는 영향이 없다.

## 4. 학습 데이터

- 형식은 `train.py`가 정한다. 백엔드는 내용을 보지 않는다.
- 예시 `train.py`는 `$DATA_DIR/*.csv`를 모두 읽는다: 헤더 없음, 숫자 열, **마지막 열이 레이블**. 예시 데이터는 `examples/make_data.py`로 만든다 ([README](../README.md#학습-데이터-make_datapy--traincsv)).
- 웹 화면의 파일 보기는 앞 64KiB만 보여 준다.
