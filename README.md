# Backend

프론트에서 받은 `job.yaml`(JobSet)·`train.py`·학습 데이터로 학습 작업을 제출하고, 진행 상태를 돌려주는 Go 서버입니다.

```
Frontend ──multipart──▶ Backend ──① S3 업로드 (code/<id>/, datasets/<id>/)
                           │
                           └──② JobSet 제출 ──▶ 로컬 k3s (Kueue + MultiKueue, training/queue-auto)
                                                   │ 먼저 승인한 EKS로 전달
                                                   ▼
                                     EKS devoops-a / devoops-b (Kueue → Karpenter GPU 노드)
                                                   │ train.py가 매 step 기록
Frontend ◀──2초 폴링── Backend ◀── ③ DynamoDB devoops-job-progress
```

## 구현 내용

### 1. 작업 제출 `POST /api/jobs`

multipart 필드: `job`(job.yaml), `train`(train.py), `data`(1개 이상)

1. 입력을 먼저 검증한다. 세 필드가 모두 있어야 하고, `job.yaml`은 `jobset.x-k8s.io/v1alpha2 JobSet`이어야 한다. 잘못되면 업로드 전에 400을 반환한다.
2. 작업 ID(8자리 hex)를 만들고 S3에 올린다. 키는 `code/<id>/train.py`와 `datasets/<id>/<파일명>`이다. 파일명은 base name만 써서 경로 탈출을 막는다.
3. `internal/jobspec`이 JobSet을 인프라 규칙에 맞게 고친 뒤 k3s의 `training` namespace에 만든다.
4. S3나 k3s 오류는 502로 반환한다. 업로드가 실패하면 JobSet은 만들지 않는다.

`jobspec.Prepare`가 JobSet에 넣는 것:

| 대상 | 넣는 값 | 이유 |
|---|---|---|
| JobSet | 이름 `job-<id>`, namespace `training` | 작업마다 고유 이름 |
| JobSet 라벨 | `kueue.x-k8s.io/queue-name: queue-auto`, `devoops/job-id: <id>` | MultiKueue 큐로 들어가 먼저 승인한 EKS로 간다. 현재 GPU는 `devoops-a`에만 있어서 사실상 항상 A로 간다 |
| `spec.failurePolicy.maxRestarts` | 없으면 3 | GPU 노드가 사라지면 JobSet 전체가 다시 떠서 standby 노드로 옮겨진다 |
| Pod 라벨 | `devoops/gpu-group: train` | standby placeholder와 같은 그룹 |
| Pod affinity | 같은 그룹과 **같은 zone**(required, `namespaceSelector: {}`) | 학습 노드와 standby 노드를 한 AZ에 모은다. placeholder는 `gpu-standby` namespace에 있어서 모든 namespace를 본다 |
| Pod anti-affinity | 같은 그룹과 **다른 hostname** | GPU 노드 1대에 Pod 1개 |
| toleration | `nvidia.com/gpu=true:NoSchedule` | Karpenter GPU 노드 taint. EKS는 이 toleration을 자동으로 붙이지 않는다 |
| 볼륨 | PVC `s3-code`, `s3-datasets`, `s3-checkpoints`를 subPath `<id>`로 `/code`, `/data`, `/ckpt`에 마운트 | Mountpoint for S3. train.py에 S3 코드가 필요 없다 |
| env | `JOB_ID`, `PROGRESS_TABLE`, `CODE_DIR`, `DATA_DIR`, `CKPT_DIR` | train.py가 쓰는 값 |

사용자가 넣은 env, affinity, toleration, `maxRestarts`는 유지하고 위 값을 추가한다.

### 2. 작업 조회 `GET /api/jobs`, `GET /api/jobs/{id}`

- **k3s에서 읽는 것 (`internal/kube`):** `devoops/job-id` 라벨이 있는 JobSet과, 그 JobSet이 소유한 Kueue Workload.
  - `phase`
    - `Pending`: 승인 대기
    - `Admitted`: 워커가 쿼터를 잡음
    - `Running`: active Pod가 있음
    - `Succeeded` / `Failed`: JobSet의 condition 기준
  - `cluster`: Workload의 `status.clusterName`. MultiKueue가 고른 EKS다.
  - `restarts`: JobSet의 `status.restarts`. 노드를 잃고 복구한 횟수가 여기 보인다.
  - `message`: 대기 이유. 예: `ClusterQueue queue-auto is inactive`
- **DynamoDB에서 읽는 것 (`internal/store`):** `BatchGetItem`으로 읽은 `step`, `totalSteps`, `loss`, `lastCheckpointStep`, `nodeName`, `status`. 한 번에 100개씩 나눠 읽는다.
  - DynamoDB 조회가 실패해도 큐 상태는 그대로 반환한다(`progress: null`).
- 결과는 최신순으로 정렬한다.

### 3. 기타

- `GET /healthz`
- CORS: `ALLOWED_ORIGIN` 한 곳만 허용
- 한 번에 올릴 수 있는 크기는 최대 2GiB

## 디렉터리

```
cmd/server/        진입점 (환경변수 → 의존성 조립)
internal/jobspec/  JobSet 검증·주입 (순수 로직)
internal/api/      HTTP 핸들러. S3·k8s·DynamoDB는 인터페이스로 받는다
internal/kube/     k3s dynamic client: JobSet 생성, JobSet + Workload → 상태
internal/store/    S3 업로드(multipart uploader), DynamoDB 진행 상태 조회
examples/          job.yaml(단일 GPU JobSet), train.py(예시 학습 코드)
```

## 예시 작업 (`examples/`)

- `job.yaml`: Pod 1개 × GPU 1개(분산 학습 없음). `python $CODE_DIR/train.py`를 실행한다.
- `train.py`가 지키는 규칙:
  - S3 코드가 없다. `/code`, `/data`, `/ckpt` 경로만 쓴다.
  - 체크포인트는 1000 step마다 `ckpt-<step>.pt`로 저장한다. **메모리에 직렬화한 뒤 새 파일에 한 번에** 쓴다. Mountpoint는 rename과 append를 지원하지 않기 때문이다.
  - 최근 체크포인트 2개만 남긴다.
  - 시작할 때 `/ckpt`의 최신 체크포인트에서 재개한다.
  - 매 step DynamoDB에 진행 상태를 덮어쓴다. DynamoDB 오류가 나도 학습은 멈추지 않는다.

## 설정 (환경변수)

| 변수 | 기본값 | 설명 |
|---|---|---|
| `S3_BUCKET` | (필수) | `infra` 20-data의 `bucket_name` |
| `PROGRESS_TABLE` | `devoops-job-progress` | 20-data의 `progress_table_name` |
| `AWS_REGION` | `us-east-1` | |
| `KUBECONFIG` / `KUBE_CONTEXT` | 기본 kubeconfig / 현재 context | 로컬 k3s |
| `TRAINING_NAMESPACE` | `training` | 50-management 출력과 같아야 한다 |
| `QUEUE_NAME` | `queue-auto` | 50-management 출력과 같아야 한다 |
| `ALLOWED_ORIGIN` | `http://localhost:5173` | 프론트 주소 |
| `LISTEN_ADDR` | `:8080` | |

AWS 자격증명은 SDK 기본 체인을 쓴다. 백엔드 전용 Role은 `infra` 20-data의 `backend_role_arn`이다. 권한은 `code/`·`datasets/` 쓰기, 버킷 읽기, 진행 상태 읽기만 있다.

## 테스트

TDD로 작성했다. S3·k8s·DynamoDB는 fake로 바꿔서 테스트한다. k8s 쪽은 `client-go/dynamic/fake`를 쓴다.

```sh
go test ./...
go vet ./...
```

- `jobspec`: 주입하는 각 값, 사용자 값 유지, 잘못된 입력 거부, `examples/job.yaml`이 통과하는지
- `api`: 업로드 키, 입력 검증 순서(검증이 먼저고 업로드가 나중), 경로 탈출 차단, 502 처리, 진행 상태 병합·정렬, DynamoDB 실패 시 동작, 404, CORS
- `kube`: Workload와 JobSet 상태 → phase·cluster·restarts·message, 백엔드가 만들지 않은 JobSet은 제외, 생성
- `store`: DynamoDB 항목 디코딩, 100개 단위 분할

## 로컬 Docker 실행

로컬 관리 클러스터는 k3d(`devoops-mgmt`)다. 백엔드 컨테이너를 k3d 네트워크에 붙여서 k3s API에 `k3d-devoops-mgmt-serverlb:6443`으로 접속한다.

```sh
# 1) 관리 클러스터 (최초 1회)
k3d cluster create devoops-mgmt --api-port 127.0.0.1:6550 \
  --kubeconfig-update-default=false --kubeconfig-switch-context=false
k3d kubeconfig get devoops-mgmt > ~/.kube/devoops-mgmt.yaml
# 컨테이너용: 서버 주소를 k3d 네트워크 안의 이름으로 바꾼다
sed 's#https://127.0.0.1:6550#https://k3d-devoops-mgmt-serverlb:6443#' \
  ~/.kube/devoops-mgmt.yaml > ~/.kube/devoops-mgmt-docker.yaml
# Kueue·JobSet·queue-auto 설치는 infra/envs/dev/50-management apply

# 2) 이미지 빌드
docker build -t devoops-backend:dev .

# 3) 실행 (AWS 자격증명은 백엔드 Role 프로필에서 환경변수로 넘긴다)
aws configure export-credentials --profile devoops-backend --format env-no-export > .env.aws
docker run -d --name devoops-backend --network k3d-devoops-mgmt -p 8080:8080 \
  -v ~/.kube/devoops-mgmt-docker.yaml:/kube/config:ro -e KUBECONFIG=/kube/config \
  --env-file .env.aws \
  -e S3_BUCKET=<20-data bucket_name> -e PROGRESS_TABLE=devoops-job-progress \
  devoops-backend:dev

curl localhost:8080/healthz
```

`.env*`는 `.gitignore`에 들어 있다.
