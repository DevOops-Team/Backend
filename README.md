# Backend

프론트의 JSON 요청으로 JobSet을 조립해 학습 작업을 제출하고, 진행 상태를 돌려주는 Go(Gin) 서버입니다.
API는 Confluence `서비스 API 명세`, JobSet은 `JobSet 템플릿 (합의안)`을 따릅니다.

```
Frontend ──JSON──▶ Backend ──① S3 저장 (code/<id>/config.json · train.py · jobset.json)
                      │
                      └──② JobSet 제출 ──▶ 로컬 k3s (Kueue + MultiKueue)
                                              │ 먼저 승인한 EKS로 전달
                                              ▼
                                EKS devoops-a (Kueue → Karpenter GPU 노드 → FSDP 학습)
                                              │ train.py → progress sidecar
Frontend ◀──3초 폴링── Backend ◀── ③ DynamoDB devoops-job-progress · devoops-job-events
```

## FSDP 연동 테스트 실행

아래는 **임시 구성**이다. 합의안과 다른 점은 `internal/service/jobspec/jobspec.go` 머리 주석에 있고, 값은 `internal/settings/default.json`에 모여 있다(`BACKEND_SETTINGS`로 교체).

```sh
export AWS_PROFILE=devoops
aws sso login --profile devoops

# 새 계정이면 최초 1회: Spot 서비스 연결 역할 (Karpenter에는 이 역할을 만들 권한이 없다)
aws iam create-service-linked-role --aws-service-name spot.amazonaws.com

# 1) 관리 클러스터와 인프라
k3d cluster create devoops-mgmt --api-port 127.0.0.1:6550 \
  --kubeconfig-update-default=false --kubeconfig-switch-context=false
k3d kubeconfig get devoops-mgmt > ~/.kube/devoops-mgmt.yaml
(cd ../infra && ./run.sh init && ./run.sh apply)

# 2) S3에 train.py · 모델 · 데이터 올리기 (최초 1회, train.py를 고치면 다시)
BUCKET=$(terraform -chdir=../infra/envs/dev/20-data output -raw bucket_name)
python3 examples/fsdp/seed_assets.py --bucket "$BUCKET"

# 3) 백엔드 (127.0.0.1:8080)
S3_BUCKET=$BUCKET KUBECONFIG=~/.kube/devoops-mgmt.yaml KUBE_CONTEXT=k3d-devoops-mgmt \
  LISTEN_ADDR=127.0.0.1:8080 go run ./cmd/api

# 4) 프론트 (Frontend 저장소). Tailscale Serve가 tailnet의 5173을 127.0.0.1:5173으로 넘긴다
npm run dev -- --host 127.0.0.1 --port 5173
```

- 프론트: `http://<tailscale 이름>:5173` / Swagger: `http://<tailscale 이름>:5173/swagger/` (Vite 프록시 경유)
- Tailscale Serve가 없으면 `tailscale serve --bg --tcp 5173 tcp://127.0.0.1:5173`
- 끝나면 비용이 나가지 않도록 `(cd ../infra && ./run.sh destroy)`, `k3d cluster delete devoops-mgmt`

## 구현 내용

### 1. 작업 제출 `POST /api/jobs`

JSON: `model`, `gpus`, `dataset`(필수), `name`, `strategy`, `hyperparameters`(선택). 프론트는 입력이 없으면 `/api/models` · `/api/datasets`의 기본값으로 요청을 채운다.

방식은 `fsdp`(GPU 2개 이상)와 `ddp`, GPU는 `gpus.options`(1 · 2 · 4개)에서 고른다. `gpus.max`는 현재 할당량으로 쓸 수 있는 최대값(지금 2)이고, 할당량이 늘면 `default.json`에서 이 값만 올린다.

실행 기록은 `service.EventReader`로 읽는다. Kafka로 옮기면 이 인터페이스의 새 구현을 `cmd/api/main.go`에 넘긴다. 서비스와 API는 그대로다.

1. 검증한다. 실패하면 400 · 404 · 422이고 아무것도 쓰지 않는다.
2. 프로파일 기본값 + 사용자 값으로 `config.json`을 만들고 sha256을 계산한다.
3. `internal/service/jobspec.Render`로 JobSet을 만든다.
4. S3 `code/<id>/`에 `config.json`, `train.py`(공용 `code/_shared/train.py` 복사), `jobset.json`을 쓴다.
5. k3s에 제출한다. 실패하면 502 `SUBMIT_FAILED`, 이름이 겹치면 409.

모델 · 데이터 조회는 `GET /api/models`, `GET /api/datasets`, `GET /api/datasets/{id}`이고 파일 앞부분 미리보기가 따로 있다. 데이터셋은 S3의 `datasets/ds-*/v*/manifest.json`에서 읽는다.

### 전달 타임라인 `GET /api/jobs/{id}/events`

작업이 GPU까지 가는 동안 거친 단계를 시간순으로 돌려준다(DynamoDB `devoops-job-events`, PK `job_id`, SK `sk`).

| 쓰는 곳 | 단계 |
|---|---|
| 백엔드 `POST /api/jobs` | `backend/Submitted` (S3 업로드 후 k3s에 JobSet 생성) |
| 백엔드 tracker (k3s, 2초마다) | k3s Kueue 이벤트(쿼터 예약), Workload 상태의 MultiKueue AdmissionCheck(`Pending` → `Ready`, 어느 워커가 가져갔는지), `Finished` |
| 각 EKS의 job-watcher (infra `cluster-addons/job_watcher.py`) | 워커 Kueue(승인·쿼터 부족 대기), JobSet·job-controller, kube-scheduler, Karpenter(노드 지정, Spot 용량 부족, NodeClaim), kubelet(이미지 pull, 시작) |

- 항목: `job_id`, `sk`(`<시각 ms UTC>#<클러스터>#<ID>`), `time`, `last_time`, `cluster`, `source`, `reason`, `message`, `type`, `object`, `count`, `expires_at`(7일 TTL). 백엔드와 job-watcher가 같은 형식을 쓴다.
- 반복 이벤트(예: `FailedScheduling`)는 같은 `sk`를 덮어써서 `count`만 늘어난다.
- 타임라인 쓰기는 best-effort다. 실패해도 작업 제출은 성공한다.

### 2. 작업 조회 `GET /api/jobs`, `GET /api/jobs/{id}`

- **k3s:** `devoops/job-id` 라벨이 있는 JobSet과 그 Workload에서 `status`, `phase`, `cluster`, `statusMessage`를, JobSet 어노테이션에서 이름 · 모델 · 방식 · GPU 수 · 데이터셋을 읽는다(서비스 DB가 없는 임시 구성).
- **DynamoDB:** `step`, `totalSteps`, `loss`, `lastCheckpointStep`, `nodeName`. 조회가 실패해도 큐 상태는 그대로 반환한다(`progress: null`).
- **S3 (상세만):** 최종 `config`.
- 결과는 최신순이고, 목록은 `{items, nextCursor}`다.

### 3. 작업 파일 보기 `GET /api/jobs/{id}/files`, `GET /api/jobs/{id}/files/{kind}/{name}`

- 목록: `code/<id>/`(config.json · jobset.json · train.py)와 그 작업이 쓴 데이터셋의 `manifest.json`(`kind: data`).
- 내용: `text/plain`으로 앞 **64KiB**만 준다. 헤더 `X-Truncated`, `X-Total-Size`.
- 작업 ID · 파일명을 검사해서 다른 경로는 읽을 수 없다. 체크포인트는 보여 주지 않는다.

### 4. API 문서 (Swagger)

- `GET /swagger/`: Swagger UI. `GET /openapi.yaml`: OpenAPI 3.0 스펙
- 스펙은 `api/openapi.yaml`을 손으로 관리하고 `go:embed`(`api/spec.go`)로 바이너리에 넣는다. 코드 생성(swag 주석)은 쓰지 않는다.
- 라우트는 `internal/handler/router.go`의 `NewRouter` 한 곳에 있다. **API를 바꾸면 두 파일을 같이 고친다.** `TestOpenAPISpecMatchesRoutes`가 스펙의 경로·메서드와 Gin 라우트(`Routes()`)를 비교해서, 빠뜨리면 테스트가 실패한다.
- Swagger UI(swagger-ui-dist 5.34.0)는 브라우저가 jsDelivr에서 받는다. 같은 주소에서 열리므로 CORS 설정 없이 "Try it out"이 된다.
- CI에서 `redocly lint`로 스펙 문법을 검사한다.

```sh
open http://localhost:8080/swagger/
npx @redocly/cli@2.11.0 lint api/openapi.yaml
```

### 5. 기타

- `GET /healthz`
- CORS: `ALLOWED_ORIGIN` 한 곳만 허용

## 디렉터리

팀 공통 Go 레이아웃이다. 의존 방향은 `handler → service → repository`이고, 모두 `model`만 공유한다. 각 레이어는 아래 레이어를 인터페이스로 받아서 테스트에서 fake로 바꾼다.

```
cmd/api/main.go             진입점. config → repository → service → handler를 조립하고 서버를 띄운다
internal/
  config/                   환경변수 → Config (S3_BUCKET 필수, 나머지는 infra 출력과 같은 기본값)
  handler/                  Gin 라우터·핸들러. JSON 파싱, service 오류 → 400/404/409/422/502, Swagger UI·스펙 제공
  middleware/               CORS(ALLOWED_ORIGIN 한 곳), slog 요청 로그
  model/                    요청·응답 DTO(Job, Progress, JobEvent, File …)와 공유 상수(라벨, JobSet 이름)
  repository/               S3(입력 업로드·목록·앞부분 읽기), DynamoDB(진행 상태, 타임라인), k3s(JobSet 생성, JobSet + Workload → 상태)
  settings/                 backend-settings: 큐 · 이미지 · Pod 자원 · 모델 프로파일 (default.json, go:embed)
  service/                  작업 제출·조회(job.go), 모델·데이터셋 조회(catalog.go)
    jobspec/                JobSet 렌더링 (순수 로직), progress_reporter.py(진행 상태 sidecar 스크립트, go:embed)
    timeline/               k8s Event·Workload → 타임라인 항목, k3s를 2초마다 확인하는 tracker
api/                        OpenAPI(Swagger) 스펙 openapi.yaml (go:embed)
deployments/                Dockerfile
docs/                       입력 형식(input-format.md)
examples/fsdp/              train.py(FSDP · DDP 학습 코드), seed_assets.py(S3 준비)
examples/                   이전 업로드 방식의 예시 (job.yaml 등). 지금 API에서는 쓰지 않는다
```

`pkg/`(외부 공용 코드)는 아직 필요한 것이 없어서 두지 않았다.

## 이전 예시 작업 (`examples/`)

> `POST /api/jobs`가 JSON으로 바뀌어 아래 예시는 더 이상 제출할 수 없다. 기록으로 남겨 둔다.

- `job.yaml`: DDP. Pod 2개 × GPU 1개(GPU 노드 2대, standby 없음). Indexed Job 하나에 `parallelism: 2`라서 Kueue가 GPU 2개를 한 번에 승인한다.
  - 각 Pod가 `torchrun --nnodes=2 --nproc-per-node=1 --node-rank=$JOB_COMPLETION_INDEX`로 `train.py`를 실행한다.
  - rank 0 주소는 JobSet headless Service의 DNS `<jobset>-trainer-0-0.<jobset>`이다. JobSet 이름은 Downward API(`JOBSET_NAME`)로 받는다. rank 1은 이 DNS가 생길 때까지 기다린다.
  - g4dn.xlarge는 EFA가 없어서 NCCL은 Pod 네트워크(`eth0`) TCP로 통신한다.
  - Pod 하나라도 실패하면 JobSet(`maxRestarts`)이 두 Pod를 같이 다시 띄운다.
- `train.py`가 지키는 규칙:
  - S3 코드가 없다. `/code`, `/data`, `/ckpt` 경로만 쓴다. 두 Pod가 같은 `checkpoints/<id>/`를 본다.
  - **모델은 S3에 하나만 저장한다.** DDP는 모든 rank의 가중치가 같으므로 rank 0만 1000 step마다 `ckpt-<step>.pt`를 쓰고, 끝나면 가중치만 `model.pt`로 쓴다.
  - 체크포인트는 **메모리에 직렬화한 뒤 새 파일에 한 번에** 쓴다. Mountpoint는 rename과 append를 지원하지 않기 때문이다.
  - 최근 체크포인트 2개만 남긴다.
  - 시작할 때 rank 0이 `/ckpt`의 최신 step을 골라 모든 rank에 broadcast하고, 모든 rank가 그 파일에서 가중치·optimizer 상태를 읽어 재개한다.
  - rank 0이 매 step `$PROGRESS_FILE`에 현재 상태(JSON: `status`, `step`, `total_steps`, `loss`, `last_checkpoint_step`)를 덮어쓴다. 임시 파일 후 rename으로 바꾼다(emptyDir라 가능).
  - DynamoDB에는 sidecar가 쓴다. 그래서 `train.py`에 boto3가 없고, DynamoDB 지연이나 오류가 학습 step을 막지 않는다.
  - `torchrun` 없이 `python train.py`로 실행하면 프로세스 1개로 돈다. 로컬 CPU에서는 `torchrun --nproc-per-node=2`(gloo)로 확인할 수 있다.

### 학습 데이터 (`make_data.py` → `train.csv`)

지금까지 제출한 예시 작업이 쓴 데이터다. 실제 데이터가 아니라 **합성 선형 회귀 데이터**이고, 인프라(배치·복구·체크포인트·진행 상태)를 확인하는 용도다.

| 항목 | 값 |
|---|---|
| 형식 | CSV, 헤더 없음, 5000행 × 9열, 약 380KB |
| 입력 (앞 8열) | x1..x8, 각각 표준정규분포 N(0, 1)에서 뽑은 값 (소수 5자리) |
| 레이블 (마지막 열) | y = w·x + ε. w는 seed 0으로 고정한 가중치 8개 `[0.942, -1.397, -0.680, 0.371, -1.016, -0.072, 0.179, -0.831]`, 노이즈 ε ~ N(0, 0.1²) |
| 모델 | `train.py`의 MLP(8 → 256 → ReLU → 1), MSE loss, Adam(lr 1e-3), rank당 batch 256 (DDP 2 rank면 실효 512) |
| 기대 결과 | 노이즈 분산이 0.01이라 loss가 약 0.01 근처까지 내려간다. 2026-10-27 실행에서 5000 step 후 loss는 0.0069였다 |

```sh
python examples/make_data.py train.csv   # 표준 라이브러리만 사용. 같은 seed라 항상 같은 파일이 나온다
```

`train.py`는 `$DATA_DIR`의 `*.csv`를 모두 읽어 마지막 열을 레이블로 쓴다. CSV가 없으면 32차원 합성 데이터를 직접 만든다. 따라서 다른 데이터를 쓰려면 같은 형식(숫자 열, 마지막 열이 레이블)의 CSV를 올리면 된다.

## 설정 (환경변수)

| 변수 | 기본값 | 설명 |
|---|---|---|
| `S3_BUCKET` | (필수) | `infra` 20-data의 `bucket_name` |
| `PROGRESS_TABLE` | `devoops-job-progress` | 20-data의 `progress_table_name` |
| `EVENTS_TABLE` | `devoops-job-events` | 20-data의 `events_table_name` (작업별 전달 타임라인) |
| `AWS_REGION` | `us-east-2` | |
| `KUBECONFIG` / `KUBE_CONTEXT` | 기본 kubeconfig / 현재 context | 로컬 k3s |
| `TRAINING_NAMESPACE` | `training` | 50-management 출력과 같아야 한다 |
| `BACKEND_SETTINGS` | 내장 `default.json` | 큐 · 이미지 · 모델 프로파일 파일 경로 |
| `ALLOWED_ORIGIN` | `http://localhost:5173` | 프론트 주소 |
| `LISTEN_ADDR` | `:8080` | |

AWS 자격증명은 SDK 기본 체인을 쓴다. 백엔드 전용 Role은 `infra` 20-data의 `backend_role_arn`이다. 권한은 `code/`·`datasets/` 쓰기, 버킷 읽기, 진행 상태 읽기만 있다.

## 테스트

TDD로 작성한다. 레이어마다 바로 아래 레이어를 fake로 바꿔서 테스트한다. S3·DynamoDB는 SDK 인터페이스 fake, k8s는 `client-go/dynamic/fake`를 쓴다.

```sh
golangci-lint run ./...   # v2, 설정은 .golangci.yml
go test -race ./...
```

| 패키지 | 무엇을 fake로 | 검증 |
|---|---|---|
| `handler` | service | multipart → 입력 전달, service 오류 → 400·404·502·500(메시지 그대로), JSON 모양(`progress: null`, 빈 배열 `[]`), 파일 내용 헤더, 인코딩된 `/` 차단, OpenAPI 스펙 ↔ Gin 라우트 일치, `/openapi.yaml`·`/swagger/` |
| `middleware` | - | CORS preflight 204, 노출 헤더, 다른 origin 거부 |
| `service` | repository | 업로드 키(job.yaml 원본 포함), 입력 검증이 업로드보다 먼저, 경로 탈출 차단, 업로드 실패 시 미제출, 진행 상태 병합·최신순, DynamoDB 실패 시 동작, 타임라인 best-effort, 파일 64KiB 자르기·다른 작업 파일 차단 |
| `service/jobspec` | - | 주입하는 각 값, 사용자 값 유지, 잘못된 입력 거부, `examples/job.yaml`이 통과하는지 |
| `service/timeline` | k3s·DynamoDB | Event(core/v1 구형·신형 필드)·Workload(MultiKueue AdmissionCheck, Finished) → 항목, 정렬 키, 변하지 않은 항목은 다시 쓰지 않기, 실패한 쓰기 재시도 |
| `repository` | SDK·k8s client | Workload·JobSet 상태 → phase·cluster·restarts·message, 백엔드가 만들지 않은 JobSet 제외, 이벤트 항목 형식(job-watcher와 공유)·TTL, Query·S3 목록 페이지 넘김, 100개 단위 분할, 앞부분만 읽기, 없는 키 → not found |
| `config` | 환경변수 | 기본값, 모든 변수, `S3_BUCKET` 필수 |

## CI (`.github/workflows/ci.yml`)

협업 규칙 §7을 따른다. CD(ArgoCD)는 아직 정하지 않았다.

| 시점 | 실행 | 이미지 |
|---|---|---|
| `dev`·`main`행 PR, `dev` push | `golangci-lint` → `redocly lint`(OpenAPI) → `go test -race ./...` → `go build` → 이미지 빌드 | push 안 함 |
| `main` push(머지) | 같은 검사 → 이미지 빌드 | `ghcr.io/devoops-team/backend:main`, `:sha-<커밋 7자리>` (linux/amd64) |

GHCR 로그인은 기본 `GITHUB_TOKEN`을 쓴다. `main`행 PR 리뷰어는 `.github/CODEOWNERS`(정·부담당)다.

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
docker build -f deployments/Dockerfile -t devoops-backend:dev .

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
