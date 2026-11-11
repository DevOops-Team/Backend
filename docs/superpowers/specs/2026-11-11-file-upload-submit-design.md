# 파일 업로드 제출 · 동적 체크포인트 간격 · Kafka 제출 사건

- 작성: 노찬경 (설계 대화 정리), 2026-11-11
- 지라: GPU-127 하위 (GPU-132 범위 확장)
- 관련 레포: Backend(이 문서 · train.py · trainer 이미지) · Frontend · Gitops · Data(ckpt-agent, 읽기만)
- 근거: ADR-12(JobSet 공용 템플릿) · ADR-14(S3 하나) · ADR-28(스팟 회수 120초) · ADR-30(최종 체크포인트만 남김) · Confluence 「JobSet 템플릿 (합의안)」 v6 · 「체크포인트 규약」 · IaC `terraform/TRAINING-IAM.md` · Data `checkpoint/README.md` · `collector/README.md`

## 1. 왜 하는가

지금 Backend의 제출은 목업이다. S3에 미리 올려 둔 공용 모델(`datasets/_shared/models/gpt2`) · 데이터셋 카탈로그(`datasets/<id>/v<n>/`) · 학습 코드(`code/_shared/train.py`)를 고르는 구조인데, 실제 S3 버킷은 비어 있고 앞으로도 미리 채우지 않는다. 실제 사용 방식은 **사용자가 자기 로컬의 Hugging Face 모델 폴더와 jsonl 학습 데이터를 화면에서 골라 설정과 함께 제출**하는 것이다.

또 체크포인트 간격이 고정값(`checkpoint_every_steps`, 기본 250 step)이라 학습 길이와 상관없이 같다. 저장에 드는 시간과 장애 때 잃는 시간의 합이 최소가 되도록 **간격을 계산**해야 한다.

제출 기록은 존재하지 않는 DynamoDB events 테이블에 쓰고 있다. 팀의 사건 수집 경로는 Kafka `job.lifecycle` → 수집기 → PostgreSQL이므로 **Backend가 `job.submitted`를 Kafka로 보낸다**.

## 2. 목표 · 비목표 · 안티목표

### 목표
1. 화면에서 HF 모델 폴더(여러 파일) + jsonl 데이터 파일 1개 이상 + 설정을 골라 제출한다.
2. Backend가 파일을 스트리밍으로 받아 S3에 저장하고, config.json · jobset.json을 만들어 관리 클러스터에 JobSet을 제출한다.
3. 체크포인트 간격을 Young/Daly 공식으로 계산해 config.json에 넣고, train.py가 그 간격으로 저장한다.
4. JobSet은 「JobSet 템플릿 (합의안)」 구조를 따른다 (ckpt-init · ckpt-sidecar · trainer, train.py는 trainer 이미지 안).
5. 제출 때 Kafka `job.lifecycle`에 `job.submitted`를 보낸다.

### 완료 기준 (사용자 확정)
화면에서 GPT-2 HF 폴더와 jsonl을 올려 제출 → S3 저장 · JobSet 생성 → eks-a GPU에서 학습(화면에 진행률 표시) → 계산된 간격마다 체크포인트 승인 → `job.submitted` · `checkpoint.approved`가 Kafka → 온프레 수집기를 거쳐 PostgreSQL(`data.job_runs` · `data.checkpoints`)에 쌓인 것을 확인.

검증 때만 Gitops에서 `mtbf_seconds` · `min_checkpoint_interval_seconds`를 낮춰(예: 600초 · 60초) 15~20분 학습에서 중간 저장이 여러 번 생기는지 확인하고, 끝나면 기본값으로 되돌린다 (사용자 확정).

### 비목표
- 장애 복구 · 재개(세대 2 이상) 경로 검증. ckpt-init의 재개 기능은 템플릿에 들어가지만 이번 완료 기준에 넣지 않는다.
- 업로드한 모델 · 데이터의 재사용(카탈로그, 다른 잡에서 다시 고르기).
- 장애 간격(MTBF) 측정 · 자동 갱신.

### 안티목표 (기술적으로 돌아가도 실패)
- Backend가 업로드 파일 전체를 메모리나 디스크에 모았다가 올리는 것 (2GB 업로드에 Pod가 죽음).
- 업로드 도중 실패했는데 반쯤 올라간 파일로 JobSet이 제출되는 것.
- 정적 AWS 키가 임시가 아니라 굳어지는 것 (6.5).
- JobSet 템플릿을 잡마다 다르게 만드는 것 (ADR-12: 공용 함수 하나, 제출본이 복구 원본).

## 3. 사용자가 정한 것

| 항목 | 결정 |
|---|---|
| 업로드 대상 | HF 폴더 형식의 학습 모델 + jsonl 학습 데이터(수백 MB). 어떤 HF 모델이든 받는다 (사용자가 직접 지정해 쓰므로 검증 최소) |
| train.py | Backend가 갖는다. 사용자가 올리지 않는다 |
| JobSet 템플릿 | ADR-12 · 합의안대로 고정 |
| 체크포인트 간격 기준 | 저장 시간 + 장애 때 잃는 시간의 합 최소 → Young/Daly |
| 장애 간격 M | 모름. Backend 설정의 고정 기본값으로 시작 |
| 제출 사건 | Kafka `job.lifecycle`에 `job.submitted` (DynamoDB events 대체) |
| AWS 자격 (온프레 Backend) | 이번 검증은 임시 정적 IAM 키 (6.5) |
| progress-reporter | 넣는다. 진행률 테이블을 지금 만든다 (6.4) |
| 수집기 | 온프레에 배포한다 (6.8) |
| 검증용 간격 | 검증 때만 M · 하한을 낮춤 (2절) |

## 4. 흐름

```
[Frontend] 모델 폴더(webkitdirectory) + jsonl 파일 + 설정 선택
   └─ POST /api/jobs  (multipart/form-data, 스트리밍)
[Frontend nginx] /api/ → Backend (body 상한 · 버퍼링 끔 · 시간 제한 늘림)
[Backend]
   1. 설정 파트 검증 (첫 파트)                         → 실패 시 400, S3에 아무것도 안 씀
   2. id 발급, 파일 파트를 하나씩 S3로 스트리밍 업로드 (sha256 · 크기를 흘려 보내며 계산)
        datasets/<id>/v1/model/<상대경로>
        datasets/<id>/v1/data/<파일명>
   3. datasets/<id>/v1/manifest.json 작성 (파일 목록 · sha256 · 크기)
   4. 체크포인트 정책 계산 → config.json → sha256
   5. jobset.json 렌더링 (합의안 템플릿, 공용 함수)
   6. S3 code/<id>/config.json · jobset.json 저장
   7. DynamoDB 펜싱 첫 항목 PutItem (attribute_not_exists(job_id)), generation 1
   8. 관리 클러스터에 JobSet 생성 (training ns, queue-light/heavy)
   9. Kafka job.lifecycle ← job.submitted (최선 노력, 실패해도 제출은 성공)
  10. 201 + 작업 정보
   실패 시: 2~8 중 실패하면 이미 올린 datasets/<id>/ · code/<id>/는 남겨 두고(삭제 권한 없음) 500. JobSet은 만들지 않는다.
[EKS 학습 Pod] ckpt-init: 세대 · owner 확인, config.json · 데이터셋 manifest의 파일 전부(모델 포함) 다운로드 · 체크섬
   trainer(torchrun /opt/train/train.py): /data/model 읽기, /data/data/*.jsonl 학습, 간격마다 /ckpt/step-N + .done
   ckpt-sidecar: 업로드 · manifest · 조건부 승인 · Kafka checkpoint.approved
```

모델을 데이터셋 manifest 안(`model/` 하위)에 함께 넣는 이유: ckpt-init은 `DATASET_URI`의 manifest에 있는 파일만 받는다. 모델용 경로를 따로 두면 Data 레포 ckpt-init을 고쳐야 한다. 같은 manifest에 넣으면 Data 쪽 변경이 없다.

## 5. 체크포인트 간격 (Young/Daly)

- 최적 간격 τ = √(2 · C · M)
  - C: 체크포인트 한 번 저장 시간(초) 추정 = 체크포인트 크기 ÷ 쓰기 속도
    - 체크포인트 크기 = 모델 가중치 바이트 × `checkpoint_size_factor`(설정, 기본 3: fp32 가중치 + Adam 모멘트 2개)
    - 쓰기 속도 = `checkpoint_write_bytes_per_second`(설정, 기본 100 MB/s, 로컬 NVMe에 DCP 저장 기준. S3 업로드는 사이드카가 학습과 병렬로 함)
  - M: `mtbf_seconds`(설정, 기본 6시간 = 21600)
- 하한 `min_checkpoint_interval_seconds`(설정, 기본 300). τ가 이보다 작으면 하한을 쓴다.
- Backend는 **시간 간격 τ**를 config.json에 넣는다. 저장 횟수(step 간격)는 학습 시간을 알아야 정해지는데, step 시간은 GPU · 모델에 따라 달라 제출 시점에 모른다. 그래서 train.py가 정한다:
  - 처음 `warmup_measure_steps`(기본 20) step의 평균 step 시간 t를 잰다 (rank 0 측정 → all_reduce max로 모든 rank 같은 값)
  - 전체 step 수 S (epoch × step/epoch 또는 max_steps), 예상 학습 시간 T = S · t
  - 저장 횟수 n = max(1, floor(T / τ)), step 간격 k = ceil(S / n)
  - k step마다 저장하고, 마지막 step에는 항상 저장 (ADR-30: 끝나면 최종 체크포인트만 남김은 사이드카 · 보존 정책 쪽, 이번 범위 밖)
  - 결정한 k · n · t · T를 로그와 progress 파일에 남긴다
- config.json 변경: `checkpoint_every_steps` 제거, 아래 추가 (train.py는 모르는 키를 거부하므로 둘 다 같이 바꾼다)
  ```json
  "checkpoint_policy": {
    "interval_seconds": 1530,
    "estimated_save_seconds": 54.2,
    "mtbf_seconds": 21600,
    "warmup_measure_steps": 20
  }
  ```
- 사용자 화면에는 계산된 τ와 예상 저장 시간을 보여 주기만 하고 입력받지 않는다.

## 6. 구성요소별 변경

### 6.1 Frontend
- 학습 생성 화면(`features/training/components/CreatePipeline.tsx`): 공용 모델 · 데이터셋 목록 선택을 없애고
  - 모델 폴더 선택 (`<input type=file webkitdirectory>`), 파일 수 · 총 크기 표시, `config.json` 없으면 제출 막음
  - 학습 데이터 선택 (`.jsonl` 여러 개 허용)
  - 설정: 이름 · GPU 수(1 · 2 · 4) · 전략(fsdp · ddp) · epochs · max_steps · batch_size_per_gpu · learning_rate · warmup_steps · seed. 기본값 · 범위는 지금 GPT-2 프로필 값
  - 업로드 진행률 (XMLHttpRequest upload progress), 업로드 중 취소
- 요청 형식: `multipart/form-data`, **첫 파트 `spec`**(JSON 설정), 이후 `model` 파트들(파일명 = 폴더 기준 상대경로), `data` 파트들
- `nginx.conf.template` `/api/`:
  - `client_max_body_size` 64m → 3g
  - `proxy_request_buffering off` (nginx 디스크에 모으지 않고 바로 Backend로)
  - `proxy_read_timeout` · `proxy_send_timeout` 1h
- 진행 화면(`RunOverview`)은 지금 API를 그대로 쓴다.

### 6.2 Backend API
- `POST /api/jobs`: JSON → multipart 스트리밍. `spec` 파트가 첫 번째가 아니면 400. 상한: 모델 합계 `max_model_bytes`(기본 2 GiB), 데이터 합계 `max_data_bytes`(기본 1 GiB), 파일 수 `max_files`(기본 200). 경로에 `..` · 절대경로 · 빈 이름이 있으면 400
- S3 업로드는 기존 `manager.Uploader`(멀티파트) 사용, 파트별 io.TeeReader로 sha256 계산
- 없애는 것: `/api/models` · `/api/models/:name/files/:file` · `/api/datasets` · `/api/datasets/:id` · `/api/datasets/:id/versions/:version/files/:name`, settings의 `model_profiles` · `train_script_key`. OpenAPI 스펙 · 라우트 테스트도 함께
- 남기는 것: `/api/jobs` GET 목록 · 상세 · events · files
- settings(`default.json`)에 추가: `checkpoint` 블록(5절 값들), `limits` 블록(6.2 상한), 하이퍼파라미터 기본값 · 범위(지금 gpt2 프로필 값을 일반 설정으로)

### 6.3 JobSet 템플릿 (`internal/service/jobspec`)
「JobSet 템플릿 (합의안)」 4절을 그대로 렌더링한다. 지금 임시 구성(Mountpoint S3 PVC · transformers 런타임 설치 · S3의 train.py)은 없앤다.
- initContainers: `ckpt-init`(image `CHECKPOINT_IMAGE` = Data 레포 `checkpoint/Dockerfile`로 만든 `ghcr.io/devoops-team/checkpoint`, 이미 main에 있음 `sha256:e05195df…`, command `python -m ckpt.init_main`), `ckpt-sidecar`(같은 이미지, 기본 CMD `python -m ckpt.sidecar`, restartPolicy Always)
- Pod `securityContext`: runAsUser · runAsGroup · fsGroup 1000. ckpt 이미지가 UID 1000으로 돌고 trainer와 `/ckpt`를 함께 쓴다 (Data Dockerfile 주석)
- containers: `trainer`(image `TRAINER_IMAGE`, `/opt/train/train.py`)
- serviceAccountName `trainer` (EKS Pod Identity → trainer role, IaC TRAINING-IAM.md)
- env 실제 값: `CKPT_BUCKET=devoops-prod-ckpt-354363695096-us-east-2`, `FENCING_TABLE=devoops-prod-fencing`, `CONFIG_URI=s3://<버킷>/code/<id>/config.json`, `DATASET_URI=s3://<버킷>/datasets/<id>/v1/`, `KAFKA_BOOTSTRAP=172.30.0.40:9092,172.30.0.41:9092,172.30.0.42:9092`, `KAFKA_TOPIC_LIFECYCLE=job.lifecycle`, `CLUSTER_NAME` ← ConfigMap `devoops-cluster-info`
- 이 값들은 Backend env(Gitops `backend-config`)에서 받는다
- 자원: g4dn.xlarge(4 vCPU)에 trainer cpu 3 + 사이드카 0.25가 들어가는지 eks-a에서 확인 (합의안 8절). 안 들어가면 trainer cpu를 줄이고 문서에 남긴다

### 6.4 progress-reporter · 진행률 테이블
합의안대로 JobSet에 넣는다. 지금 Backend의 `internal/service/jobspec/progress_reporter.py`(네이티브 사이드카, `PROGRESS_FILE` → DynamoDB 덮어쓰기)를 그대로 쓴다.
- 테이블: `devoops-prod-job-progress` (기존 `devoops-prod-fencing`과 같은 이름 규칙), 온디맨드, 파티션 키 `job_id`(S). 항목 속성은 지금 Backend `repository/progress.go`가 읽는 것 그대로 (`generation` 포함 — IaC TRAINING-IAM.md 미결 사항 "GENERATION 포함" 충족)
- 만드는 곳: IaC Terraform `environments/prod/data/dynamodb.tf` (IaC 레포 GPU-n 브랜치 → develop → main, 노찬경이 SSO로 plan 확인 후 apply)
- 권한 (같은 IaC 변경):
  - EKS trainer role (`modules/eks` 학습 Pod 권한): `dynamodb:PutItem` on 진행률 테이블
  - Backend 임시 사용자 (6.5): `dynamodb:BatchGetItem` · `GetItem` on 진행률 테이블
- Backend env `PROGRESS_TABLE=devoops-prod-job-progress` (Gitops `backend-config`)
- 옛 세대가 늦게 덮어쓰는 문제는 이번에 막지 않는다 (복구 경로가 범위 밖)

### 6.5 Backend AWS 자격 (임시)
- IAM 사용자 `devoops-prod-backend-temp` + 액세스 키, Secret `platform/backend-aws`(Git에 넣지 않음)
- 인라인 정책 (IaC의 onprem-backend 역할 범위 + 업로드에 필요한 것):
  - s3:PutObject · AbortMultipartUpload · ListMultipartUploadParts on `code/*`, `datasets/*`
  - s3:GetObject on `code/*` (잡 파일 보기 API)
  - s3:ListBucket (prefix `code/`, `datasets/`)
  - dynamodb:PutItem · GetItem on `devoops-prod-fencing`
  - dynamodb:BatchGetItem · GetItem on `devoops-prod-job-progress`
- 태그 `devoops:temporary=true`, `devoops:replace-with=rolesanywhere`. Roles Anywhere 전환은 별도 티켓으로 박기현과
- (내 결정) 노찬경이 SSO(`--profile devoops`)로 aws CLI로 직접 만든다. IaC 문서(정적 키 없음)와 다르므로 박기현에게 Slack 초안으로 알리고, 전환 티켓을 만든다. Terraform에는 넣지 않는다 (임시라서)

### 6.6 Kafka `job.submitted`
- 토픽 `job.lifecycle`, 메시지 키 `job_id`
- 봉투 (Data `collector/envelope.py`): `event_id`=`submitted-<id>-g0001`, `schema_version`=1, `type`=`job.submitted`, `occurred_at`(UTC, `Z`), `source`=`backend`, `cluster`=`manager`, `job_id`, `generation`=1
- payload: `jobset_name` · `gpus` · `strategy` · `queue` · `config_sha256` · `image_digest`(trainer 이미지 digest)
- 프로듀서: acks=all, 멱등, 짧은 시간 제한(10초). 실패하면 경고 로그만 (제출은 성공)
- Go 클라이언트는 빌더가 고른다 (franz-go 권장: 순수 Go, distroless static 이미지 유지)
- 기존 DynamoDB events 쓰기 · 읽기는 제거. `/api/jobs/:id/events`는 Kubernetes 이벤트(timeline)만

### 6.7 train.py · trainer 이미지 (Backend 레포 `trainer/`)
- `examples/fsdp/train.py`를 `trainer/train.py`로 옮기고 이미지에 넣는다 (`/opt/train/train.py`)
- 이미지: `trainer/Dockerfile`(Backend 레포, 새로 만듦). PyTorch CUDA 런타임 + transformers 고정 버전, UID 1000 비루트 (ckpt 이미지와 같은 UID). Backend CI가 main에서 `ghcr.io/devoops-team/trainer` push → 봇이 Gitops backend overlay `images:` digest 갱신 → Backend env `TRAINER_IMAGE`(지금 `CHECKPOINT_IMAGE`와 같은 방식, `image-paths.yaml`)
- 일반 HF 모델: `AutoModelForCausalLM` · `AutoTokenizer`를 `/data/model`에서. 블록 길이 = min(설정 상한, 모델 `max_position_embeddings`), FSDP wrap 단위는 모델의 `_no_split_modules`로 자동
- trainer ↔ ckpt-sidecar 약속(합의안 7절)을 지킨다: `/ckpt/step-{8}/` 후 `.done`, 마지막 저장 후 `.approved`를 최대 120초 기다림, step마다 `/ckpt/STOP` 확인 → 종료 코드 75 (torchrun이 75를 1로 바꾸므로 진입 스크립트에서 STOP 있으면 `exit 75`)
- Mountpoint 우회(로컬에 쓴 뒤 복사)는 없앤다 (`/ckpt`가 emptyDir)

### 6.8 Gitops
- `onprem/platform/backend`: `backend-config`에 `S3_BUCKET` 실제 값, `FENCING_TABLE`, `PROGRESS_TABLE=devoops-prod-job-progress`, `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC_LIFECYCLE`; `EVENTS_TABLE` 제거. overlay `images:`에 trainer 추가, Deployment env `TRAINER_IMAGE`
- Backend Deployment 메모리 limit 확인 (스트리밍이라 늘릴 필요 없을 것으로 봄)
- `eks/scheduling`(GPU-133): `training` ns · `trainer` SA · ConfigMap `devoops-cluster-info`(CLUSTER_NAME=eks-a 등) 추가
- (내 결정) GHCR 패키지가 private이므로 `trainer` SA에 `imagePullSecrets: [ghcr-pull]`를 둔다 (템플릿은 고정, SA는 Gitops). `training/ghcr-pull` Secret은 GPU-133에서 IaC 방식(seed-secrets)과 같은 형식으로 eks-a에 넣는다
- 수집기(Data `collector/Dockerfile` → `ghcr.io/devoops-team/collector`, main `sha256:31562ce1…`)가 온프레 `data-system`에 아직 없다. Gitops `onprem/data/collector/{base,overlays/onprem}`(Deployment · `ghcr-pull` · Secret `collector-pg` 참조) + Application을 추가해 온프레에 배포한다 (사용자 확정). 환경 변수는 Data `collector/README.md` 표대로. PostgreSQL 16(Data README: 설치 · 스키마 완료) 위치와 `data_collector` 비밀번호는 구현 때 온프레에서 찾고, 못 찾으면 김현수에게 Slack 초안으로 요청. Secret은 Git에 넣지 않는다

## 7. 검증 방식 (내 결정)
- 화면 GPU 선택지는 1 · 2 · 4 그대로 둔다 (full 구성 기준). 지금 eks-a는 GPU 2장이라 완료 검증은 **2 GPU FSDP**(Pod 2개, 노드당 1)로 한다. 4를 고르면 대기 상태로 남는 것은 정상

## 8. 빌더에게 맡기는 것
- Go Kafka 클라이언트 선택 (distroless static 유지가 조건)
- Frontend 화면 배치 · 문구 · 컴포넌트 구조
- multipart 파싱 세부, 에러 메시지 문구, 테스트 구성
- trainer 이미지의 정확한 베이스 태그 (torch 2.5.1 CUDA 12.4 계열 유지 권장)

## 9. 열린 질문
없음.
