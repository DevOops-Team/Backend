# Backend

사용자가 화면에서 고른 **Hugging Face 모델 폴더 · jsonl 학습 데이터 · 학습 코드(train.py) · 설정**을 받아 학습 작업을 제출하고, 진행 상태를 돌려주는 Go(Gin) 서버입니다.
API는 Confluence `서비스 API 명세`(이 레포 `api/openapi.yaml`), JobSet은 `JobSet 템플릿 (합의안)`(ADR-12)을 따릅니다. 설계: `docs/superpowers/specs/2026-11-11-file-upload-submit-design.md`.

```
Frontend ──multipart(spec + model/* + data/* + train/*.py)──▶ Backend (온프레 platform)
   ① S3 datasets/<id>/v1/{model,data,code}/ 스트리밍 + manifest.json
   ② 체크포인트 간격 계산 (Young/Daly τ = √(2·C·M)) → code/<id>/train.py(사본) · config.json · jobset.json
   ③ DynamoDB 펜싱 첫 항목 (generation 1)
   ④ 관리 클러스터에 JobSet 제출 → Kueue → dispatcher → MultiKueue → EKS
   ⑤ Kafka job.lifecycle ← job.submitted
EKS 학습 Pod: ckpt-init(데이터 · 모델 · train.py 받기) → trainer(torchrun /data/code/train.py) + ckpt-sidecar(체크포인트 승인 · 진행률 → Kafka)
Frontend ◀── 폴링 ── Backend ◀── Kafka job.lifecycle 구독 (상태 · 진행률 · 체크포인트) + 관리 클러스터 JobSet(이름 · 설정)
```

## API

| 메서드 · 경로 | 내용 |
|---|---|
| `GET /api/training/options` | 방식 · GPU 범위 · 하이퍼파라미터 기본값과 범위 · 업로드 상한 · 체크포인트 계산 기본값 |
| `POST /api/jobs` | multipart. 첫 파트 `spec`(JSON), 이어서 `model/<폴더>/<경로>` 파일들(`config.json` 필수), `data/<파일>.jsonl` 1개 이상, `train/<파일>.py` 정확히 1개(최대 1MiB, `/data/code/train.py`로 주입). 경로는 파트 **이름**에 넣는다 |
| `GET /api/jobs` · `/api/jobs/{id}` | 관리 클러스터 JobSet + Kafka `job.lifecycle`로 만든 상태 · 진행률 · 마지막 체크포인트 |
| `GET /api/jobs/{id}/events` | 타임라인: Kafka 사건(제출 · 입장 · 배치 · 시작 · 체크포인트 · 종료) + 관리 클러스터 Event (메모리) |
| `GET /api/jobs/{id}/files` · `/files/{kind}/{name}` | `code/<id>/`의 train.py · config.json · jobset.json, 업로드 manifest.json 앞부분 |

오류는 `{"error","code","fields"}`: 400 입력 · 409 중복 · 413 업로드 상한 초과 · 502 S3 · 관리 클러스터 · DynamoDB(펜싱).

## 체크포인트 간격

- Backend: `C = 모델 폴더 바이트 × size_factor ÷ write_bytes_per_second`, `τ = max(min_interval_seconds, √(2·C·mtbf_seconds))` → config.json `checkpoint_policy`
- train.py: 처음 `warmup_measure_steps` step의 시간으로 예상 학습 시간 T를 구해 저장 횟수 n = ⌊T/τ⌋, step 간격 ⌈전체 step/n⌉ (마지막 step은 항상 저장)
- 기본값은 `internal/settings/default.json` `checkpoint`(M = 6시간). 검증 때 짧게 보려면 `BACKEND_SETTINGS`로 M · 하한을 낮춘다

## 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `S3_BUCKET` | (필수) | 체크포인트 버킷 하나에 code · datasets · jobs (ADR-14) |
| `FENCING_TABLE` | `devoops-prod-fencing` | 세대 · owner · 승인본 (ADR-6) |
| `KAFKA_BOOTSTRAP` | 비어 있음 | `job.lifecycle` 브로커. Backend가 job.submitted를 보내고 토픽 전체를 읽어 화면 상태를 만든다. 비우면 둘 다 안 함(상태는 관리 클러스터만). JobSet의 ckpt-sidecar에도 넘김 |
| `KAFKA_TOPIC_LIFECYCLE` | `job.lifecycle` | |
| `TRAINER_IMAGE` | (필수) | `ghcr.io/devoops-team/trainer@sha256:…` (Data 레포 `trainer/`, 봇이 Gitops에 digest) |
| `CHECKPOINT_IMAGE` | (필수) | `ghcr.io/devoops-team/checkpoint@sha256:…` (Data `checkpoint/`) |
| `TRAINING_NAMESPACE` | `training` | |
| `AWS_REGION` | `us-east-2` | AWS 자격은 SDK 기본 체인 |
| `KUBECONFIG` · `KUBE_CONTEXT` | 클러스터 안 | |
| `BACKEND_SETTINGS` | 내장 `default.json` | 큐 · 이미지 · Pod 자원 · 학습 옵션 · 체크포인트 · 업로드 상한 |
| `ALLOWED_ORIGIN` · `LISTEN_ADDR` | `http://localhost:5173` · `:8080` | |

## 구조

```
cmd/api/                    배선 (config → repository → service → handler)
internal/handler/           Gin, multipart 스트리밍 → service.UploadSource
internal/service/           제출 · 조회 로직
internal/service/ckptpolicy Young/Daly 간격
internal/service/jobspec    합의안 JobSet 렌더링 (ckpt-init · ckpt-sidecar · trainer)
internal/service/jobview    Kafka job.lifecycle 사건 → 잡별 상태 · 진행률 · 타임라인 (GPU-146)
internal/service/timeline   Kubernetes Event · Workload → 타임라인
internal/repository/        S3 · 관리 클러스터 · DynamoDB(펜싱) · Kafka(발행 · 구독) · 메모리 타임라인
internal/settings/          default.json
api/openapi.yaml            API 명세 (테스트가 Gin 라우트와 비교)
```

## 테스트 · CI

```sh
go test -race ./...                         # 순수 로직 (설정 · 간격 계산 · JobSet 렌더링 · 봉투 · 라우트 ↔ 명세)
```

외부 시스템(S3 · DynamoDB · 관리 클러스터 · Kafka)을 가짜로 바꾸는 테스트는 두지 않는다. 동작은 실제 인프라에 배포해 확인한다.

CI(`.github/workflows/ci.yml`): PR · develop push는 lint · 테스트 · 이미지 빌드(push 안 함). main push는 `ghcr.io/devoops-team/backend`를 push하고, 봇이 Gitops `onprem/platform/backend/overlays/onprem`의 digest를 갱신한다 (ADR-23).
