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
Kafka job.lifecycle ──▶ collector(Data) ──▶ PostgreSQL gpuf_data (job_runs · job_progress · checkpoints)
Frontend ◀── 폴링 ── Backend ◀── 데이터 DB v_job_status (backend_ro, 상태 · 진행률 · 체크포인트) + 관리 클러스터 JobSet(이름 · 설정)
```

## API

| 메서드 · 경로 | 내용 |
|---|---|
| `GET /api/training/options` | 방식 · GPU 범위 · 하이퍼파라미터 기본값과 범위 · 업로드 상한 · 체크포인트 계산 기본값 |
| `POST /api/jobs` | multipart. 첫 파트 `spec`(JSON), 이어서 `model/<폴더>/<경로>` 파일들(`config.json` 필수), `data/<파일>.jsonl` 1개 이상, `train/<파일>.py` 정확히 1개(최대 1MiB, `/data/code/train.py`로 주입). 경로는 파트 **이름**에 넣는다 |
| `GET /api/jobs` · `/api/jobs/{id}` | 관리 클러스터 JobSet + 데이터 DB `v_job_status`의 상태 · 진행률 · 마지막 승인 체크포인트 |
| `GET /api/jobs/{id}/events` | 타임라인: 데이터 DB `job_runs` · `checkpoints`(세대별 제출 · 시작 · 종료, 승인 체크포인트) + 관리 클러스터 Event (메모리) |
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
| `KAFKA_BOOTSTRAP` | 비어 있음 | `job.lifecycle` 브로커. Backend가 job.submitted를 보낸다(collector가 DB에 저장). 비우면 안 보냄. JobSet의 ckpt-sidecar에도 넘김 |
| `KAFKA_TOPIC_LIFECYCLE` | `job.lifecycle` | |
| `PGHOST` · `PGPORT` · `PGDATABASE` · `PGUSER` · `PGPASSWORD` · `PGSSLMODE` | 비어 있음 | 데이터 DB(`gpuf_data`, 계정 `backend_ro`, Data `db/004`). 화면 상태 · 진행률 · 타임라인을 읽는다. `PGHOST`를 비우면 관리 클러스터 상태만. DB 장애 때도 목록은 관리 클러스터 상태로 나온다 |
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
internal/service/jobview    데이터 DB 행(v_job_status · job_runs · checkpoints) → 잡별 상태 · 진행률 · 타임라인 (GPU-185)
internal/service/timeline   Kubernetes Event · Workload → 타임라인
internal/repository/        S3 · 관리 클러스터 · DynamoDB(펜싱) · Kafka(job.submitted 발행) · 데이터 DB(읽기) · 메모리 타임라인
internal/settings/          default.json
api/openapi.yaml            API 명세 (테스트가 Gin 라우트와 비교)
```

## 테스트 · CI

```sh
go test -race ./...                         # 순수 로직 (설정 · 간격 계산 · JobSet 렌더링 · 봉투 · 라우트 ↔ 명세)
```

외부 시스템(S3 · DynamoDB · 관리 클러스터 · Kafka)의 동작을 흉내 내는 테스트는 두지 않는다. 동작은 실제 인프라에 배포해 확인한다.
예외: `internal/service/job_test.go`는 관리 클러스터 상태와 데이터 DB 기록을 **합치는 규칙**(DB 장애 때 클러스터 상태 유지, 타임라인 중복 제거)만 메모리 값으로 확인한다.

데이터 DB 읽기 SQL은 Data 레포 `db/001_init.sql` · `db/003_job_progress.sql`을 적용한 PostgreSQL 16에서 확인한다 (CI는 건너뜀).

```sh
docker run -d --name pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=gpuf_data -p 55432:5432 postgres:16-alpine
docker exec pg psql -U postgres -d gpuf_data -c 'CREATE SCHEMA data'
for f in 001_init.sql 003_job_progress.sql; do docker exec -i pg psql -U postgres -d gpuf_data < ../Data/db/$f; done
BACKEND_TEST_PG=1 PGHOST=127.0.0.1 PGPORT=55432 PGUSER=postgres PGPASSWORD=test PGDATABASE=gpuf_data PGSSLMODE=disable \
  go test -run TestJobRecords ./internal/repository/
```

CI(`.github/workflows/ci.yml`): PR · develop push는 lint · 테스트 · 이미지 빌드(push 안 함). develop → main PR 머지 때만 버전을 붙여 배포한다 (ADR-40, GPU-136).

1. PR 본문(GitHub PR 설명 칸)의 `## 릴리스 노트`로 다음 버전을 계산한다. 형식은 `.github/pull_request_template.md`, 검사는 main 필수 검사 `release-notes / release notes`.
2. `ghcr.io/devoops-team/backend:{X.Y.Z, X.Y, main, sha-<7자리>}`를 push한다. description 라벨은 릴리스 노트 요약(Jira 키 포함)이다.
3. 버전 봇이 태그 `vX.Y.Z`와 GitHub Release를 만든다.
4. 배포 봇이 Gitops `onprem/platform/backend/overlays/onprem`에 `newTag: X.Y.Z` + digest를 반영한다.

릴리스 노트가 없는 main 머지는 이미지를 push하지 않는다. 버전 규칙과 봇 권한은 Gitops `tools/release/README.md`.
