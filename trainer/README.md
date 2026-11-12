# trainer

학습 Pod의 trainer 컨테이너 이미지 `ghcr.io/devoops-team/trainer` (JobSet 템플릿 합의안: `train.py`는 이미지 안 `/opt/train`).

- `train.py`: 어떤 Hugging Face causal LM이든 FSDP · DDP로 학습. 업로드한 모델은 `/data/model`, 데이터는 `/data/data/*.jsonl`(ckpt-init이 받아 둠)
- 체크포인트 간격: config.json `checkpoint_policy.interval_seconds`(Young/Daly)를 처음 몇 step의 시간으로 step 간격으로 바꾼다 (설계 문서 5절)
- ckpt-sidecar 약속: `step-N/` 뒤 `step-N.done`, `STOP`이면 종료 코드 75, 마지막 저장 뒤 `.approved`를 120초 기다림
- 테스트: `python3 -m unittest trainer/test_train.py` (torch 없이 순수 함수만)
- CI: `trainer/`가 바뀐 main push에서 이미지 push → 봇이 Gitops backend overlay의 `images:` digest 갱신 → Backend env `TRAINER_IMAGE`
