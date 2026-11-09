"""학습 진행 상태 sidecar. 백엔드가 모든 학습 Pod에 native sidecar로 넣는다.

train.py는 $PROGRESS_FILE(공유 emptyDir)에 JSON으로 현재 상태만 쓴다. 이 sidecar가 그 파일을 주기적으로 읽어
바뀌었으면 DynamoDB($PROGRESS_TABLE, PK job_id) 항목을 덮어쓴다. 학습 코드는 AWS SDK가 필요 없고,
DynamoDB 지연이 학습 step을 막지 않는다.

학습 컨테이너가 끝나면 kubelet이 SIGTERM을 보낸다. 마지막 상태(COMPLETED/FAILED)를 한 번 더 쓰고 끝낸다.
파일이 없는 Pod(DDP rank 0이 아닌 Pod)에서는 아무것도 하지 않는다.
"""

import json
import os
import signal
import subprocess
import sys
import time
from decimal import Decimal

JOB_ID = os.environ["JOB_ID"]
TABLE = os.environ.get("PROGRESS_TABLE", "")
PATH = os.environ.get("PROGRESS_FILE", "/progress/progress.json")
NODE = os.environ.get("NODE_NAME", "")
REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
INTERVAL = float(os.environ.get("PROGRESS_INTERVAL", "2"))

stopping = False


def on_term(*_):
    global stopping
    stopping = True


def log(msg):
    print(f"progress-reporter: {msg}", flush=True)


def read_progress():
    try:
        with open(PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def open_table():
    # 기본 이미지에 boto3가 없다. 진행 파일이 처음 생겼을 때 설치한다.
    for attempt in range(1, 6):
        r = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--disable-pip-version-check", "boto3"])
        if r.returncode == 0:
            break
        log(f"pip install boto3 failed (attempt {attempt})")
        time.sleep(5)
    import boto3

    return boto3.resource("dynamodb", region_name=REGION).Table(TABLE)


def push(table, fields):
    fields = dict(fields, updated_at=int(time.time() * 1000))
    if NODE:
        fields.setdefault("node_name", NODE)
    # boto3는 float를 받지 않는다(DynamoDB N 타입은 Decimal).
    values = {f":{k}": (Decimal(str(v)) if isinstance(v, float) else v) for k, v in fields.items()}
    try:
        table.update_item(
            Key={"job_id": JOB_ID},
            UpdateExpression="SET " + ", ".join(f"#{k} = :{k}" for k in fields),
            ExpressionAttributeNames={f"#{k}": k for k in fields},
            ExpressionAttributeValues=values,
        )
        return True
    except Exception as e:  # noqa: BLE001
        log(f"write failed: {e}")
        return False


def main():
    signal.signal(signal.SIGTERM, on_term)
    if not TABLE:
        log("PROGRESS_TABLE not set, idle")
    table, last = None, None
    while True:
        cur = read_progress() if TABLE else None
        if cur is not None and cur != last:
            if table is None:
                table = open_table()
            if push(table, cur):
                last = cur
        if stopping:
            return
        deadline = time.time() + INTERVAL
        while time.time() < deadline and not stopping:
            time.sleep(0.2)


if __name__ == "__main__":
    main()
