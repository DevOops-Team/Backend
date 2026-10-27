"""예시 학습 데이터(train.csv)를 만든다. 지금까지 제출한 예시 작업이 쓴 데이터와 같은 파일이 나온다.

합성 선형 회귀 데이터:
  - 5000행 × 9열 CSV, 헤더 없음
  - 앞 8열은 입력 x1..x8, 각각 표준정규분포 N(0, 1)
  - 마지막 열은 정답 y = w·x + ε. w는 seed 0으로 고정한 N(0, 1) 가중치 8개이고 ε ~ N(0, 0.1²)
train.py는 마지막 열을 레이블로 읽어 MLP(8 → 256 → 1)를 MSE로 학습한다.
노이즈 분산이 0.01이라 loss는 약 0.01 근처까지 내려가는 것이 정상이다.

사용법: python make_data.py [출력 경로]   (기본: train.csv, 표준 라이브러리만 사용)
"""

import random
import sys

ROWS, FEATURES, NOISE = 5000, 8, 0.1


def main(path="train.csv"):
    random.seed(0)
    w = [random.gauss(0, 1) for _ in range(FEATURES)]
    with open(path, "w") as f:
        for _ in range(ROWS):
            x = [random.gauss(0, 1) for _ in range(FEATURES)]
            y = sum(a * b for a, b in zip(w, x)) + random.gauss(0, NOISE)
            f.write(",".join(f"{v:.5f}" for v in x + [y]) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
