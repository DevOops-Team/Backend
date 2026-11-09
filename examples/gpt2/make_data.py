"""GPT-2 예시 학습 데이터(train.txt)를 받는다.

tiny-shakespeare(약 1.1MB, 셰익스피어 희곡 발췌) 원문을 그대로 저장한다.
토크나이즈는 train.py가 Pod 안에서 GPT-2 토크나이저로 하므로 여기서는 텍스트만 준비한다.
다른 텍스트로 학습하려면 UTF-8 .txt 파일을 데이터로 올리면 된다 (여러 개면 이름순으로 이어 붙인다).

사용법: python make_data.py [출력 경로]   (기본: train.txt, 표준 라이브러리만 사용)
"""

import sys
import urllib.request

URL = "https://raw.githubusercontent.com/karpathy/char-rnn/master/data/tinyshakespeare/input.txt"


def main(path="train.txt"):
    text = urllib.request.urlopen(URL).read().decode("utf-8")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"{path}: {len(text):,} chars")


if __name__ == "__main__":
    main(*sys.argv[1:])
