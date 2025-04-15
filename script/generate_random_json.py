import sys
import random

with open("sample/input-a.json", "w") as f:
    la = lambda _: random.randint(0, 100000000000)
    for x in map(la, range(10000000)):
        print(f"{{\"key\": \"{x:07}\"}}", file=f)
