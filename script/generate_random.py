import sys
import random

with open("sample/input-a", "w") as f:
    la = lambda _: random.randint(0, 1000000)
    for x in sorted(set(map(la, range(10000)))):
        print(f"{x:07}", file=f)

with open("sample/input-b", "w") as f:
    la = lambda _: random.randint(0, 1000000)
    for x in sorted(set(map(la, range(10000)))):
        print(f"{x:07}", file=f)

with open("sample/input-unsorted", "w") as f:
    la = lambda _: random.randint(0, 1000000)
    for x in set(map(la, range(10000))):
        print(f"{x:07}", file=f)
