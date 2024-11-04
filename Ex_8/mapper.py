#!/usr/bin/env python3
import sys
from itertools import combinations

def generate_combinations(item_list, length):
    return list(combinations(item_list, length))

# Input comes from standard input (stdin)
for line in sys.stdin:
    line = line.strip()
    items = line.split()
    for length in range(1, len(items) + 1):
        for combination in generate_combinations(items, length):
            print(f"{','.join(combination)}\t1")
