#!/usr/bin/env python3
import sys

current_itemset = None
current_count = 0

# Input comes from standard input (stdin)
for line in sys.stdin:
    line = line.strip()
    itemset, count = line.split('\t', 1)
    count = int(count)

    if current_itemset == itemset:
        current_count += count
    else:
        if current_itemset:
            print(f"{current_itemset}\t{current_count}")
        current_count = count
        current_itemset = itemset

if current_itemset == itemset:
    print(f"{current_itemset}\t{current_count}")
