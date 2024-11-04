#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

# Input comes from standard input (stdin)
for line in sys.stdin:
    # Remove leading and trailing whitespace
    line = line.strip()

    # Parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # Ignore/discard this line if count is not an integer
        continue

    # This IF-switch works because Hadoop sorts map output
    # by key (here: word) before passing it to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # Output the word and its aggregated count
            print(f'{current_word}\t{current_count}')
        current_count = count
        current_word = word

# Output the last word if needed
if current_word == word:
    print(f'{current_word}\t{current_count}')
