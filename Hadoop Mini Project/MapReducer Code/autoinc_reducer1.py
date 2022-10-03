#!/usr/bin/env python3.6
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    line = line.strip().split(',')
# Filter only accident records and add count of 1 to each vin number/make/year combination
# Duplicates may be present
    if line[1] == 'A':
        print(f"{line[0]},{line[2]},{line[3]}")

