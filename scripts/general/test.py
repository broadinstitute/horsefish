# Usage: python test.py file1 file2
# file1: file to loop through
# file2: file to search for matching string
# if no matching string found, print the string
# if found, do nothing
# output: print the string if no matching string found, print "found" if all strings are found

import sys

with open(sys.argv[1]) as f:
    lines = [line.strip() for line in f.readlines()]

with open(sys.argv[2], 'r') as f2:
    data = f2.read()

counter = 0
for line in lines:
    if line not in data:
        print(f'Not Found: {line}')
        counter += 1
    else:
        print("counter")