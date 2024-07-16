# loop through the file as a list and search in another file for matching string
# if no match found, print the string


import sys

with open(sys.argv[1]) as f:
    lines = [line.strip() for line in f.readlines()]

with open(sys.argv[2], 'r') as f2:
    data = f2.read()

for line in lines:
    if line in data:
        pass
    else:
        print(f'Not Found: {line}')

# Usage: python find_loop.py file1 file2
# file1: file to loop through
# file2: file to search for matching string
# if no matching string found, print the string
# if found, do nothing
# output: print the string if no matching string found

# Example: You have a list of files in file1 and you want to check if they are present in a json response (file2)
# docker run --rm -v ~bhill/:/bhill us-east4-docker.pkg.dev/dsp-fieldeng-dev/horsefish/general_python:0.2 python3 scripts/find_loop.py /bhill/whattofind.txt /bhill/wheretofind.txt
