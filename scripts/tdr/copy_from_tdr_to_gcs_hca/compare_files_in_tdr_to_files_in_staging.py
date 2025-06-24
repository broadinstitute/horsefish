# Read the first file (access URLs filenames)
with open('dcp51-062425-1855_access_urls_filenames_sorted.txt', 'r') as f:
    file1_lines = [line.strip() for line in f if line.strip()]

# Read the second file (staging areas) and extract just the filenames
with open('dcp51-062425-1855_nonempty_staging_areas.txt', 'r') as f:
    file2_content = f.read()

# Extract filenames from the staging areas file (after the gs:// paths)
import re
file2_lines = []
for line in file2_content.split('\n'):
    if 'gs://' in line:
        # Extract filename from the end of the gs:// path
        match = re.search(r'/([^/]+)$', line.strip())
        if match:
            file2_lines.append(match.group(1))

file1_set = set(file1_lines)
file2_set = set(file2_lines)

print('=== FILE COMPARISON REPORT ===')
print(f'Files in first file: {len(file1_set)}')
print(f'Files in second file: {len(file2_set)}')
print(f'Files in both: {len(file1_set & file2_set)}')
print()

# Files only in first file
only_in_first = file1_set - file2_set
if only_in_first:
    print(f'FILES ONLY IN FIRST FILE ({len(only_in_first)}):')
    for filename in sorted(only_in_first):
        print(f'  - {filename}')
    print()

# Files only in second file
only_in_second = file2_set - file1_set
if only_in_second:
    print(f'FILES ONLY IN SECOND FILE ({len(only_in_second)}):')
    for filename in sorted(only_in_second):
        print(f'  - {filename}')
    print()

# Files in both
in_both = file1_set & file2_set
print(f'FILES IN BOTH ({len(in_both)}):')
for filename in sorted(in_both):
    print(f'  - {filename}')