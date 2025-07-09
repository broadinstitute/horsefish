#!/usr/bin/env python3
"""
Compare files between TDR access URLs list and staging area files.

This script compares filenames from two sources:
1. A sorted list of filenames from TDR access URLs
2. Filenames extracted from a staging area report

Usage:
    python compare_files_in_tdr_to_files_in_staging.py <access_urls_filenames_sorted_file> <nonempty_staging_areas_file>
"""

import sys
import re
import argparse


def extract_filenames_from_staging_report(file_path):
    """Extract filenames from staging areas report file."""
    with open(file_path, 'r') as f:
        content = f.read()

    filenames = []
    for line in content.split('\n'):
        if 'gs://' in line:
            # Extract filename from the end of the gs:// path
            match = re.search(r'/([^/]+)$', line.strip())
            if match:
                filenames.append(match.group(1))
    
    return filenames


def compare_file_lists(access_urls_file, staging_areas_file):
    """Compare files between access URLs list and staging areas report."""
    # Read the first file (access URLs filenames)
    with open(access_urls_file, 'r') as f:
        file1_lines = [line.strip() for line in f if line.strip()]

    # Extract filenames from staging areas file
    file2_lines = extract_filenames_from_staging_report(staging_areas_file)

    file1_set = set(file1_lines)
    file2_set = set(file2_lines)

    print('=== FILE COMPARISON REPORT ===')
    print(f'Files in access URLs file: {len(file1_set)}')
    print(f'Files in staging areas file: {len(file2_set)}')
    print(f'Files in both: {len(file1_set & file2_set)}')
    print()

    # Files only in first file
    only_in_first = file1_set - file2_set
    if only_in_first:
        print(f'FILES ONLY IN ACCESS URLs FILE ({len(only_in_first)}):')
        for filename in sorted(only_in_first):
            print(f'  - {filename}')
        print()

    # Files only in second file
    only_in_second = file2_set - file1_set
    if only_in_second:
        print(f'FILES ONLY IN STAGING AREAS FILE ({len(only_in_second)}):')
        for filename in sorted(only_in_second):
            print(f'  - {filename}')
        print()

    # Files in both
    in_both = file1_set & file2_set
    print(f'FILES IN BOTH ({len(in_both)}):')
    for filename in sorted(in_both):
        print(f'  - {filename}')


def main():
    """Main function to parse arguments and run comparison."""
    parser = argparse.ArgumentParser(
        description='Compare files between TDR access URLs list and staging area files'
    )
    parser.add_argument(
        'access_urls_file',
        help='Path to file containing sorted filenames from TDR access URLs'
    )
    parser.add_argument(
        'staging_areas_file', 
        help='Path to staging areas report file'
    )
    
    args = parser.parse_args()
    
    try:
        compare_file_lists(args.access_urls_file, args.staging_areas_file)
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()