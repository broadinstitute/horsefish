# Description: This script copies data from Terra Data Repository (TDR) HCA project buckets to HCA staging area buckets.
# It is based on the bash script get_snapshot_files_and_transfer.sh, written by Samantha Velasquez.

import os
import sys
import csv
import logging
import re
import requests
import google.auth
import google.auth.transport.requests
import subprocess
import concurrent.futures
from functools import partial
import gc
import argparse
from datetime import datetime

STAGING_AREA_BUCKETS = {
    "prod": {
        "EBI": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "UCSC": "gs://broad-dsp-monster-hca-prod-ebi-storage/prod",
        "LANTERN": "gs://broad-dsp-monster-hca-prod-lantern",
        "LATTICE": "gs://broad-dsp-monster-hca-prod-lattice/staging",
        "TEST": "gs://broad-dsp-monster-hca-prod-ebi-storage/broad_test_dataset"
        },
    "dev": {
        "EBI": "gs://broad-dsp-monster-hca-dev-ebi-staging/dev",
        "UCSC": "gs://broad-dsp-monster-hca-dev-ebi-staging/dev"
        },
}


def generate_timestamped_basename(csv_path: str) -> str:
    """Generate timestamped base filename from CSV manifest path.
    
    Extracts the base filename, strips '_manifest' if present, and adds timestamp.
    Format: basename_MMDDYY-HHMM
    
    Args:
        csv_path: Path to the CSV manifest file
        
    Returns:
        str: Timestamped base filename (e.g., 'HCADevRefresh_May2025-070325-1400')
    """
    # Get base filename without extension
    base_filename = os.path.splitext(os.path.basename(csv_path))[0]
    
    # Strip '_manifest' suffix if present
    if base_filename.endswith('_manifest'):
        base_filename = base_filename[:-9]  # Remove '_manifest'
    
    # Generate timestamp in MMDDYY-HHMM format
    timestamp = datetime.now().strftime('%m%d%y-%H%M')
    
    # Combine base filename with timestamp
    return f"{base_filename}-{timestamp}"


def get_output_directory(csv_path: str) -> str:
    """Create and return the output directory for the current run.
    
    Creates a timestamped directory in runs/ for storing all output files.
    
    Args:
        csv_path: Path to the CSV manifest file
        
    Returns:
        str: Path to the output directory
    """
    # Get the directory of this script file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Go up one level to project root
    project_root = os.path.dirname(script_dir)
    
    # Create runs directory if it doesn't exist
    runs_dir = os.path.join(project_root, 'runs')
    os.makedirs(runs_dir, exist_ok=True)
    
    # Generate timestamped basename
    basename = generate_timestamped_basename(csv_path)
    
    # Create run-specific directory
    output_dir = os.path.join(runs_dir, basename)
    os.makedirs(output_dir, exist_ok=True)
    
    return output_dir


def setup_cli_logging_format(log_filename: str) -> None:    
    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Create handlers for both console and file output
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_filename, mode='w')
    
    # Set format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(message)s')
    
    console_handler.setFormatter(console_formatter)
    file_handler.setFormatter(formatter)
    
    # Configure root logger directly instead of using basicConfig
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

def validate_input(csv_path: str):
    """
    input should be a manifest csv of those projects that need data copied back
    format is <institution>,<project_id>
    """
    if not os.path.isfile(csv_path):
        logging.info(f"{csv_path} not found")
        sys.exit(1)

    if not csv_path.endswith('.csv'):
        logging.info(f"{csv_path} is not a csv file")
        sys.exit(1)

    else:
        return csv_path


def find_project_id_in_str(s: str) -> str:
    """
    The selected function find_project_id_in_str(s: str) -> str:
    is used to extract a valid UUID (Universally Unique Identifier) from a given string s.
    :param s:
    :return:
    Attribution:
    https://github.com/DataBiosphere/hca-ingest/blob/main/orchestration/hca_orchestration/support/matchers.py
    """
    uuid_matcher = re.compile('[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}', re.I)
    project_ids = uuid_matcher.findall(s)

    if len(project_ids) != 1:
        raise Exception(f"Found more than one or zero project UUIDs in {s}")

    return str(project_ids[0])


def _sanitize_staging_gs_path(path: str) -> str:
    return path.strip().strip("/")


def _parse_csv(csv_path: str, env: str):
    """
    Parses the csv file and returns a list of staging areas
    :param csv_path:
    :param env: Environment to use ('prod' or 'dev')
    :return:
    Attribution:
    https://github.com/DataBiosphere/hca-ingest/blob/main/orchestration/hca_manage/manifest.py
    """
    if env not in STAGING_AREA_BUCKETS:
        raise Exception(f"Unknown environment '{env}'. Must be one of: {list(STAGING_AREA_BUCKETS.keys())}")
    
    env_buckets = STAGING_AREA_BUCKETS[env]
    tuple_list = []
    
    # Handle relative paths from the project root
    if not os.path.isabs(csv_path):
        # Get the directory of this script file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up one level to project root, then construct path
        project_root = os.path.dirname(script_dir)
        csv_path = os.path.join(project_root, csv_path)
    
    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                logging.debug("Empty path detected, skipping")
                continue

            assert len(row) == 2
            institution = row[0]
            project_id = find_project_id_in_str(row[1])

            if institution not in env_buckets:
                raise Exception(f"Unknown institution {institution} found for environment '{env}'. "
                                f"Make sure the institution is in the list of staging area buckets and is in all caps. "
                                f"Available institutions for {env}: {list(env_buckets.keys())}")

            institution_bucket = env_buckets[institution]
            path = institution_bucket + "/" + project_id

            # sanitize and dedupe
            path = _sanitize_staging_gs_path(path)
            assert path.startswith("gs://"), "Staging area path must start with gs:// scheme"
            staging_gs_path = path

            tuple_list.append((staging_gs_path, project_id))

        return tuple_list


def get_access_token():
    creds, project = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    creds.refresh(auth_req)
    access_token = creds.token
    return access_token


def get_latest_snapshot(target_snapshot: str, access_token: str):
    snapshot_response = requests.get(
        f'https://data.terra.bio/api/repository/v1/snapshots?offset=0&limit=10&sort=created_date&direction=desc&filter='
        f'{target_snapshot}',
        headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
    )
    snapshot_response.raise_for_status()
    latest_snapshot_id = snapshot_response.json()['items'][0]['id']
    return latest_snapshot_id


def get_access_urls(snapshot: str, access_token: str, output_basename: str = None):
    """
    Retrieves access URLs for files in a given snapshot from the Terra Data Repository (TDR).
    Also writes the access URLs and filenames to a file for debugging and verification purposes.
    If there are more than 150 files or there are a mix of sequence and analysis files, check with the wranglers before copying.
    :param snapshot:
    :param access_token:
    :param output_basename: Base filename for output files (optional)
    :return:
    """

    list_of_access_urls = []
    offset = 0
    limit = 1000
    logging.info(f"getting access urls for snapshot {snapshot}")
    while True:
        files_response = requests.get(
            f'https://data.terra.bio/api/repository/v1/snapshots/{snapshot}/files?offset={offset}&limit={limit}',
            headers={'accept': 'application/json', 'Authorization': f'Bearer {access_token}'}
        )
        files_response.raise_for_status()
        data = files_response.json()
        if not data:
            break
        for item in data:
            list_of_access_urls.append(item['fileDetail']['accessUrl'])
        offset += limit
    num_access_urls = len(list_of_access_urls)

    # for debugging and maybe we want a manifest?
    logging.info(f'number of access urls for snapshot {snapshot} is {num_access_urls}')
    
    # Use provided basename or default filenames
    access_urls_filename = f'{output_basename}_access_urls.txt' if output_basename else 'access_urls.txt'
    sorted_filenames_file = f'{output_basename}_access_urls_filenames_sorted.txt' if output_basename else 'access_urls_filenames_sorted.txt'
    
    with open(access_urls_filename, 'w') as f:
        for access_url in list_of_access_urls:
            f.write(f'{access_url}\n')
    # Extract filenames, sort them, and write to a different file
    filenames = [access_url.split('/')[-1] for access_url in list_of_access_urls]
    filenames.sort()
    with open(sorted_filenames_file, 'w') as f:
        for filename in filenames:
            f.write(f'{filename}\n')

    return list_of_access_urls




def check_single_staging_area(staging_dir: str) -> dict:
    """Check a single staging area for contents"""
    staging_data_dir = staging_dir + '/data/'
    logging.info(f'checking contents of staging_data dir: {staging_data_dir}')
    # using gsutil as output is cleaner & faster
    output = subprocess.run(['gsutil', 'ls', staging_data_dir], capture_output=True)
    stdout = output.stdout.strip()
    files = stdout.decode('utf-8').split('\n')
    
    if len(files) > 0 and files != ['']:
        logging.error(f"Staging area {staging_data_dir} is not empty")
        logging.info(f"files in staging area are: {files}")
        # Extract project_id from staging_dir for the report
        project_id = staging_dir.split('/')[-1]
        return {
            'project_id': project_id,
            'staging_dir': staging_data_dir,
            'files': files
        }
    else:
        logging.info(f"Staging area {staging_data_dir} is empty or does not exist")
        return {}


def check_staging_areas_batch(staging_gs_paths: set[str], allow_override: bool = False, output_basename: str = None) -> dict:
    """Check multiple staging areas in parallel and handle non-empty areas"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_path = {
            executor.submit(check_single_staging_area, path): path 
            for path in staging_gs_paths
        }
        
        results = {}
        nonempty_staging_details = []
        
        for future in concurrent.futures.as_completed(future_to_path):
            path = future_to_path[future]
            try:
                result = future.result()
                results[path] = result
                # If result contains staging area details (not empty), add to tracking
                if result and 'project_id' in result:
                    nonempty_staging_details.append(result)
            except Exception as e:
                logging.error(f"Error checking {path}: {e}")
                results[path] = {'error': str(e)}
        
        # Write non-empty staging areas to file
        if len(nonempty_staging_details) > 0:
            nonempty_filename = f'{output_basename}_nonempty_staging_areas.txt' if output_basename else 'nonempty_staging_areas.txt'
            with open(nonempty_filename, 'w') as f:
                f.write("Non-empty staging areas report\n")
                f.write("=" * 50 + "\n\n")
                for detail in nonempty_staging_details:
                    f.write(f"Project ID: {detail['project_id']}\n")
                    f.write(f"Staging Directory: {detail['staging_dir']}\n")
                    f.write("Files:\n")
                    for file in detail['files']:
                        if file:  # Skip empty strings
                            f.write(f"  - {file}\n")
                    f.write("\n")

            # Extract staging areas for logging (if needed)
            nonempty_areas = [detail['staging_dir'] for detail in nonempty_staging_details]
            logging.error("One or more staging areas are not empty.")
            logging.info(f"Non-empty staging areas are: {nonempty_areas}")
            logging.info(f"Details written to {nonempty_filename}")
            
            if allow_override:
                user_input = input("Would you like to proceed anyway? (y/n): ")
                if user_input.lower() == 'y' or user_input.lower() == 'yes':
                    logging.info("User chose to proceed with non-empty staging areas.")
                    return results
                else:
                    logging.info("User chose not to proceed with non-empty staging areas. Exiting.")
                    sys.exit(1)
            else:
                sys.exit(1)
        
        return results


def process_projects_streaming(tuple_list: list[tuple[str, str]], access_token: str, dry_run: bool = False, verify_integrity: bool = True, output_basename: str = None):
    """
    Stream-process projects one at a time to reduce memory usage while maintaining parallel file copying.
    This approach processes each project individually and cleans up memory between projects.
    """
    # Initialize cumulative tracking variables
    cumulative_failed_urls = []
    cumulative_access_urls_for_file = []
    cumulative_integrity_failed_files = []
    
    # Get unique project IDs to process
    unique_projects = list(set([x[1] for x in tuple_list]))
    total_projects = len(unique_projects)
    
    logging.info(f"Starting streaming processing of {total_projects} projects...")
    
    for i, project_id in enumerate(unique_projects, 1):
        logging.info(f"Processing project {i}/{total_projects}: {project_id}")
        
        try:
            # Get project-specific information
            target_snapshot = f"hca_prod_{project_id.replace('-', '')}"
            latest_snapshot_id = get_latest_snapshot(target_snapshot, access_token)
            logging.info(f'Latest snapshot id for project {project_id} is {latest_snapshot_id}')
            
            # Get access URLs for this project
            access_urls = get_access_urls(latest_snapshot_id, access_token, output_basename)
            num_access_urls = len(access_urls)
            
            # Add to comprehensive list with project info
            for url in access_urls:
                # Extract bucket from access URL
                bucket_match = re.search(r'gs://([^/]+)/', url)
                bucket_name = bucket_match.group(1) if bucket_match else 'unknown_bucket'
                cumulative_access_urls_for_file.append({
                    'project_id': project_id,
                    'bucket': bucket_name,
                    'url': url
                })
            
            # Get staging path for this project
            staging_gs_path = [x[0] for x in tuple_list if x[1] == project_id][0]
            staging_data_dir = staging_gs_path + '/data/'
            
            if dry_run:
                logging.info(f'DRY RUN: Would copy {num_access_urls} files from snapshot {latest_snapshot_id} to staging area {staging_data_dir}')
                logging.info(f'See local file access_urls.txt for the list of access URLs')
                continue
            
            # Get a list of files already in the staging directory before copying
            output_before = subprocess.run(['gsutil', 'ls', staging_data_dir],
                                          capture_output=True).stdout.decode('utf-8').split('\n')
            files_before = set([x.split('/')[-1] for x in output_before if x and x.split('/')[-1]])
            logging.info(f'Found {len(files_before)} files already in staging area before copying')
            
            logging.info(f'Copying {num_access_urls} files from snapshot {latest_snapshot_id} to staging area {staging_data_dir}')
            
            # Use parallel copying for better performance
            successfully_copied, failed_copies = copy_files_parallel(access_urls, staging_data_dir, project_id, verify_integrity=verify_integrity)
            
            # Process failed copies and add to cumulative tracking
            for failed_copy in failed_copies:
                cumulative_failed_urls.append(failed_copy)
                # Track integrity verification failures separately with metadata
                if 'integrity verification failed' in failed_copy.get('error', '').lower():
                    # Extract bucket from access URL
                    bucket_match = re.search(r'gs://([^/]+)/', failed_copy['url'])
                    bucket_name = bucket_match.group(1) if bucket_match else 'unknown_bucket'
                    cumulative_integrity_failed_files.append({
                        'project_id': project_id,
                        'bucket': bucket_name,
                        'url': failed_copy['url']
                    })
            
            # Verify what's actually in the staging directory after copying
            output_after = subprocess.run(['gsutil', 'ls', staging_data_dir],
                                        capture_output=True).stdout.decode('utf-8').split('\n')
            files_after = set([x.split('/')[-1] for x in output_after if x and x.split('/')[-1]])
            
            # Calculate newly copied files (files that exist now but didn't before)
            newly_copied = files_after - files_before
            
            # Compare our tracking with what's actually in the directory
            if len(newly_copied) != len(successfully_copied):
                logging.warning(f'Tracking mismatch: tracked {len(successfully_copied)} successful copies, '
                              f'but found {len(newly_copied)} new files in the staging directory')
            
            # Log the results for this project
            logging.info(f'Project {project_id}: {len(successfully_copied)} out of {num_access_urls} files successfully copied to {staging_data_dir}')
            if len(successfully_copied) < num_access_urls:
                logging.warning(f'Project {project_id}: Failed to copy {num_access_urls - len(successfully_copied)} files')
            
            # For verification, list the files in the staging directory
            logging.info(f'Project {project_id}: Files now in staging directory: {len(files_after)}')
            
            # Clean up project-specific variables to free memory
            del access_urls, successfully_copied, failed_copies, files_before, files_after, newly_copied
            
        except Exception as e:
            logging.error(f"Error processing project {project_id}: {str(e)}")
            # Continue with next project even if one fails
            continue
        
        # Force garbage collection between projects
        gc.collect()
        logging.info(f"Completed processing project {i}/{total_projects}: {project_id}")
    
    # Write all output files at the end
    _write_output_files(cumulative_failed_urls, cumulative_integrity_failed_files, cumulative_access_urls_for_file, output_basename)
    
    logging.info(f"Completed streaming processing of all {total_projects} projects")


def _write_output_files(all_failed_urls, integrity_failed_files, all_access_urls_for_file, output_basename: str = None):
    """Helper function to write all output files"""
    # Use provided basename or default filenames
    failed_urls_filename = f'{output_basename}_failed_access_urls.txt' if output_basename else 'failed_access_urls.txt'
    integrity_failed_filename = f'{output_basename}_integrity_verification_failed.txt' if output_basename else 'integrity_verification_failed.txt'
    all_urls_filename = f'{output_basename}_all_access_urls_by_bucket.txt' if output_basename else 'all_access_urls_by_bucket.txt'
    
    # Write failed URLs to file
    if all_failed_urls:
        with open(failed_urls_filename, 'w') as f:
            f.write("Failed Access URLs Report\n")
            f.write("=" * 40 + "\n\n")
            for failed in all_failed_urls:
                f.write(f"Project ID: {failed['project_id']}\n")
                f.write(f"URL: {failed['url']}\n")
                f.write(f"Error: {failed['error']}\n")
                f.write("-" * 40 + "\n")
        logging.info(f"Failed URLs written to {failed_urls_filename} ({len(all_failed_urls)} failed)")
    
    # Write integrity verification failed files to separate file
    if integrity_failed_files:
        # Sort by bucket, then by project_id, then by URL
        integrity_failed_files.sort(key=lambda x: (x['bucket'], x['project_id'], x['url']))
        
        with open(integrity_failed_filename, 'w') as f:
            f.write("Integrity Verification Failed Files Grouped by Bucket\n")
            f.write("=" * 60 + "\n\n")
            current_bucket = None
            for item in integrity_failed_files:
                if item['bucket'] != current_bucket:
                    if current_bucket is not None:
                        f.write("\n")
                    f.write(f"BUCKET: {item['bucket']}\n")
                    f.write("-" * 40 + "\n")
                    current_bucket = item['bucket']
                f.write(f"Project: {item['project_id']} | {item['url']}\n")
        logging.info(f"Integrity verification failed files written to {integrity_failed_filename} ({len(integrity_failed_files)} files)")
    
    # Write comprehensive access URLs file, sorted and grouped by bucket
    if all_access_urls_for_file:
        # Sort by bucket, then by project_id, then by URL
        all_access_urls_for_file.sort(key=lambda x: (x['bucket'], x['project_id'], x['url']))
        
        with open(all_urls_filename, 'w') as f:
            f.write("All Access URLs Grouped by Bucket\n")
            f.write("=" * 50 + "\n\n")
            current_bucket = None
            for item in all_access_urls_for_file:
                if item['bucket'] != current_bucket:
                    if current_bucket is not None:
                        f.write("\n")
                    f.write(f"BUCKET: {item['bucket']}\n")
                    f.write("-" * 30 + "\n")
                    current_bucket = item['bucket']
                f.write(f"Project: {item['project_id']} | {item['url']}\n")
        logging.info(f"All access URLs written to {all_urls_filename} ({len(all_access_urls_for_file)} total)")

def copy_single_file(access_url: str, staging_data_dir: str, project_id: str, verify_integrity: bool = True) -> dict:
    """Copy a single file and return result info with optional integrity verification"""
    filename = access_url.split('/')[-1]
    dest_path = staging_data_dir + filename
    
    try:
        # Copy the file
        result = subprocess.run(['gcloud', 'storage', 'cp', access_url, dest_path],
                              capture_output=True, timeout=300)  # 5 min timeout
        if result.returncode == 0:
            # Verify file integrity if requested
            if verify_integrity:
                if verify_file_integrity(access_url, dest_path):
                    return {'status': 'success', 'filename': filename, 'project_id': project_id}
                else:
                    return {'status': 'failed', 'url': access_url, 'project_id': project_id, 
                           'error': 'File integrity verification failed - checksums do not match'}
            else:
                return {'status': 'success', 'filename': filename, 'project_id': project_id}
        else:
            return {'status': 'failed', 'url': access_url, 'project_id': project_id, 
                   'error': result.stderr.decode("utf-8").strip()}
    except Exception as e:
        return {'status': 'failed', 'url': access_url, 'project_id': project_id, 'error': str(e)}


def copy_files_parallel(access_urls: list, staging_data_dir: str, project_id: str, max_workers: int = 5, verify_integrity: bool = True):
    """Copy files in parallel with limited concurrency and optional integrity verification"""
    copy_func = partial(copy_single_file, staging_data_dir=staging_data_dir, project_id=project_id, verify_integrity=verify_integrity)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(copy_func, access_urls))
    
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'failed']
    
    return successful, failed


def compare_checksums(source_stat: str, dest_stat: str) -> bool:
    """Compare checksums from gsutil stat output"""
    try:
        # Extract MD5 or CRC32C checksums from gsutil stat output
        def extract_checksums(stat_output: str) -> dict:
            checksums = {}
            for line in stat_output.split('\n'):
                line = line.strip()
                if line.startswith('Hash (md5):'):
                    checksums['md5'] = line.split(':', 1)[1].strip()
                elif line.startswith('Hash (crc32c):'):
                    checksums['crc32c'] = line.split(':', 1)[1].strip()
            return checksums
        
        source_checksums = extract_checksums(source_stat)
        dest_checksums = extract_checksums(dest_stat)
        
        # Compare available checksums
        if 'md5' in source_checksums and 'md5' in dest_checksums:
            return source_checksums['md5'] == dest_checksums['md5']
        elif 'crc32c' in source_checksums and 'crc32c' in dest_checksums:
            return source_checksums['crc32c'] == dest_checksums['crc32c']
        else:
            logging.warning("No matching checksums found for comparison")
            return False
            
    except Exception as e:
        logging.warning(f"Error comparing checksums: {e}")
        return False


def verify_file_integrity(source_url: str, dest_path: str) -> bool:
    """Verify file was copied correctly using checksums"""
    try:
        # Get source file info
        source_info = subprocess.run(['gsutil', 'stat', source_url], 
                                   capture_output=True, text=True, timeout=60)
        if source_info.returncode != 0:
            logging.warning(f"Could not get source file info for {source_url}: {source_info.stderr}")
            return False
            
        # Get destination file info  
        dest_info = subprocess.run(['gsutil', 'stat', dest_path], 
                                 capture_output=True, text=True, timeout=60)
        if dest_info.returncode != 0:
            logging.warning(f"Could not get destination file info for {dest_path}: {dest_info.stderr}")
            return False
        
        # Compare checksums
        return compare_checksums(source_info.stdout, dest_info.stdout)
    except Exception as e:
        logging.warning(f"Could not verify integrity for {dest_path}: {e}")
        return False





def main():
    """Parse command-line arguments and run specified tool.

    Usage: python copy_from_tdr_to_gcs_hca.py <csv_path> --env <env> [--dry-run] [--allow-override] [--skip-integrity-check]

    Args:
        csv_path: Path to a CSV file with institution and project ID.
        --env: Environment to copy to ('prod' or 'dev').
        --dry-run: Optional flag. If present, only access URLs will be displayed without copying files.
        --allow-override: Optional flag. If present, user will be prompted whether to continue
                        when non-empty staging areas are found.
        --skip-integrity-check: Optional flag. If present, file integrity verification will be skipped (faster but less safe).
    """
    import argparse

    # Set up argument parser first to get csv_path for basename generation
    parser = argparse.ArgumentParser(description='Copy data from TDR to GCS for HCA projects.')
    parser.add_argument('csv_path', help='Path to CSV file with institution and project ID')
    parser.add_argument('--env', required=True, choices=['prod', 'dev'], 
                        help='Environment to copy to (prod or dev)')
    parser.add_argument('--dry-run', action='store_true', help='Only display access URLs without copying files')
    parser.add_argument('--allow-override', action='store_true',
                        help='When non-empty staging areas are found, prompt user whether to continue')
    parser.add_argument('--skip-integrity-check', action='store_true',
                        help='Skip file integrity verification after copying (faster but less safe)')

    # Parse arguments
    args = parser.parse_args()

    # Create output directory and get paths
    output_dir = get_output_directory(args.csv_path)
    output_basename = generate_timestamped_basename(args.csv_path)
    log_filename = os.path.join(output_dir, f'{output_basename}_copy_tdr_to_gcs_hca.log')

    # Validate input
    validate_input(args.csv_path)
    logging.info(f"Using environment: {args.env}")
    logging.info(f"Output basename: {output_basename}")
    tuple_list = _parse_csv(args.csv_path, args.env)
    logging.debug(f"staging_gs_paths and project id tuple list is {tuple_list}")

    # Change to output directory for file operations
    original_cwd = os.getcwd()
    os.chdir(output_dir)

    # Set up logging with timestamped filename
    setup_cli_logging_format(log_filename)
    access_token = get_access_token()

    # staging dir is the first element in each tuple
    staging_gs_paths = set([x[0] for x in tuple_list])

    # check if the staging areas are empty (in parallel)
    check_staging_areas_batch(staging_gs_paths, args.allow_override, output_basename)
    # copy the files from the TDR project bucket to the staging area bucket using streaming approach
    process_projects_streaming(tuple_list, access_token, args.dry_run, verify_integrity=not args.skip_integrity_check, output_basename=output_basename)
    
    # Log information about output files
    logging.info("Script execution completed. Output files created:")
    logging.info(f"- {log_filename}: Complete log of all operations")
    
    # Check for files with the timestamped basename
    failed_urls_file = f'{output_basename}_failed_access_urls.txt'
    if os.path.exists(failed_urls_file):
        logging.info(f"- {failed_urls_file}: List of URLs that failed to copy")
    
    integrity_failed_file = f'{output_basename}_integrity_verification_failed.txt'
    if os.path.exists(integrity_failed_file):
        logging.info(f"- {integrity_failed_file}: List of files that failed integrity verification")
    
    all_urls_file = f'{output_basename}_all_access_urls_by_bucket.txt'
    if os.path.exists(all_urls_file):
        logging.info(f"- {all_urls_file}: All access URLs sorted and grouped by bucket")
    
    nonempty_staging_file = f'{output_basename}_nonempty_staging_areas.txt'
    if os.path.exists(nonempty_staging_file):
        logging.info(f"- {nonempty_staging_file}: Report of staging areas that were not empty")
    
    access_urls_file = f'{output_basename}_access_urls.txt'
    if os.path.exists(access_urls_file):
        logging.info(f"- {access_urls_file}: Raw access URLs (from get_access_urls function)")
    
    sorted_filenames_file = f'{output_basename}_access_urls_filenames_sorted.txt'
    if os.path.exists(sorted_filenames_file):
        logging.info(f"- {sorted_filenames_file}: Sorted filenames (from get_access_urls function)")
    
    # Restore original working directory
    os.chdir(original_cwd)
    logging.info(f"All output files written to: {output_dir}")
    


if __name__ == '__main__':
    sys.exit(main())
