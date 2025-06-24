"""
Test suite for copy_from_tdr_to_gcs_hca.py

This module contains comprehensive tests for the TDR to GCS HCA copy script,
including unit tests for individual functions and integration tests for
the main workflows.
"""

import pytest
import os
import csv
import tempfile
import subprocess
import json
from unittest.mock import Mock, patch, mock_open, MagicMock
from copy_from_tdr_to_gcs_hca import (
    validate_input,
    find_project_id_in_str,
    _sanitize_staging_gs_path,
    _parse_csv,
    get_access_token,
    get_latest_snapshot,
    get_access_urls,
    check_single_staging_area,
    check_staging_areas_batch,
    copy_single_file,
    copy_files_parallel,
    compare_checksums,
    verify_file_integrity,
    _write_output_files,
    generate_timestamped_basename,
    STAGING_AREA_BUCKETS
)


class TestValidateInput:
    """Test cases for validate_input function"""
    
    @pytest.mark.unit
    def test_validate_input_valid_csv(self, tmp_path):
        """Test validation of a valid CSV file"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("EBI,test-project-id\n")
        
        result = validate_input(str(csv_file))
        assert result == str(csv_file)
    
    @pytest.mark.unit
    def test_validate_input_file_not_found(self):
        """Test validation fails when file doesn't exist"""
        with pytest.raises(SystemExit):
            validate_input("/nonexistent/file.csv")
    
    @pytest.mark.unit
    def test_validate_input_not_csv(self, tmp_path):
        """Test validation fails when file is not a CSV"""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("some content")
        
        with pytest.raises(SystemExit):
            validate_input(str(txt_file))


class TestFindProjectIdInStr:
    """Test cases for find_project_id_in_str function"""
    
    def test_find_valid_uuid(self):
        """Test extraction of valid UUID"""
        test_string = "project-12345678-1234-4abc-8def-123456789012-suffix"
        result = find_project_id_in_str(test_string)
        assert result == "12345678-1234-4abc-8def-123456789012"
    
    def test_find_uuid_with_hyphens(self):
        """Test extraction of UUID with hyphens"""
        test_string = "12345678-1234-4abc-8def-123456789012"
        result = find_project_id_in_str(test_string)
        assert result == "12345678-1234-4abc-8def-123456789012"
    
    def test_no_uuid_found(self):
        """Test exception when no UUID is found"""
        with pytest.raises(Exception, match="Found more than one or zero project UUIDs"):
            find_project_id_in_str("no-uuid-here")
    
    def test_multiple_uuids_found(self):
        """Test exception when multiple UUIDs are found"""
        test_string = "12345678-1234-4abc-8def-123456789012 and 87654321-1234-4cba-8def-210987654321"
        with pytest.raises(Exception, match="Found more than one or zero project UUIDs"):
            find_project_id_in_str(test_string)


class TestSanitizeStagingGsPath:
    """Test cases for _sanitize_staging_gs_path function"""
    
    def test_strip_trailing_slash(self):
        """Test removal of trailing slash"""
        result = _sanitize_staging_gs_path("gs://bucket/path/")
        assert result == "gs://bucket/path"
    
    def test_strip_whitespace(self):
        """Test removal of whitespace"""
        result = _sanitize_staging_gs_path("  gs://bucket/path  ")
        assert result == "gs://bucket/path"
    
    def test_no_changes_needed(self):
        """Test path that doesn't need sanitization"""
        result = _sanitize_staging_gs_path("gs://bucket/path")
        assert result == "gs://bucket/path"


class TestGenerateTimestampedBasename:
    """Test cases for generate_timestamped_basename function"""
    
    @pytest.mark.unit
    @patch('copy_from_tdr_to_gcs_hca.datetime')
    def test_generate_timestamped_basename_with_manifest_suffix(self, mock_datetime):
        """Test basename generation with _manifest suffix"""
        # Mock datetime to return a specific timestamp
        mock_datetime.now.return_value.strftime.return_value = "062425-1400"
        
        result = generate_timestamped_basename("HCADevRefresh_May2025_manifest.csv")
        assert result == "HCADevRefresh_May2025-062425-1400"
    
    @pytest.mark.unit
    @patch('copy_from_tdr_to_gcs_hca.datetime')
    def test_generate_timestamped_basename_without_manifest_suffix(self, mock_datetime):
        """Test basename generation without _manifest suffix"""
        mock_datetime.now.return_value.strftime.return_value = "062425-1400"
        
        result = generate_timestamped_basename("test_file.csv")
        assert result == "test_file-062425-1400"
    
    @pytest.mark.unit
    @patch('copy_from_tdr_to_gcs_hca.datetime')
    def test_generate_timestamped_basename_with_path(self, mock_datetime):
        """Test basename generation with full file path"""
        mock_datetime.now.return_value.strftime.return_value = "062425-1400"
        
        result = generate_timestamped_basename("/path/to/HCA_prod_manifest.csv")
        assert result == "HCA_prod-062425-1400"
    
    @pytest.mark.unit
    @patch('copy_from_tdr_to_gcs_hca.datetime')
    def test_generate_timestamped_basename_simple_filename(self, mock_datetime):
        """Test basename generation with simple filename"""
        mock_datetime.now.return_value.strftime.return_value = "062425-1400"
        
        result = generate_timestamped_basename("simple.csv")
        assert result == "simple-062425-1400"
    
    @pytest.mark.unit
    def test_generate_timestamped_basename_real_timestamp(self):
        """Test basename generation with real timestamp (format validation)"""
        result = generate_timestamped_basename("test_manifest.csv")
        # Should be in format: test-MMDDYY-HHMM
        import re
        pattern = r"test-\d{6}-\d{4}"
        assert re.match(pattern, result), f"Result '{result}' doesn't match expected pattern"


class TestParseCsv:
    """Test cases for _parse_csv function"""
    
    def test_parse_valid_csv_prod(self, tmp_path):
        """Test parsing valid CSV for prod environment"""
        csv_file = tmp_path / "test.csv"
        csv_content = "EBI,12345678-1234-4abc-8def-123456789012\n"
        csv_file.write_text(csv_content)
        
        result = _parse_csv(str(csv_file), "prod")
        expected_path = "gs://broad-dsp-monster-hca-prod-ebi-storage/prod/12345678-1234-4abc-8def-123456789012"
        assert result == [(expected_path, "12345678-1234-4abc-8def-123456789012")]
    
    def test_parse_valid_csv_dev(self, tmp_path):
        """Test parsing valid CSV for dev environment"""
        csv_file = tmp_path / "test.csv"
        csv_content = "EBI,12345678-1234-4abc-8def-123456789012\n"
        csv_file.write_text(csv_content)
        
        result = _parse_csv(str(csv_file), "dev")
        expected_path = "gs://broad-dsp-monster-hca-dev-ebi-staging/dev/12345678-1234-4abc-8def-123456789012"
        assert result == [(expected_path, "12345678-1234-4abc-8def-123456789012")]
    
    def test_parse_invalid_environment(self, tmp_path):
        """Test parsing with invalid environment"""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("EBI,test-project\n")
        
        with pytest.raises(Exception, match="Unknown environment 'invalid'"):
            _parse_csv(str(csv_file), "invalid")
    
    def test_parse_invalid_institution(self, tmp_path):
        """Test parsing with invalid institution"""
        csv_file = tmp_path / "test.csv"
        csv_content = "INVALID,12345678-1234-4abc-8def-123456789012\n"
        csv_file.write_text(csv_content)
        
        with pytest.raises(Exception, match="Unknown institution INVALID found"):
            _parse_csv(str(csv_file), "prod")
    
    def test_parse_empty_rows(self, tmp_path):
        """Test parsing CSV with empty rows"""
        csv_file = tmp_path / "test.csv"
        csv_content = "EBI,12345678-1234-4abc-8def-123456789012\n\n"
        csv_file.write_text(csv_content)
        
        result = _parse_csv(str(csv_file), "prod")
        assert len(result) == 1


class TestGetAccessToken:
    """Test cases for get_access_token function"""
    
    @pytest.mark.unit
    @patch('copy_from_tdr_to_gcs_hca.google.auth.default')
    def test_get_access_token_success(self, mock_auth_default):
        """Test successful token retrieval"""
        mock_creds = Mock()
        mock_creds.token = "test-token"
        mock_auth_default.return_value = (mock_creds, "test-project")
        
        result = get_access_token()
        assert result == "test-token"
        mock_creds.refresh.assert_called_once()


class TestGetLatestSnapshot:
    """Test cases for get_latest_snapshot function"""
    
    @pytest.mark.external
    @patch('copy_from_tdr_to_gcs_hca.requests.get')
    def test_get_latest_snapshot_success(self, mock_get):
        """Test successful snapshot retrieval"""
        mock_response = Mock()
        mock_response.json.return_value = {
            'items': [{'id': 'snapshot-123'}]
        }
        mock_get.return_value = mock_response
        
        result = get_latest_snapshot("test-snapshot", "test-token")
        assert result == "snapshot-123"
    
    @patch('copy_from_tdr_to_gcs_hca.requests.get')
    def test_get_latest_snapshot_http_error(self, mock_get):
        """Test snapshot retrieval with HTTP error"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception, match="HTTP Error"):
            get_latest_snapshot("test-snapshot", "test-token")


class TestGetAccessUrls:
    """Test cases for get_access_urls function"""
    
    @patch('copy_from_tdr_to_gcs_hca.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_get_access_urls_success(self, mock_file, mock_get):
        """Test successful access URL retrieval"""
        # Mock response for first request
        mock_response1 = Mock()
        mock_response1.json.return_value = [
            {'fileDetail': {'accessUrl': 'gs://bucket/file1.txt'}},
            {'fileDetail': {'accessUrl': 'gs://bucket/file2.txt'}}
        ]
        
        # Mock response for second request (empty to end loop)
        mock_response2 = Mock()
        mock_response2.json.return_value = []
        
        mock_get.side_effect = [mock_response1, mock_response2]
        
        result = get_access_urls("test-snapshot", "test-token")
        
        assert result == ['gs://bucket/file1.txt', 'gs://bucket/file2.txt']
        assert mock_file.call_count == 2  # Two files written
    
    @patch('copy_from_tdr_to_gcs_hca.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_get_access_urls_with_basename(self, mock_file, mock_get):
        """Test access URL retrieval with custom output basename"""
        # Mock response
        mock_response1 = Mock()
        mock_response1.json.return_value = [
            {'fileDetail': {'accessUrl': 'gs://bucket/file1.txt'}}
        ]
        
        mock_response2 = Mock()
        mock_response2.json.return_value = []
        
        mock_get.side_effect = [mock_response1, mock_response2]
        
        result = get_access_urls("test-snapshot", "test-token", "test-basename-062425-1400")
        
        assert result == ['gs://bucket/file1.txt']
        # Should write to files with custom basename
        mock_file.assert_any_call('test-basename-062425-1400_access_urls.txt', 'w')
        mock_file.assert_any_call('test-basename-062425-1400_access_urls_filenames_sorted.txt', 'w')


class TestCheckSingleStagingArea:
    """Test cases for check_single_staging_area function"""
    
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_check_empty_staging_area(self, mock_run):
        """Test checking empty staging area"""
        mock_result = Mock()
        mock_result.stdout = b''
        mock_run.return_value = mock_result
        
        result = check_single_staging_area("gs://bucket/project-id")
        assert result == {}
    
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_check_nonempty_staging_area(self, mock_run):
        """Test checking non-empty staging area"""
        mock_result = Mock()
        mock_result.stdout = b'gs://bucket/project-id/data/file1.txt\ngs://bucket/project-id/data/file2.txt'
        mock_run.return_value = mock_result
        
        result = check_single_staging_area("gs://bucket/project-id")
        
        assert 'project_id' in result
        assert 'staging_dir' in result
        assert 'files' in result
        assert result['project_id'] == 'project-id'


class TestCopySingleFile:
    """Test cases for copy_single_file function"""
    
    @patch('copy_from_tdr_to_gcs_hca.verify_file_integrity')
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_copy_single_file_success(self, mock_run, mock_verify):
        """Test successful file copy"""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        mock_verify.return_value = True
        
        result = copy_single_file(
            "gs://source/file.txt", 
            "gs://dest/", 
            "project-123", 
            verify_integrity=True
        )
        
        assert result['status'] == 'success'
        assert result['filename'] == 'file.txt'
        assert result['project_id'] == 'project-123'
    
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_copy_single_file_failure(self, mock_run):
        """Test failed file copy"""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr.decode.return_value = "Copy failed"
        mock_run.return_value = mock_result
        
        result = copy_single_file(
            "gs://source/file.txt", 
            "gs://dest/", 
            "project-123", 
            verify_integrity=False
        )
        
        assert result['status'] == 'failed'
        assert result['error'] == 'Copy failed'
    
    @patch('copy_from_tdr_to_gcs_hca.verify_file_integrity')
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_copy_single_file_integrity_failure(self, mock_run, mock_verify):
        """Test file copy with integrity verification failure"""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_run.return_value = mock_result
        mock_verify.return_value = False
        
        result = copy_single_file(
            "gs://source/file.txt", 
            "gs://dest/", 
            "project-123", 
            verify_integrity=True
        )
        
        assert result['status'] == 'failed'
        assert 'integrity verification failed' in result['error'].lower()


class TestCompareChecksums:
    """Test cases for compare_checksums function"""
    
    def test_compare_md5_checksums_match(self):
        """Test MD5 checksum comparison - match"""
        source_stat = "Hash (md5): abc123def456\nOther info"
        dest_stat = "Hash (md5): abc123def456\nOther info"
        
        result = compare_checksums(source_stat, dest_stat)
        assert result is True
    
    def test_compare_md5_checksums_mismatch(self):
        """Test MD5 checksum comparison - mismatch"""
        source_stat = "Hash (md5): abc123def456\nOther info"
        dest_stat = "Hash (md5): different123\nOther info"
        
        result = compare_checksums(source_stat, dest_stat)
        assert result is False
    
    def test_compare_crc32c_checksums_match(self):
        """Test CRC32C checksum comparison - match"""
        source_stat = "Hash (crc32c): xyz789uvw012\nOther info"
        dest_stat = "Hash (crc32c): xyz789uvw012\nOther info"
        
        result = compare_checksums(source_stat, dest_stat)
        assert result is True
    
    def test_compare_no_matching_checksums(self):
        """Test comparison with no matching checksums"""
        source_stat = "Hash (md5): abc123def456\nOther info"
        dest_stat = "Hash (crc32c): xyz789uvw012\nOther info"
        
        result = compare_checksums(source_stat, dest_stat)
        assert result is False
    
    def test_compare_checksums_exception(self):
        """Test checksum comparison with exception"""
        # Invalid input that will cause an exception
        result = compare_checksums(None, "valid string")
        assert result is False


class TestVerifyFileIntegrity:
    """Test cases for verify_file_integrity function"""
    
    @patch('copy_from_tdr_to_gcs_hca.compare_checksums')
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_verify_file_integrity_success(self, mock_run, mock_compare):
        """Test successful file integrity verification"""
        # Mock successful gsutil stat calls
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Hash (md5): abc123\n"
        mock_run.return_value = mock_result
        mock_compare.return_value = True
        
        result = verify_file_integrity("gs://source/file.txt", "gs://dest/file.txt")
        assert result is True
    
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_verify_file_integrity_source_error(self, mock_run):
        """Test integrity verification with source file error"""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "File not found"
        mock_run.return_value = mock_result
        
        result = verify_file_integrity("gs://source/file.txt", "gs://dest/file.txt")
        assert result is False
    
    @patch('copy_from_tdr_to_gcs_hca.subprocess.run')
    def test_verify_file_integrity_exception(self, mock_run):
        """Test integrity verification with exception"""
        mock_run.side_effect = Exception("Subprocess error")
        
        result = verify_file_integrity("gs://source/file.txt", "gs://dest/file.txt")
        assert result is False


class TestWriteOutputFiles:
    """Test cases for _write_output_files function"""
    
    @patch('builtins.open', new_callable=mock_open)
    def test_write_output_files_with_failures(self, mock_file):
        """Test writing output files with failed URLs"""
        failed_urls = [
            {'project_id': 'proj1', 'url': 'gs://bucket/file1.txt', 'error': 'Error 1'},
            {'project_id': 'proj2', 'url': 'gs://bucket/file2.txt', 'error': 'Error 2'}
        ]
        integrity_failed = []
        access_urls = []
        
        _write_output_files(failed_urls, integrity_failed, access_urls)
        
        # Verify that files were opened for writing
        assert mock_file.called
    
    @patch('builtins.open', new_callable=mock_open)
    def test_write_output_files_with_basename(self, mock_file):
        """Test writing output files with custom basename"""
        failed_urls = [
            {'project_id': 'proj1', 'url': 'gs://bucket/file1.txt', 'error': 'Error 1'}
        ]
        integrity_failed = []
        access_urls = []
        
        _write_output_files(failed_urls, integrity_failed, access_urls, "test-basename-062425-1400")
        
        # Verify that files were opened with custom basename
        mock_file.assert_called_with('test-basename-062425-1400_failed_access_urls.txt', 'w')
    
    @patch('builtins.open', new_callable=mock_open)
    def test_write_output_files_with_integrity_failures(self, mock_file):
        """Test writing output files with integrity failures"""
        failed_urls = []
        integrity_failed = [
            {'project_id': 'proj1', 'bucket': 'bucket1', 'url': 'gs://bucket1/file1.txt'},
            {'project_id': 'proj2', 'bucket': 'bucket2', 'url': 'gs://bucket2/file2.txt'}
        ]
        access_urls = []
        
        _write_output_files(failed_urls, integrity_failed, access_urls)
        
        # Verify that files were opened for writing
        assert mock_file.called
    
    @patch('builtins.open', new_callable=mock_open)
    def test_write_output_files_with_access_urls(self, mock_file):
        """Test writing output files with access URLs"""
        failed_urls = []
        integrity_failed = []
        access_urls = [
            {'project_id': 'proj1', 'bucket': 'bucket1', 'url': 'gs://bucket1/file1.txt'},
            {'project_id': 'proj2', 'bucket': 'bucket2', 'url': 'gs://bucket2/file2.txt'}
        ]
        
        _write_output_files(failed_urls, integrity_failed, access_urls)
        
        # Verify that files were opened for writing
        assert mock_file.called


class TestCheckStagingAreasBatch:
    """Test cases for check_staging_areas_batch function"""
    
    @patch('copy_from_tdr_to_gcs_hca.check_single_staging_area')
    def test_check_staging_areas_all_empty(self, mock_check_single):
        """Test checking staging areas when all are empty"""
        mock_check_single.return_value = {}
        
        staging_paths = {"gs://bucket/proj1", "gs://bucket/proj2"}
        result = check_staging_areas_batch(staging_paths)
        
        assert len(result) == 2
        for path in staging_paths:
            assert path in result
    
    @patch('builtins.input')
    @patch('builtins.open', new_callable=mock_open)
    @patch('copy_from_tdr_to_gcs_hca.check_single_staging_area')
    def test_check_staging_areas_with_override_yes(self, mock_check_single, mock_file, mock_input):
        """Test checking staging areas with non-empty areas and user allows override"""
        mock_check_single.return_value = {
            'project_id': 'proj1',
            'staging_dir': 'gs://bucket/proj1/data/',
            'files': ['file1.txt']
        }
        mock_input.return_value = 'y'
        
        staging_paths = {"gs://bucket/proj1"}
        result = check_staging_areas_batch(staging_paths, allow_override=True)
        
        assert len(result) == 1
    
    @patch('builtins.input')
    @patch('builtins.open', new_callable=mock_open)
    @patch('copy_from_tdr_to_gcs_hca.check_single_staging_area')
    def test_check_staging_areas_with_override_no(self, mock_check_single, mock_file, mock_input):
        """Test checking staging areas with non-empty areas and user denies override"""
        mock_check_single.return_value = {
            'project_id': 'proj1',
            'staging_dir': 'gs://bucket/proj1/data/',
            'files': ['file1.txt']
        }
        mock_input.return_value = 'n'
        
        staging_paths = {"gs://bucket/proj1"}
        
        with pytest.raises(SystemExit):
            check_staging_areas_batch(staging_paths, allow_override=True)


class TestCopyFilesParallel:
    """Test cases for copy_files_parallel function"""
    
    @patch('copy_from_tdr_to_gcs_hca.copy_single_file')
    def test_copy_files_parallel_all_success(self, mock_copy_single):
        """Test parallel file copying with all successes"""
        mock_copy_single.return_value = {
            'status': 'success',
            'filename': 'test.txt',
            'project_id': 'proj1'
        }
        
        access_urls = ['gs://bucket/file1.txt', 'gs://bucket/file2.txt']
        successful, failed = copy_files_parallel(
            access_urls, 'gs://dest/', 'proj1', max_workers=2
        )
        
        assert len(successful) == 2
        assert len(failed) == 0
    
    @patch('copy_from_tdr_to_gcs_hca.copy_single_file')
    def test_copy_files_parallel_mixed_results(self, mock_copy_single):
        """Test parallel file copying with mixed results"""
        def side_effect(url, *args, **kwargs):
            if 'file1' in url:
                return {'status': 'success', 'filename': 'file1.txt', 'project_id': 'proj1'}
            else:
                return {'status': 'failed', 'url': url, 'project_id': 'proj1', 'error': 'Failed'}
        
        mock_copy_single.side_effect = side_effect
        
        access_urls = ['gs://bucket/file1.txt', 'gs://bucket/file2.txt']
        successful, failed = copy_files_parallel(
            access_urls, 'gs://dest/', 'proj1', max_workers=2
        )
        
        assert len(successful) == 1
        assert len(failed) == 1


# Integration tests
class TestIntegration:
    """Integration tests for the script"""
    
    @pytest.mark.integration
    def test_staging_area_buckets_config(self):
        """Test that staging area buckets configuration is valid"""
        assert 'prod' in STAGING_AREA_BUCKETS
        assert 'dev' in STAGING_AREA_BUCKETS
        
        for env, buckets in STAGING_AREA_BUCKETS.items():
            for institution, bucket_path in buckets.items():
                assert bucket_path.startswith('gs://')
                assert institution.isupper()


# Fixtures
@pytest.fixture
def sample_csv_content():
    """Fixture providing sample CSV content"""
    return "EBI,12345678-1234-4abc-8def-123456789012\nUCSC,87654321-4321-4cba-fedc-210987654321\n"


@pytest.fixture
def mock_gsutil_stat_output():
    """Fixture providing mock gsutil stat output"""
    return """URL:         gs://bucket/file.txt
Creation time:   Mon, 01 Jan 2024 12:00:00 GMT
Update time:     Mon, 01 Jan 2024 12:00:00 GMT
Storage class:   STANDARD
Content-Length:  1024
Content-Type:    text/plain
Hash (crc32c):   AAAAAA==
Hash (md5):      abc123def456ghi789
ETag:            abc123
Generation:      1234567890
Metageneration:  1
"""


if __name__ == "__main__":
    pytest.main([__file__])
