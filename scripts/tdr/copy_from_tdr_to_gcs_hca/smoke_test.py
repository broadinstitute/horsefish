#!/usr/bin/env python3
"""
Quick smoke test for copy_from_tdr_to_gcs_hca.py
This script performs basic import and functionality tests.
"""

import sys
import os

# Add the current directory to the path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_imports():
    """Test that all required functions can be imported"""
    try:
        from copy_from_tdr_to_gcs_hca import (
            validate_input,
            find_project_id_in_str,
            _sanitize_staging_gs_path,
            _parse_csv,
            STAGING_AREA_BUCKETS
        )
        print("✓ All imports successful")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_basic_functions():
    """Test basic functionality of core functions"""
    try:
        from copy_from_tdr_to_gcs_hca import find_project_id_in_str, _sanitize_staging_gs_path
        
        # Test UUID extraction
        test_uuid = "12345678-1234-4abc-8def-123456789012"
        result = find_project_id_in_str(f"project-{test_uuid}")
        assert result == test_uuid, f"Expected {test_uuid}, got {result}"
        print("✓ UUID extraction works")
        
        # Test path sanitization
        result = _sanitize_staging_gs_path("  gs://bucket/path/  ")
        expected = "gs://bucket/path"
        assert result == expected, f"Expected {expected}, got {result}"
        print("✓ Path sanitization works")
        
        return True
    except Exception as e:
        print(f"✗ Basic function test failed: {e}")
        return False

def test_configuration():
    """Test that configuration is valid"""
    try:
        from copy_from_tdr_to_gcs_hca import STAGING_AREA_BUCKETS
        
        # Check that both environments exist
        assert 'prod' in STAGING_AREA_BUCKETS, "Missing 'prod' environment"
        assert 'dev' in STAGING_AREA_BUCKETS, "Missing 'dev' environment"
        
        # Check that buckets are properly formatted
        for env, buckets in STAGING_AREA_BUCKETS.items():
            for institution, bucket_path in buckets.items():
                assert bucket_path.startswith('gs://'), f"Invalid bucket path: {bucket_path}"
                assert institution.isupper(), f"Institution should be uppercase: {institution}"
        
        print("✓ Configuration is valid")
        return True
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        return False

def main():
    """Run all smoke tests"""
    print("Running smoke tests for copy_from_tdr_to_gcs_hca.py")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_basic_functions,
        test_configuration
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
        print()
    
    print(f"Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 All smoke tests passed!")
        return 0
    else:
        print("❌ Some smoke tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
