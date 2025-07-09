"""
TDR to GCS HCA Copy Module

This module provides functionality for copying data from Terra Data Repository (TDR) 
HCA project buckets to HCA staging area buckets.

Main components:
- copy_from_tdr_to_gcs_hca.py: Main script for copying files
- compare_files_in_tdr_to_files_in_staging.py: Utility for comparing file lists

Usage:
    python -m src.copy_from_tdr_to_gcs_hca config/manifests/manifest.csv --env prod
"""

__version__ = "2.0.0"
__author__ = "Barbara A Hill, Samantha Velasquez"
