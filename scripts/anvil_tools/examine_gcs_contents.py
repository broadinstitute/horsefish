"""Outputs contents of up to two GCS paths, comparing if multiple paths are specified. 

Usage:
    > python3 examine_gcs_contents.py -s GCS_PATH_1 [-t GCS_PATH_2] """

# Imports
import os
import argparse
import datetime
import pandas as pd
import numpy as np
import re
import subprocess

# Function to parse gsutil ls -L contents into dict structure
def parse_ls_output(subprocess_output):
    records = []
    file_dict = {}
    for line in subprocess_output.split("\n"):
        if line[0:2] == "gs":
            if file_dict:
                records.append(file_dict)
            file_dict = {}
            if "/:" not in line:
                file_dict["GlobalPath"] = re.sub(":$", "", line)
        else:
            if file_dict:
                if "Content-Length:" in line:
                    m = re.match("\s*Content\-Length\:\s*([0-9]+)", line).group(1)
                    file_dict["Size"] = m
                elif "Hash (crc32c):" in line:
                    m = re.match("\s*Hash \(crc32c\):\s*(.*)", line).group(1)
                    file_dict["crc32c"] = m
                elif "Hash (md5):" in line:
                    m = re.match("\s*Hash \(md5\):\s*(.*)", line).group(1)
                    file_dict["md5"] = m
                elif "Update time:" in line:
                    m = re.match("\s*Update time:\s*(.*)", line).group(1)
                    file_dict["Modified"] = m
    if file_dict:
        records.append(file_dict)
    return records

# Function to compare contents of two specified GCS paths
def output_and_compare_contents(gcs_path_1, gcs_path_2):
    
    # Output and parse contents of gcs_path_1 directory, then structure in dataframe
    #!gsutil -u anvil-datastorage ls -L $gcs_path_1/** > output_1.txt
    cmd = f"gsutil -u anvil-datastorage ls -L {gcs_path_1}/**"
    output_1 = subprocess.check_output(cmd, shell=True, universal_newlines=True)
    records = parse_ls_output(output_1)
    df1 = pd.DataFrame(records)
    df1["Directory"] = gcs_path_1
    df1["Path"] = df1["GlobalPath"].str.replace(gcs_path_1, "")
    df1["Name"] = df1.apply(lambda x: os.path.basename(x["GlobalPath"]), axis=1)
    df1 = df1.fillna("")
    df1_final = df1[["Directory", "Path", "Name", "Size", "crc32c", "md5", "Modified"]].copy()
    df1_final = df1_final[df1_final["Name"] != ""].sort_values(by="Path")
    df1_final = df1_final.convert_dtypes()

    # Output and parse contents of gcs_path_2 directory, then structure in dataframe
    if gcs_path_2 != "UNSPECIFIED":
        #!gsutil -u anvil-datastorage ls -L $gcs_path_2/** > output_2.txt
        cmd = f"gsutil -u anvil-datastorage ls -L {gcs_path_2}/**"
        output_2 = subprocess.check_output(cmd, shell=True, universal_newlines=True)
        records = parse_ls_output(output_2)
        df2 = pd.DataFrame(records)
        df2["Directory"] = gcs_path_2
        df2["Path"] = df2["GlobalPath"].str.replace(gcs_path_2, "")
        df2["Name"] = df2.apply(lambda x: os.path.basename(x["GlobalPath"]), axis=1)
        df2 = df2.fillna("")
        df2_final = df2[["Directory", "Path", "Name", "Size", "crc32c", "md5", "Modified"]].copy()
        df2_final = df2_final[df2_final["Name"] != ""].sort_values(by="Path")
        df2_final = df2_final.convert_dtypes()

    # Join together dataframes and flag differences
    if gcs_path_2 != "UNSPECIFIED":
        df3 = pd.merge(df1_final, df2_final, suffixes=("_1", "_2"), how="outer", on="Path")
        df3 = df3.fillna("")
        df3["Mismatch_Flag"] = np.where((df3["Directory_1"] == "") | (df3["Directory_2"] == ""), "Y", "N")
        df3["Size_Diff_Flag"] = np.where((df3["Mismatch_Flag"] == "N") & (df3["Size_1"] != df3["Size_2"]), "Y", "N")
        df3["crc32c_Diff_Flag"] = np.where((df3["Mismatch_Flag"] == "N") & (df3["crc32c_1"] != df3["crc32c_2"]), "Y", "N")
        df3["md5_Diff_Flag"] = np.where((df3["Mismatch_Flag"] == "N") & (df3["md5_1"] != df3["md5_2"]), "Y", "N")
        df3["md5_Missing_Flag"] = np.where(((df3["Directory_1"] != "") & (df3["md5_1"] == "")) | ((df3["Directory_2"] != "") & (df3["md5_2"] == "")), "Y", "N")
        df3["Check"] = np.where((df3["Mismatch_Flag"] == "Y") | (df3["Size_Diff_Flag"] == "Y") | (df3["crc32c_Diff_Flag"] == "Y") | (df3["md5_Diff_Flag"] == "Y") | (df3["md5_Missing_Flag"] == "Y"), "Y", "N")
        df3_final = df3[["Path", "Check", "Mismatch_Flag", "Size_Diff_Flag", "crc32c_Diff_Flag", "md5_Diff_Flag", "md5_Missing_Flag", "Directory_1", "Name_1", "Size_1", "crc32c_1", "md5_1", "Modified_1", "Directory_2", "Name_2", "Size_2", "crc32c_2", "md5_2", "Modified_2"]].copy()
    else:
        df3_final = df1_final[["Path", "Directory", "Name", "Size", "crc32c", "md5", "Modified"]]

    # Write out dataframe to TSV
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    output_filename = f"gcs_path_contents_{timestamp}.tsv"
    df3_final.to_csv(output_filename, sep="\t", index=False)
    print(f"{output_filename} created successfully.")

# Main function
if __name__ == "__main__":

    # Set up argument parser
    parser = argparse.ArgumentParser(description="Compare contents of two GCS paths.")
    parser.add_argument("-s", "--source_path", required=True, type=str, help="First GCS path to output contents for.")
    parser.add_argument("-t", "--target_path", type=str, default="UNSPECIFIED", help="Optional second GCS path to output contents for, and compare to first GCS path.")
    args = parser.parse_args()

    # Call to output and compare GCS path contents
    output_and_compare_contents(args.source_path, args.target_path)
