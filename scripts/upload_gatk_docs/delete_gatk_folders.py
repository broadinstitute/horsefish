"""Delete GATK `Clean` and `Update` Folders and contents."""
import os
import shutil
import argparse


def delete_folder(dir_path):
    """Delete Folder and contents."""
    if os.path.isdir(dir_path):
        try:
            shutil.rmtree(dir_path)
            print(f" Deleted {dir_path}")
        except OSError as e:
            print(f"Error: {dir_path} : {e.strerror}")
    else:
        print(f"Error: {dir_path} : Path Doesn't Exist")


# Main Function
if __name__ == "__main__":
    # Get GATK version and docs file path arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--gatk_version', '-V', required=True, type=str, help='The Gatk Version Number')
    parser.add_argument('--gatkdoc_clean_path', '-C', default=None, required=False, type=str, help='The GATK Clean Docs Path /gatkdoc_clean_<version>')
    parser.add_argument('--gatkdoc_update_path', '-U', default=None, required=False, type=str, help='The GATK Update Docs Path /gatkdoc_update_<version>')
    args = parser.parse_args()

    # GATK version
    gatk_version = args.gatk_version

    # GATK Clean Docs Path
    if args.gatkdoc_clean_path:
        gatkdoc_clean_path = args.gatkdoc_clean_path
    else:
        gatkdoc_clean_path = "gatkdoc_clean_" + gatk_version

    # GATK Updated Docs Path
    if args.gatkdoc_update_path:
        gatkdoc_update_path = args.gatkdoc_update_path
    else:
        gatkdoc_update_path = "gatkdoc_update_" + gatk_version

    # Delete GATK Clean Docs
    delete_folder(gatkdoc_clean_path)

    # Delete GATK Updated Docs
    delete_folder(gatkdoc_update_path)
