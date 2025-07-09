from google.cloud import storage

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Downloaded {source_blob_name} to {destination_file_name}.")

if __name__ == "__main__":
    bucket_name = "fc-748030e6-9937-4bfb-9ca2-b94f9a8267fc"
    source_blob_name = "Synapse_seq_v1_data.pkl"
    destination_file_name = "Synapse_seq_v1_data.pkl"  # Local path

    download_blob(bucket_name, source_blob_name, destination_file_name)