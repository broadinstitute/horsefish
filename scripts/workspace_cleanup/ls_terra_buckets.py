"""ls_terra_buckets.py.

for a list of Terra buckets (csv, either from data warehouse query or `find_terra_buckets.py`),
perform a recursive listing of files and retrieve name, md5 hash, and file size.
export as csv to be uploaded to BigQuery.

to make `terra_buckets_prod.csv`, which must have columns for bucket_name, billing_account_id, project, terra_env
can use this query in data warehouse (requires firecloud.org login):

SELECT namespace AS project, name AS workspace_name, bucket_name, envs.billing_account_id, envs.terra_env, rawls.created_date
FROM `broad-dsde-prod-analytics-dev.warehouse.rawls_workspaces` AS rawls
JOIN `dsp-cloud-billing-analysis.cloud_billing_analysis.terra_project_envs` AS envs
ON rawls.namespace = envs.project_id
WHERE envs.terra_env = 'prod'
AND rawls.created_date <= "2020-03-25"

"""

import subprocess
import csv
import pandas as pd
import time
from tqdm import tqdm

from google.cloud import bigquery
from google.cloud import storage


def list_blobs(path_set, bucket_name, storage_client):
    """Lists all the blobs in the bucket, recursively, and returns md5 and file size info."""
    print("\n\nlisting contents of bucket gs://" + bucket_name)
    start = time.time()
    # check if bucket exists

    try:
        bucket = storage_client.get_bucket(bucket_name)
    except:
        print('Bucket does not exist!')
        return

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        filename = blob.name

        if not filename.endswith('/'):  # if this is not a directory

            md5 = blob.md5_hash
            size = blob.size

            info = f'{filename} {str(md5)} {str(size)}'
            # print(info)

            path_set.add(info)

    end = time.time()
    print(f'-------------------------------------------> {str(len(path_set))} files found in {end - start:.3f} seconds')


def run_subprocess(cmd, errorMessage):
    if isinstance(cmd, list):
        cmd = ' '.join(cmd)
    try:
        # print("running command: " + cmd)
        return subprocess.check_output(
            cmd, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        print(errorMessage)
        print("Exited with " + str(e.returncode) + "-" + e.output)
        exit(1)


def list_contents_with_details(path_set, path):
    print("(" + str(len(path_set)) + " files added so far.)")
    print("listing contents of " + path)

    # start = time.time()
    files = run_subprocess("gsutil ls -L \"" + path + "\"", "Unable to list bucket directory").split("gs://")
    # end = time.time()
    # print(f'gsutil ls -L took {end - start:.3f} seconds\n')

    for item in files:

        if len(item) > 0:
            filename = 'gs://' + item.split('\n')[0].split(':')[0]
            # print(filename)

            if not filename.endswith('/'):  # if this is not a directory

                md5 = None
                size = None

                for line in item.split('\n'):
                    if 'Hash (md5):' in line:
                        md5 = line.split(' ')[-1].rstrip('\n')
                    if 'Content-Length:' in line:
                        size = line.split(' ')[-1].rstrip('\n')
                    if md5 and size:
                        break  # don't keep going if you've found both

                info = f'{filename} {str(md5)} {str(size)}'
                # print(info)

                path_set.add(info)

            else:  # otherwise it's a directory, so list its contents
                if filename != path:  # ignore the folder you're listing originally
                    list_contents_with_details(path_set, filename)


# # test how much faster this shit is!!!
# # test_bucket = 'fc-0445daab-a67d-4df8-af28-26c73e7fd962' # has 2 files
# # test_bucket = 'fc-a552bd2f-8ace-4685-875c-28dbc36ba1c5' # has a gazillion files - takes 170 sec
# test_bucket = 'fc-6e8897cd-18e3-4a1e-990f-5cbbae8d0170' # has a file with no md5

# storage_client = storage.Client()
# # test with storage client
# print('trying storage client')
# start = time.time()
# path_set_1 = set()
# list_blobs(path_set_1, test_bucket, storage_client)
# print(len(path_set_1))
# end = time.time()
# print(f'storage client took {end - start:.5f} seconds\n')

# for item in path_set_1:
#     if 'phg001280.v1.TOPMed_WGS_Amish_v4.genotype-calls-vcf.WGS_markerset_grc38.c2.HMB-IRB-MDS.tar.gz.part006086' in item:
#         print(item)
#         items = item.split(' ') # item stored as string 'path md5 size' e.g. 'gs://dfe-32-bucket/cromwell-drs-localizer-49-37e4a11-SNAP.jar SeWbMkwn6lR/c6wx//nORw== 57808764'
#         path = items[0]
#         md5 = items[1]
#         size = items[2]

# exit(1)

# # test with gsutil
# print('trying gsutil')
# start = time.time()
# path_set_2 = set()
# list_contents_with_details(path_set_2, f'gs://{test_bucket}')
# print(len(path_set_2))
# end = time.time()
# print(f'gsutil took {end - start:.3f} seconds\n')

# exit(1)


def generate_manifest(main_bucket, storage_client, billing_acct=None, project=None, terra_env=None):
    # get set of paths in bucket
    path_set = set()
    list_blobs(path_set, main_bucket, storage_client)
    # list_contents_with_details(path_set, 'gs://'+ main_bucket)

    # write to csv
    csv_file = main_bucket + '_manifest.csv'
    headers = ['billing_account', 'project', 'env', 'bucket', 'path', 'file_name', 'extension', 'md5', 'size']

    with open(csv_file, 'w') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',', quotechar='"',
                                quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(headers)
        # if you found things inside the bucket, write them to the csv
        if len(path_set) > 0:
            print("Writing rows...")
            for item in path_set:
                items = item.split(' ')  # item stored as string 'path md5 size' e.g. 'dfe-32-bucket/cromwell-drs-localizer-49-37e4a11-SNAP.jar SeWbMkwn6lR/c6wx//nORw== 57808764'
                path = 'gs://' + items[0]
                assert(item.split('/')[0] == main_bucket)
                md5 = items[1]
                size = items[2]
                file_name = path.split('/')[-1]
                extension = 'N/A' if '.' not in file_name else file_name.split('.')[-1]
                # headers = ['billing_account','project','env','bucket','path','file_name','extension','md5','size']
                row = [billing_acct, project, terra_env, main_bucket, path, file_name, extension, md5, size]
                csv_writer.writerow(row)

        else:  # if the bucket was empty, write a 1-line csv with nulls
            csv_writer.writerow([billing_acct, project, terra_env, main_bucket, None, None, None, None, None])

    return csv_file


def upload_to_bq(csv_bucket, bq_table_ref):
    # set up job config
    job_config = bigquery.LoadJobConfig()
    # job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # this overwrites the contents of the table
    job_config.skip_leading_rows = 1

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    # uri = f'gs://{csv_bucket}'
    load_job = bq_client.load_table_from_uri(
        csv_bucket, bq_table_ref, job_config=job_config
    )  # API request
    # print(f"Starting job {load_job.job_id}")

    load_job.result()  # Waits for table load to complete.
    # print("Job finished.")

    destination_table = bq_client.get_table(bq_table_ref)
    print(f"BigQuery: upload complete to {destination_table.table_id}")


def main(df_prod, staging_bucket_path, bq_table_ref, storage_client, finished_buckets=[]):

    for i in tqdm(range(len(df_prod))):
        bucket = df_prod.bucket_name[i]

        if bucket not in finished_buckets:  # check if bucket has already been queried
            billing_acct = df_prod.billing_account_id[i]
            project = df_prod.project[i]
            terra_env = df_prod.terra_env[i]

            # make the manifest
            csv_local = generate_manifest(bucket, storage_client, billing_acct, project, terra_env)

            if csv_local:  # generate_manifest returns None if the bucket is empty
                # upload csv to staging bucket
                cmd = f'gsutil cp {csv_local} {staging_bucket_path}'
                run_subprocess(cmd, f'Error uploading local file {csv_local} to bucket {staging_bucket_path}')

                # add rows to bigquery table
                csv_bucket = staging_bucket_path + '/' + csv_local
                upload_to_bq(csv_bucket, bq_table_ref)

                # remove local csv file
                cmd = f'rm {csv_local}'
                run_subprocess(cmd, f'Error removing local file {csv_local}')


if __name__ == '__main__':
    # main_bucket = sys.argv[1]

    # load dataframe that contains columns for bucket_name, billing_account_id, project, terra_env
    bucket_file = 'terra_buckets_prod.csv'
    df_prod = pd.read_csv(bucket_file)

    staging_bucket_path = 'gs://dfe-55-bucket-manifests'
    bq_project = 'dsp-cloud-billing-analysis'
    bq_dataset = 'cloud_billing_analysis'
    bq_table = 'bucket_manifests'

    # set up bigquery client
    bq_client = bigquery.Client(project=bq_project)
    bq_table_ref = bq_client.dataset(bq_dataset).table(bq_table)

    # set up storage client
    storage_client = storage.Client()

    # get a list of buckets that have already been queried
    query = f"""
SELECT DISTINCT bucket
FROM `{bq_project}.{bq_dataset}.{bq_table}`
WHERE bucket IS NOT NULL
"""
    query_job = bq_client.query(query)  # Make an API request.
    finished_buckets = [row[0] for row in query_job]

    # TEMP - these are huge buckets that take forever to ls
    # finished_buckets.append('fc-2738e399-0404-4b0b-a595-8005f8a76b0f')
    # finished_buckets.append('fc-b9d9c7d7-244e-4752-8e5c-a04782437a21')

    # for row in query_job:
    #     finished_buckets.append(row[0])

    # run the thing
    main(df_prod, staging_bucket_path, bq_table_ref, storage_client, finished_buckets)