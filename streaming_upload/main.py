from firecloud import api as fapi

namespace = "broad-firecloud-dsde"
workspace = "streaming_upload"
entities_tsv = "streaming_upload/flowcell.tsv"
# print(entities_tsv)

# res = fapi.upload_entities_tsv(namespace, workspace, entities_tsv, model='firecloud')
# print(res.json())

# SampleSheet.csv:
# - gs://fc-d557463b-6592-4f3c-af26-f4d758536b62/201112_M04004_0509_000000000-DB53C/SampleSheet.csv
# RunInfo.xml:
# - gs://fc-d557463b-6592-4f3c-af26-f4d758536b62/201112_M04004_0509_000000000-DB53C/RunInfo.xml
# Incremental tarballs:
# - gs://fc-d557463b-6592-4f3c-af26-f4d758536b62/201112_M04004_0509_000000000-DB53C/run.201112_M04004_0509_000000000-DB53C.lane.all_000.tar.gz
# - gs://fc-d557463b-6592-4f3c-af26-f4d758536b62/201112_M04004_0509_000000000-DB53C/run.201112_M04004_0509_000000000-DB53C.lane.all_001.tar.gz
# Monolithic tarball made by extracing incremental tarballs and re-packing:
# - gs://fc-d557463b-6592-4f3c-af26-f4d758536b62/201112_M04004_0509_000000000-DB53C/201112_M04004_0509_000000000-DB53C.tar.gz (edited) 


python streaming_upload/monitor_runs.py -c streaming_upload/monitor_runs.config -p "fc-156ce5ce-ac56-40ee-adcc-71ad6dc9d351" -d streaming_upload/miseq -w "merge_tar_chunks" -v 