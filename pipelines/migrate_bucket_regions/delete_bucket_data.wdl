version 1.0

workflow delete_bucket_objects {
    input {
        File   source_bucket_details
        File   copy_from_local_log
    }

    call delete_migrated_data_from_source {
        input:
            source_bucket_details = source_bucket_details,
            copy_from_local_log = copy_from_local_log
    }

    output {
        File objects_deleted = delete_migrated_data_from_source.deleted_files
    }
}

task delete_migrated_data_from_source {
    input {
        File source_bucket_details
        File copy_from_local_log
    }

    command {

        awk NR\>1  ~{copy_from_local_log} | tr "," "\t" | \  # remove header, set tab delimiter
            awk -F"\t" '{$2 = substr($2, 46); print "gs://"$2, $9}' OFS="\t" | \  # get obj dest path, parse out extra folder
            awk 'NR==FNR{a[$1]=$0;next}{$3=a[$2]}1' OFS="\t" - ~{source_bucket_details} | \  # match with input source details file
            awk -F"\t" '$4 == "OK"' | cut -f2 > objects_to_delete.txt  # only get rows where copy was successful ( = "OK")

        cat objects_to_delete.txt | gsutil rm -
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        memory: "2 GB"
        disks: "local-disk 10 HDD"
        zones: "us-central1-c us-central1-b"
    }

    output {
        deleted_files = "objects_to_delete.txt"
    }
}