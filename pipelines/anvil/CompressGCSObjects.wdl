version 1.0

workflow CompressGCSObjects {
    input {
        String  gcp_project
        String  bq_dataset
        String  bq_dataset_table

        String  tar_object
        # Array[String]   uncompressed_objects
    }

    call QueryUncompressedObjects {
        input:
            gcp_project         =   gcp_project,
            bq_dataset          =   bq_dataset,
            bq_dataset_table    =   bq_dataset_table,
            tar_object          =   tar_object
    }

    call CompressObjects {
        input:
            tar_object              =   tar_object,
            # uncompressed_objects    =   uncompressed_objects
            uncompressed_objects    =   QueryUncompressedObjects.uncompressed_objects
    }

    output {
        File    uncompressed_objects    =   QueryUncompressedObjects.uncompressed_objects_tsv
        File    copy_logs               =   CompressObjects.copy_log
        String  md5sum                  =   CompressObjects.md5
    }
}

task QueryUncompressedObjects {
    input {
        String  gcp_project
        String  bq_dataset
        String  bq_dataset_table
        String  tar_object

        String  docker = "broadinstitute/horsefish:compress_objects_v1"
    }

    command {
        python3 /scripts/compress_gcs_objects.py -g ~{gcp_project} \
                                        -d ~{bq_dataset} \
                                        -t ~{bq_dataset_table} \
                                        -z ~{tar_object}
    }

    runtime {
        docker: docker
    }

    output {
        File            uncompressed_objects_tsv    =   "uncompressed_objects_to_compress.tsv"
        Array[String]   uncompressed_objects        = read_lines("uncompressed_objects_to_compress.tsv")
    }
}

task CompressObjects {
    input {
        String      tar_object
        Array[File] uncompressed_objects


        Int         memory_gb
        Int         disk_size_gb
        String      docker = "gcr.io/google.com/cloudsdktool/google-cloud-cli:latest"
    }

    String  tar_gz_filename =   basename(tar_object)

    command <<<

        set -eo pipefail

        # gets the fc- bucket id only
        zip_dir=$(echo ~{tar_object} | tr '/' '\t' | awk '{ print $2 }')
        echo "ZIP DIR = ${zip_dir}"
        echo ${zip_dir}
  
        # compress objects that are localized to /cromwell_root/fc-/
        tar -vczf ~{tar_gz_filename} -C /cromwell_root/${zip_dir}/ .

        # copy the compressed object to its final destination
        # gsutil cp -c -L copy_from_local_log.csv ~{tar_gz_filename} ~{tar_object}
        gcloud config set storage/parallel_composite_upload_enabled False
        gcloud storage cp -L copy_from_local_log.csv ~{tar_gz_filename} ~{tar_object}


        # get the md5 of compressed object
        cat copy_from_local_log.csv | tail -1 | awk -F, '{print $5}' > tar_gz_file_md5sum

    >>>

    runtime {
        docker: docker
        disks: "local-disk " + disk_size_gb + " SSD"
        memory: memory_gb + " GiB"
    }

    output {
        File    copy_log    = "copy_from_local_log.csv"
        String  md5         = read_string("tar_gz_file_md5sum")
    }
}