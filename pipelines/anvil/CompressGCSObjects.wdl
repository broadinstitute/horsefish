version 1.0

workflow CompressGCSObjects {
    input {
        String  gcp_project
        String  bq_dataset
        String  bq_dataset_table

        String  tar_object
        Array[String]   uncompressed_objects
    }

    # call QueryUncompressedObjects {
    #     input:
    #         gcp_project         =   gcp_project,
    #         bq_dataset          =   bq_dataset,
    #         bq_dataset_table    =   bq_dataset_table,
    #         tar_object          =   tar_object
    # }

    call CompressObjects {
        input:
            tar_object              =   tar_object,
            uncompressed_objects    =   uncompressed_objects
            # uncompressed_objects    =   QueryUncompressedObjects.uncompressed_objects
    }

    output {
        # File    uncompressed_objects    =   QueryUncompressedObjects.uncompressed_objects_tsv
        File    copy_logs               =   CompressObjects.copy_log
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
        Array[String]   uncompressed_objects          = read_lines("uncompressed_objects_to_compress.tsv")
    }
}

task CompressObjects {
    input {
        String      tar_object
        Array[File] uncompressed_objects

        String      docker = "broadinstitute/horsefish:compress_objects_v1"
    }

    String  tar_gz_filename =   basename(tar_object)

    command <<<

        zip_dir=$(echo ~{tar_object} | tr "/" "\t" | awk "{print $2}")
        echo $zip_dir
        # compress objects that are localized to /cromwell_root
        tar -vczf ~{tar_gz_filename} -C /cromwell_root/schaluva-bucket/ .

        # copy the compressed object to its final destination
        # gcloud storage cp ~{tar_gz_filename} ~{tar_object}
        gsutil cp -c -L copy_from_local_log.csv ~{tar_gz_filename} ~{tar_object}

    >>>

    runtime {
        docker: docker
        disks: "local-disk 250 SSD"
    }

    output {
        File    copy_log    = "copy_from_local_log.csv"
    }
}