version 1.0

workflow CompressGCSObjects {
    input {
        String  gcp_project
        String  bq_dataset
        String  bq_dataset_table

        String  tar_object
    }

    call QueryUncompressedObjects {
        input:
            gcp_project         =   gcp_project,
            bq_dataset          =   bq_dataset,
            bq_dataset_table    =   bq_dataset_table,
            tar_object          =   tar_object    }

    call CompressObjects {
        input:
            tar_object              =   tar_object,
            uncompressed_objects    =   QueryUncompressedObjects.compressed_objects
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
        python3 compress_gcs_objects.py -g ~{gcp_project} \
                                        -d ~{bq_dataset} \
                                        -t ~{bq_dataset_table} \
                                        -z ~{tar_object}
    }

    runtime {
        docker: docker
    }

    output {
        Array[String] compressed_objects = read_lines("uncompressed_objects_to_compress.tsv")
    }
}

task CompressObjects {
    input {
        String      tar_object
        Array[File] uncompressed_objects

        String      docker = "broadinstitute/horsefish:compress_objects_v1"
    }

    command <<<

        # get just final file name for compressed object
        outfile_name=$(echo $tar_object | tr '/' '\t' | awk '{print $NF}')

        # compress objects
        tar cvfz $outfile_name /cromwell_root

        # copy the compressed object to its final destination
        gsutil cp $outfile_name ~{tar_object}
    >>>

    runtime {
        docker: docker
    }
}