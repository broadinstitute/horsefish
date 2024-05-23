version 1.0

workflow IngestDragenWorkflow {
    input {
        Array[String] sample_ids
        String        sample_set_id
        String        bucket_id
        String        namespace
        String        workspace
    }

    call create_ingest_tsv {
        input:
            sample_ids = sample_ids,
            sample_set_id = sample_set_id,
            bucket_id   = bucket_id
    }

    call ingest_to_workspace {
        input:
            ingest_tsv = create_ingest_tsv.output_file,
            namespace = namespace,
            workspace = workspace
    }

    output {
        File ingest_tsv = create_ingest_tsv.output_file
    }
}

task create_ingest_tsv {

    input {
        Array[String] sample_ids
        String        sample_set_id
        String        bucket_id
    }

    command {
        # write gvcf index + gvcf paths from output bucket based on sample_set_id and sample_id to a TSV
        # paths are of the form:

        # gs://<workspace_bucket_id>/<sample_set_id>/<sample_id>/<run_date>/<sample_id>.hard-filtered.gvcf.gz
        # gs://<workspace_bucket_id>/<sample_set_id>/<sample_id>/<run_date>/<sample_id>.hard-filtered.gvcf.gz.tbi

        # generate header
        echo "entity:sample_id	gvcf_index_path	gvcf_path" >> ingest.tsv

        # write values to file in newline delimited format
        echo "~{sep='\t' sample_ids}"   > sample_ids.tsv

        # call python script:
        python3 generate_ingest_tsv.py -b ~{bucket_id} -s ~{sample_set_id} -f sample_ids.tsv
      }

      runtime {
        docker: "us-central1-docker.pkg.dev/gp-cloud-dragen-prod/wdl-images/terra-workspace-ingest-img:latest"
      }


    output {
        File output_file = "ingest.tsv"
    }
}

task ingest_to_workspace {

    input {
        File    ingest_tsv
        String  namespace
        String  workspace
    }

    command <<<

        python3 upload_ingest_tsv.py -p ~{namespace} -w ~{workspace} -t ~{ingest_tsv}
                                                                                                                                                                                                                                                                                                                    }
        >>>

    runtime {
        docker: "us-central1-docker.pkg.dev/gp-cloud-dragen-prod/wdl-images/terra-workspace-ingest-img:latest"
    }

    output {
        File    ingest_logs = stdout()
    }
}