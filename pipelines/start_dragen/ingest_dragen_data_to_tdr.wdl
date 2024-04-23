version 1.0

workflow ingest_dragen_data_to_tdr {
    input {
        String  sample_set
        String  rp
        String  data_set_id
        String  target_table_name
    }

    call create_ingest_tsv {
        input:
            sample_set = sample_set
    }

    call ingest_to_tdr {
        input:
            ingest_tsv = create_ingest_tsv.output_file,
            data_set_id = data_set_id,
            target_table_name = target_table_name
    }

    output {
        File ingest_tsv = create_ingest_tsv.output_file
    }
}

task ingest_to_tdr {
        input {
            File    ingest_tsv
            String  data_set_id
            String  target_table_name
            String  docker_name = "gcr.io/emerge-production/emerge_wdls:v.1.0"
        }

        command {

            python3 /scripts/emerge/ingest_to_tdr.py --dataset_id ~{data_set_id} \
                                                 --target_table_name ~{target_table_name} \
                                                 --tsv ~{ingest_tsv}

        }

        runtime {
            docker: docker_name
        }

        output {
            File    ingest_logs = stdout()
        }
}

task create_ingest_tsv {

    input {
        String  sample_set
        String  docker_name = " us-central1-docker.pkg.dev/dsp-cloud-dragen-stanley/wdl-images/parse_dragen_metrics:v1"
    }

    command {

        python3 /scripts/get_tsv_for_ingest.py -s ~{sample_set} -o ingest.tsv

    }

    runtime {
        docker: docker_name
    }

    output {
        File output_file = "ingest.tsv"
    }
}