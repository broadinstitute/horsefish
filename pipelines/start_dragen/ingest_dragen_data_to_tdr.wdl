version 1.0

workflow ingest_dragen_data_to_tdr {
    input {
        String  sample_set
        String  rp
        String  target_table_name
        String?  docker_name
    }

    String docker = select_first([docker_name, "us-central1-docker.pkg.dev/dsp-cloud-dragen-stanley/wdl-images/parse_dragen_metrics:v1"])

    call create_ingest_tsv {
        input:
            sample_set = sample_set,
            docker_name = docker
    }

    call ingest_to_tdr {
        input:
            ingest_tsv = create_ingest_tsv.output_file,
            rp = rp,
            target_table_name = target_table_name,
            docker_name = docker
    }

    output {
        File ingest_tsv = create_ingest_tsv.output_file
    }
}

task ingest_to_tdr {
        input {
            File    ingest_tsv
            String  rp
            String  target_table_name
            String  docker_name
        }

        command {

            python3 /scripts/ingest_to_tdr_stanley.py --rp ~{rp} \
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
        String  docker_name
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