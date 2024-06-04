version 1.0


workflow ingest_dragen_data_to_tdr {
    input {
        String   sample_set
        String   rp
        String   target_table_name
        String?  docker_name
        # Only use if not going to one of standard RP data sets
        String?  data_set_id
        # Use if you want bulk mode used for ingest
        Boolean  bulk_mode
        Boolean  filter_entity_already_in_dataset = false
        Int      batch_size = 100
        Int      max_retries = 5
        Int      max_backoff_time = 300
        Int      waiting_time_to_poll = 60
        String   update_strategy = "append" # append or merge
    }

    String  docker = select_first([docker_name, "us-central1-docker.pkg.dev/dsp-cloud-dragen-stanley/wdl-images/parse_dragen_metrics:v1"])

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
            docker_name = docker,
            update_strategy = update_strategy,
            data_set_id = data_set_id,
            bulk_mode = bulk_mode,
            batch_size = batch_size,
            max_retries = max_retries,
            max_backoff_time = max_backoff_time,
            waiting_time_to_poll = waiting_time_to_poll,
            filter_entity_already_in_dataset = filter_entity_already_in_dataset
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
            Boolean bulk_mode
            Boolean filter_entity_already_in_dataset
            String  update_strategy
            Int     batch_size
            Int     max_retries
            Int     max_backoff_time
            Int     waiting_time_to_poll
            String? data_set_id
        }

        command {

            python3 /scripts/ingest_to_tdr_stanley.py --rp ~{rp} \
                                                 --target_table_name ~{target_table_name} \
                                                 --input_tsv ~{ingest_tsv} \
                                                 --update_strategy ~{update_strategy} \
                                                 --batch_size ~{batch_size} \
                                                 --max_retries ~{max_retries} \
                                                 --max_backoff_time ~{max_backoff_time} \
                                                 --waiting_time_to_poll ~{waiting_time_to_poll} \
                                                 ~{"--data_set_id " + data_set_id} \
                                                 ~{if bulk_mode then "--bulk_mode" else ""}
                                                 ~{if filter_entity_already_in_dataset then "--filter_entity_already_in_dataset" else ""}
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