version 1.0

workflow parse_dragen_metrics {
    input {
        String  sample_name
        String  output_path
        String  docker_name
    }

    call get_metrics {
        input:
            sample_name = sample_name,
            output_path = output_path,
            docker_name = docker_name
    }

    output {
        String mapped_reads = get_metrics.mapped_reads
        String chimera_rate = get_metrics.chimera_rate
        String contamination_rate = get_metrics.contamination_rate
        String mean_target_coverage = get_metrics.mean_target_coverage
        String percent_target_bases_at_10x = get_metrics.percent_target_bases_at_10x
        String total_bases = get_metrics.total_bases
        String percent_wgs_bases_at_1x = get_metrics.percent_wgs_bases_at_1x
        String percent_callability = get_metrics.percent_callability
    }
}

task get_metrics {

    input {
        String  sample_name
        String output_path
        String  docker_name
    }

    command{

        python3 scripts/get_dragen_metrics.py -s ~{sample_name} -o ~{output_path}

    }

    runtime {
        docker: docker_name
    }

    output {
        String mapped_reads = read_string("mapped_reads.tsv")
        String chimera_rate = read_string("chimera_rate.tsv")
        String contamination_rate = read_string("contamination_rate.tsv")
        String mean_target_coverage = read_string("mean_target_coverage.tsv")
        String percent_target_bases_at_10x = read_string("percent_target_bases_at_10x.tsv")
        String total_bases = read_string("total_bases.tsv")
        String percent_wgs_bases_at_1x = read_string("percent_wgs_bases_at_1x.tsv")
        String percent_callability = read_string("percent_callability.tsv")
    }
}