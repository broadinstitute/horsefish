version 1.0

workflow simulations_demo {
    input {
        String      sim_id
        String      subdir
        Array[File] sim_files
    }

    call run_simulation {
        input:
            sim_id      = sim_id,
            subdir      = subdir,
            sim_files   = sim_files
    }

    output {
        File sim_outfile = run_simulation.sim_filepaths
    }
}

task run_simulation {
    input {
        String sim_id
        String subdir
        Array[File] sim_files
    }

    command <<<

        sim_file_paths='~{sep='","' sim_files}'
        
        # write the path to each file in the array of files to an output file
        echo -e "~{sim_id}\n~{subdir}\n[\"${sim_file_paths}\"]" \
            > sim_demo_outputs.tsv
    >>>

    runtime {
        docker: "broadinstitute/horsefish"
    }

    output {
        File sim_filepaths = "sim_demo_outputs.tsv"
    }
}