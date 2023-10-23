version 1.0

workflow CreatePGSVisualizations {
    input {
        String  workspace_name
        String  workspace_project
        String  input_table_name

        String? grouping_column_name
        String? run_id_filter
        String? output_filename
    }

    call create_viz {
        input:
            workspace_name          =   workspace_name,
            workspace_project       =   workspace_project,
            input_table_name        =   input_table_name,
            grouping_column_name    =   grouping_column_name,
            run_id_filter           =   run_id_filter,
            output_filename         =   output_filename
    }

    output {
        File viz_pdf    =   create_viz.vizualizations
    }
}

task create_viz {
    input {
        String  workspace_name
        String  workspace_project
        String  input_table_name

        String? grouping_column_name
        String? run_id_filter
        String  output_filename = "QC_vizualizations.pdf"

        String  docker  =   "broadinstitute/horsefish:pgs_visualizations"        
    }

    command {
        python3 scripts/create_visualizations.py -t ~{input_table_name} \
                                                 -w ~{workspace_name} \
                                                 -p ~{workspace_project} \
                                                 ~{"-r " + run_id_filter} \
                                                 ~{"-g" + grouping_column_name} \
                                                 ~{"-o" + output_filename}
    }

    runtime {
        docker: docker
    }

    output {
        File vizualizations = "~{output_filename}"
    }
}