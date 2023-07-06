version 1.0

workflow RetryPrsSubmissionFailures {
    meta {
        description: "Gather all failed and/or aborted workflows in a PRS submission and reingest to TDR to trigger WFL retry."
    }
    String pipeline_version = "1.0.0"

    input {
        String  submission_id
        String  dataset_id
        String  table_name
        String  primary_key
        
        # source workspace
        String          workspace_name
        String          workspace_namespace
    }

    call retry_prs_failures{
        input:
            submission_id       =   submission_id,
            dataset_id          =   dataset_id,
            table_name          =   table_name,
            primary_key         =   primary_key,
            workspace_name      =   workspace_name,
            workspace_namespace =   workspace_namespace   
    }

    output {
        String  job_id  =   retry_prs_failures.request_id
    }
}

task retry_prs_failures {
    input {
        String  submission_id
        String  dataset_id
        String  table_name
        String  primary_key
        Boolean include_aborted = false

        String  workspace_namespace
        String  workspace_name
    }

    command {
        python3 /scripts/emerge/retry_wfl_submission_failures.py -s ~{submission_id} \
                                                                 -d ~{dataset_id} \
                                                                 -t ~{table_name} \
                                                                 -k ~{primary_key} \
                                                                 -p ~{workspace_namespace} \
                                                                 -w ~{workspace_name} \
                                                                 ~{if include_aborted then "-a" else ""} # include aborted workflows
    }

    runtime {
        docker: "broadinstitute/horsefish"
    }

    output {
        String request_id   =   read_string("job_id.txt")
    }
}