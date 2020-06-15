version 1.0

workflow monitor_submission {
    meta {
        description: "WDL that monitors progress of a submitted workflow and returns Boolean success flag and metadata upon completion."
    }
    
    parameter_meta {
        terra_project: "Namespace/project of the workspace."
        terra_workspace: "Name of Terra workspace."
        submission_id: "Submission ID (found from Job History tab or API)."
    }
    input {
        String terra_project
        String terra_workspace
        String submission_id
    }

    call run_monitor {
        input:
            terra_project = terra_project,
            terra_workspace = terra_workspace,
            submission_id = submission_id
    } 

}
task run_monitor {
    parameter_meta {
        time_check_interval: "Time (seconds) to wait between checking submission status. Default = 300s."
    }
    
    input {
        String terra_project
        String terra_workspace
        String submission_id
        Int? time_check_interval
    }

    command {
        python3 -c 'from scripts/monitor_submission import monitor_submission; \
        monitor_submission(~{terra_workspace}, \
            ~{terra_project}, \
            ~{submission_id}, \
            ~{time_check_interval}, \
            write_outputs_to_disk=True)'
    }

    runtime {
        docker: "broadinstitute/horsefish:latest"
        preemptible: 3
    }

    output {
        completion_status = "submission_succeeded.txt"
        json = "monitor_submission_metadata.json"
    }
}