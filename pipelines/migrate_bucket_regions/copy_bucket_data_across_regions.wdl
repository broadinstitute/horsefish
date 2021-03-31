version 1.0

workflow copy_bucket_across_regions {
    input {
        String source_bucket_path
        String destination_bucket_path
    }
    
    call copy_data {
        input:
            source_bucket_path = source_bucket_path,
            destination_bucket_path = destination_bucket_path
    }

    output {
        File copy_log = copy_data.log
    }
}

task copy_data {
    meta {
        description: "Copies data from source bucket to destination bucket and creates log file."
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        String source_bucket_path
        String destination_bucket_path
        Int? preemptible_tries
        Int? memory
    }

    command {
        set -x
        set -e

        gsutil -m cp -R -D -p -c -L copy.log ~{source_bucket_path} ~{destination_bucket_path}

    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        memory: select_first([memory, 10]) + " GB"
        disks: "local-disk 10 HDD"
        zones: "us-central1-c us-central1-b"
        # us-central1-a and us-central1-f are also us-central1 zones - can we just set us-central1
        preemptible: select_first([preemptible_tries, 5])
        cpu: 1
    }

    output {
        # TODO: parse out the bucket source and destination and add into the log file name
        File log = "copy.log"
    }
}