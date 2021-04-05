version 1.0

workflow migrate_data_via_local {
    input {
        String source_bucket_path
        String destination_bucket_path
    }
    
    call calculate_largest_file_size {
        input:
            source_bucket_path = source_bucket_path
    }

    call copy_to_destination {
        input:
            source_bucket_path = source_bucket_path,
            destination_bucket_path = destination_bucket_path,
            disk_size = calculate_largest_file_size.max_gb
    }

    output {
        File copy_log = copy_to_destination.log
    }
}

task calculate_largest_file_size {
    meta {
        description: "Determine the size of the largest file in the given source bucket."
    }

    input {
        String source_bucket_path
    }

    command {
        largest_file_in_bytes=$(gsutil du ~{source_bucket_path} | sed "/\/$/d" | tr " " "\t" | tr -s "\t" | sort -n -k1,1nr | awk 'NR==1' | cut -f1) 

        largest_file_in_gb=$(((largest_file_in_bytes/1000000000)+1))
        echo "$largest_file_in_gb" > file_gb
    }

    output {
        Int max_gb = read_int("file_gb")
    }
}

task copy_to_destination {
    meta {
        description: "Copies data from source bucket to destination bucket and creates log file."
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        String source_bucket_path
        String destination_bucket_path
        Int disk_size
        Int? preemptible_tries
        Int? memory
    }

    command {
        set -x
        set -e

        # -p requires OWNER access to maintain ACLs
        gsutil -m cp -R -D -c -L copy.log ~{source_bucket_path} ~{destination_bucket_path}

    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        memory: select_first([memory, 10]) + " GB"
        disks: "local-disk " + disk_size + " SSD"
        zones: "us-central1-c us-central1-b"
        # us-central1-a and us-central1-f are also us-central1 zones - can we just set us-central1
        preemptible: select_first([preemptible_tries, 0])
        cpu: 3
    }

    output {
        # TODO: parse out the bucket source and destination and add into the log file name
        File log = "copy.log"
    }
}