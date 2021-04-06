version 1.0

workflow migrate_data_via_local {
    input {
        String source_bucket_path
        String destination_bucket_path
    }
    
    call get_source_bucket_details{
        input:
            source_bucket_path = source_bucket_path
    }

    call calculate_largest_file_size {
        input:
            source_bucket_details = get_source_bucket_details.source_bucket_details_file
    }

    call copy_to_destination {
        input:
            source_bucket_details = get_source_bucket_details.source_bucket_details_file,
            destination_bucket_path = destination_bucket_path,
            disk_size = calculate_largest_file_size.max_gb
    }

    output {
        File source_bucket_file_info = get_source_bucket_details.source_bucket_details_file
        Int calculated_memory_size = calculate_largest_file_size.max_gb
        File log_copy_from_src_to_local = copy_to_destination.copy_to_local_log
        File log_copy_from_local_to_dest = copy_to_destination.copy_from_local_log
    }
}

task get_source_bucket_details {
    meta {
        description: "Get a file with the list of file paths to copy and size of each file in bytes."
    }

    input {
        String source_bucket_path
    }

    command {
        gsutil du ~{source_bucket_path} | sed "/\/$/d" | tr " " "\t" | tr -s "\t" | sort -n -k1,1nr > source_bucket_details.txt
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        memory: "2 GB"
        disks: "local-disk 10 HDD"
        zones: "us-central1-c us-central1-b"
    }

    output {
        File source_bucket_details_file = "source_bucket_details.txt"
    }
}


task calculate_largest_file_size {
    meta {
        description: "Determine the size of the largest file in the given source bucket."
    }

    input {
        File source_bucket_details
    }

    command {
        largest_file_in_bytes=$(awk 'NR==1' ~{source_bucket_details}  | cut -f1) 

        largest_file_in_gb=$(((largest_file_in_bytes/1000000000)+1))
        echo "$largest_file_in_gb" > file_gb
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        memory: "2 GB"
        disks: "local-disk 10 HDD"
        zones: "us-central1-c us-central1-b"
    }

    output {
        Int max_gb = read_int("file_gb")
    }
}


task copy_to_destination {
    meta {
        description: "Copy data from source bucket to destination bucket and creates log file."
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        File source_bucket_details
        String destination_bucket_path
        Int disk_size
        Int? preemptible_tries
        Int? memory
    }

    command {
        set -x
        set -e

        cut -f2 ~{source_bucket_details} > source_bucket_file_paths.txt

        while IFS="\t" read -r file_path
        do
            # get the path minus the fc-** to copy to local disk
            local_file_path=$(echo "$file_path" | tr "/" "\t" | cut -f4- | tr "\t" "/")
            gsutil cp -L copy_to_local.log "$file_path" "/cromwell_root/$local_file_path"

            # use path of local copy to copy to destination bucket
            gsutil cp -L copy_from_local.log "/cromwell_root/$local_file_path" "~{destination_bucket_path}/$local_file_path"

            # remove the file before copying next one
            rm "/cromwell_root/$local_file_path"
        done < source_bucket_file_paths.txt
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        memory: select_first([memory, 10]) + " GB"
        # disks: "local-disk " + (disk_size + 10) + " SSD"
        disks: "local-disk 100 SSD"
        zones: "us-central1-c us-central1-b"
        preemptible: select_first([preemptible_tries, 0])
        cpu: 2
    }

    output {
        File copy_to_local_log = "copy_to_local.log"
        File copy_from_local_log = "copy_from_local.log"
    }
}