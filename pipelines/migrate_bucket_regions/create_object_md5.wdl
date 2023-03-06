version 1.0

workflow create_object_md5 {
    input {
        String  src_object_path
        String  tmp_object_path
        Int     file_size_bytes
    }

    call calculate_file_size {
        input:
            file_size_bytes = file_size_bytes
    }

    call copy_to_destination {
        input:
            src_object_path = src_object_path,
            tmp_object_path = tmp_object_path,
            disk_size = calculate_file_size.max_gb
    }

    output {
        File md5_log = copy_to_destination.copy_log
    }
}

task calculate_file_size {
    meta {
        description: "Determine the size in GB of source object with the given file size in bytes."
    }

    input {
        Int file_size_bytes
    }

    command {
        file_size_gb=$(((~{file_size_bytes}/1000000000)+1))
        echo "$file_size_gb" > file_gb
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
    }

    output {
        Int max_gb = read_int("file_gb")
    }
}

task copy_to_destination {
    meta {
        description: "Copy src object, create tmp object to generate md5, rename tmp object to same name as src object."
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        String  src_object_path
        String  tmp_object_path

        Int     disk_size
        Int?    memory
    }

    command {
        gsutil cat "~{src_object_path}" | gsutil cp -c -L create_md5_log.csv - "~{tmp_object_path}"
        gsutil mv "~{tmp_object_path}" "~{src_object_path}"
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        disks: "local-disk " + (disk_size + 2) + " SSD"
        memory: select_first([memory, 2]) + " GB"
    }

    output {
        File copy_log = "create_md5_log.csv"
    }
}