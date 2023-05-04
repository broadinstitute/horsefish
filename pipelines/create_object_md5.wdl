version 1.0

workflow create_object_md5 {
    input {
        String  src_object_path
        String  tmp_object_path
        String  dest_object_path
        Int     file_size_gb
    }

    call copy_to_destination {
        input:
            src_object_path = src_object_path,
            tmp_object_path = tmp_object_path,
            dest_object_path = dest_object_path,
            disk_size = file_size_gb
    }

    call parse_md5 {
        input:
            copy_log = copy_to_destination.copy_log
    }

    output {
        File md5_log = copy_to_destination.copy_log
        String md5 = parse_md5.md5sum
    }
}

task copy_to_destination {
    meta {
        description: "Copy src object, create tmp object to generate md5, copy tmp object to dest now with an md5."
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        String  src_object_path
        String  tmp_object_path
        String  dest_object_path

        Int     disk_size
        Int?    memory
    }

    command {
        # # stream from source and copy contents in memory to _tmp version of src object
        # gsutil -u anvil-datastorage cat "~{src_object_path}" | gsutil -u anvil-datastorage cp -c -L create_md5_log.csv - "~{tmp_object_path}"
        # # move tmp object back to src object
        # gsutil -u anvil-datastorage mv -c -L create_md5_log.csv "~{tmp_object_path}" "~{src_object_path}"
        gsutil cp -D -c -L create_md5_log.log ~{src_object_path} ~{tmp_object_path}
        gsutil cp -D -c -L create_md5_log.log ~{tmp_object_path} ~{dest_object_path}
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        # disks: "local-disk " + (disk_size + 2) + " SSD"
        memory: select_first([memory, 2]) + " GB"
    }

    output {
        File copy_log = "create_md5_log.csv"
    }
}

task parse_md5 {
    meta {
        description: "Parse gsutil cp log file to get md5."
    }

    input {
        File  copy_log
    }

    command {
        # get md5sum after the mv file command
        sed '3q;d' ~{copy_log} | cut -d"," -f5 > md5
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
    }

    output {
        String md5sum = read_string("md5")
    }
}