version 1.0

workflow migrate_data_via_local {
    input {
        String src_object_path
        String tmp_object_path
    }

    call copy_to_destination {
        input:
            src_object_path = src_object_path,
            tmp_object_path = tmp_object_path
    }

    output {
        File md5_log = copy_to_destination.copy_log
    }
}

task copy_to_destination {
    meta {
        description: "Copy src object, create tmp object to generate md5, rename tmp object to same name as src object."
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        String src_object_path
        String tmp_object_path
    }

    command {
        gsutil cat "~{src_object_path}" | gsutil cp -c -L create_md5_log.csv - "~{tmp_object_path}"
        gsutil mv "~{tmp_object_path}" "~{src_object_path}"
    }

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
    }

    output {
        File copy_log = "create_md5_log.csv"
    }
}