version 1.0

workflow create_object_md5 {
    input {
        String  original_object
        Int     file_size_gb
    }

    call get_object_hash {
        input:
            original_object = original_object,
            disk_size = file_size_gb
    }

    output {
        String md5 = get_object_hash.md5_hash
    }
}

task get_object_hash {
    meta {
        description: "Copy src object, create tmp object to generate md5, rename tmp object to same name as src object."
        volatile: true # do not call cache even if set at workflow level
    }

    input {
        String  original_object
        String? backup_object_dir
        String? requester_pays_project

        Int     disk_size
        Int?    memory
    }

    command {

        python3 /scripts/create_object_md5.py -o ~{original_object} ~{"-d=" + backup_object_dir} ~{"-r=" + requester_pays_project}

    }

    runtime {
        docker: "gcr.io/dsp-solutions-eng-playground/cloud-sdk:435.0.1-md5"
        disks: "local-disk " + (disk_size + 2) + " SSD"
        memory: select_first([memory, 2]) + " GB"
    }

    output {
        String md5_hash = read_string("object_md5.txt")
    }
}