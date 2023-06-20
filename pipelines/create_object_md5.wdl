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

        python3 scripts/create_object_md5.py -o ~{original_object} \
                                             ~{"-d=" + backup_object_dir} \
                                             ~{"-r=" + requester_pays_project}

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

# task copy_to_destination {
#     meta {
#         description: "Copy src object, create tmp object to generate md5, rename tmp object to same name as src object."
#         volatile: true # do not call cache even if set at workflow level
#     }

#     input {
#         String  original_object
#         String? backup_object_dir
#         String? requester_pays_project

#         Int     disk_size
#         Int?    memory
#     }

#     String original_object_name = basename(original_object) # filename.txt
#     String original_object_path = sub(original_object, original_object_name, "") # gs://bucket_name/object_path/
#     String tmp_object_name = original_object_name + ".tmp" # filename.txt.tmp

#     command <<<
#     python CODE <<
#     if ~{original_object}:
#         print("Backup directory has been provided.")
#         backup_object = "~{backup_object_dir}" + "~{original_object_name}"
#         print(f"Starting creation of backup copy to: {backup_object}")
#         cmd = f"gsutil ~{if defined(requester_pays_project) then '-u ' + requester_pays_project else ""} cp -L create_md5_log.csv -D '~{original_object}' {backup_object}"
#         print(cmd)
#         subprocess.run(cmd)
#         # gsutil ~{if defined(requester_pays_project) then "-u " + requester_pays_project else ""} cp -L create_md5_log.csv -D ~{original_object} $backup_object

#     CODE
#     >>>

#     runtime {
#         docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
#         disks: "local-disk " + (disk_size + 2) + " SSD"
#         memory: select_first([memory, 2]) + " GB"
#     }

#     output {
#         File copy_log = "create_md5_log.csv"
#     }
# }

# task get_md5 {
#     meta {
#         description: "Use gsutil to get md5 of object."
#     }

#     input {
#         String  copy_log
#     }

#     command {
#         # get md5sum after the mv file command
#         sed '3q;d' ~{copy_log} | cut -d"," -f5 > md5
#     }

#     runtime {
#         docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
#     }

#     output {
#         String md5sum = read_string("md5")
#     }
# }