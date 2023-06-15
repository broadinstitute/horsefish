version 1.0

workflow create_object_md5 {
    input {
        String  original_object
        Int     file_size_gb
    }

    call copy_to_destination {
        input:
            original_object = original_object,
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

    String original_object_name = basename(original_object) # filename.txt
    String original_object_path = sub(original_object, original_object_name, "") # gs://bucket_name/object_path/
    String tmp_object_name = original_object_name + ".tmp" # filename.txt.tmp

    command <<<
        set -e

        # user selects backup location - create back up copy and confirm successful copy comparing file sizes
        if [ ! -z "~{backup_object_dir}" ]
        then
            # make a copy of the original file in the backup location
            # TODO: handle if there is a trailing / or not based on the user input
            echo "Backup directory has been provided."
            backup_object="~{backup_object_dir}~{original_object_name}"
            echo "Starting creation of backup copy to: $backup_object"
            gsutil ~{if defined(requester_pays_project) then "-u " + requester_pays_project else ""} cp -L create_md5_log.csv -D ~{original_object} $backup_object

            # confirm that original and backup object file sizes are same
            original_object_size=$(gsutil du "~{original_object}" | tr " " "\t" | cut -f1)
            backup_object_size=$(gsutil du $backup_object | tr " " "\t" | cut -f1)
            echo -e "original object size: $original_object_size bytes"
            echo -e "backup object size: $backup_object_size bytes"

            # if file sizes don't match, exit script with error message
            if [[ $original_object_size == $backup_object_size ]]
            then
                echo "Backup copy of original object complete - original and backup copy have the same file size."
            else
                echo "Backup copy of original object failed - original and backup copy do not have the same file size."
                echo "This is likely a transient failure. Please submit workflow again."
                exit 1
            fi
        fi
        
        # user does not select backup location - no backup copy is created
        # create a tmp copy of the original object
        tmp_object="~{original_object_path}~{tmp_object_name}" 
        echo "Starting creation of tmp copy to: $tmp_object"

        gsutil cp -L create_md5_log.csv -D ~{original_object} $tmp_object

        # # confirm that original and tmp object file sizes are same
        # original_object_size=$(gsutil du "~{original_object}" | tr " " "\t" | cut -f1)
        # tmp_object_size=$(gsutil du $tmp_object | tr " " "\t" | cut -f1)
        # echo -e "original object size: $original_object_size bytes"
        # echo -e "tmp object size: $tmp_object_size bytes"
    
        # # if file sizes don't match, exit script with error message
        # if [[ $original_object_size == $tmp_object_size ]]
        # then
        #     echo "Tmp copy of original object complete - original and tmp copy have the same file size."
        # else
        #     echo "Tmp copy of original object failed - original and tmp copy do not have the same file size."
        #     echo "This is likely a transient failure. Please submit workflow again."
        #     exit 1
        # fi
        
        # if tmp copy succeeds, replace original with tmp - should have md5
        echo "Starting replace of the original object with tmp object to generate md5."
        # TODO: want to change this cp to mv in order to replace and not copy, no daisy chain
        gsutil ~{if defined(requester_pays_project) then "-u " + requester_pays_project else ""} cp -L create_md5_log.csv $tmp_object "~{original_object}"
    >>>

    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
        disks: "local-disk " + (disk_size + 2) + " SSD"
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