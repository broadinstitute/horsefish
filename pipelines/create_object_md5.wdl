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
        volatile: true # do not call cache even if otherwise set at workflow level
    }

    input {
        String  original_object
        String? backup_object_dir

        Int     disk_size
        Int?    memory
    }

    String original_object_name = basename(original_object) # filename.txt
    String original_object_path = sub(original_object, original_object_name, "") # gs:///
    String tmp_object_name = original_object_name + ".tmp" # filename.txt.tmp
    String tmp_object = original_object_path + tmp_object_name

    command {
        set -e

        echo ~{original_object}
        echo ~{original_object_name}
        echo ~{original_object_path}
        echo ~{tmp_object_name}
        echo ~{tmp_object}

        # if user selects backup location - create back up copy and confirm successful copy comparing file sizes
        if [ -z "${backup_object_dir}" ]
        then
            echo "User has provided a backup directory. Starting creation of backup copy."
            # TODO: handle if there is a trailing / or not based on the user input
            backup_object="~{backup_object_dir}"+"~{original_object_name}"
            echo $backup_object
            # make a copy of the original file in the backup location
            gsutil -u anvil-datastorage cp -L create_md5_log.csv -D "~{original_object}" $backup_object
        
            # confirm that original and backup object file sizes are same
            original_object_size=$(gsutil du "~{original_object}" | tr " " "\t" | cut -f1)
            backup_object_size=$(gsutil du $backup_object | tr " " "\t" | cut -f1)

            # if file sizes don't match, exit script with error message
            if [[ $original_object_size == $backup_object_size ]]
            then
                echo "Backup copy of original object complete."
            else
                echo "Backup copy of original object failed."
                exit 1
            fi
        
        # if user doesn't select backup location
        else
            echo "No user backup directory provided. Starting creation of tmp copy."
            # make a TMP copy of the original file
            gsutil -u anvil-datastorage cp -L create_md5_log.csv -D "~{original_object}" "~{tmp_object}"

            # confirm that original and backup object file sizes are same
            original_object_size=$(gsutil du "~{original_object}" | tr " " "\t" | cut -f1)
            tmp_object_size=$(gsutil du "~{tmp_object}" | tr " " "\t" | cut -f1)

            # if file sizes don't match, exit script with error message
            if [[ $original_object_size == $tmp_object_size ]]
            then
                echo "Tmp copy of original object complete."
            else
                echo "Tmp copy of original object failed."
                exit 1
            fi 
        fi
        
        # if tmp copy succeeds, replace original with tmp - should have md5
        gsutil -u anvil-datastorage cp -L create_md5_log.csv -D "~{tmp_object}" "~{original_object}"

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