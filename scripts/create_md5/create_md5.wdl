version 1.0

# Workflow for creating a checksum MD5 file.
# The workflow takes an input file and creates its respective MD5.
workflow create_md5 {
    input {
        File input_file         # Input file to be MD5'd
    }

    # Main task call to generate MD5 checksum.
    call generate_checksum {
        input:
            input_file = input_file,
    }
    
    # Output of the workflow is the generated MD5 checksum.
    output {
        File md5_path = generate_checksum.new_md5
    }
}

# Task definition for generating the MD5 checksum.
task generate_checksum {
    input {
        File input_file         # Input file to be MD5'd
    }

    # Naming convention for new file
    String new_md5 = input_file + ".md5"
  
    # Calculate the required disk size based on input file sizes
    Int disk_size = ceil(2 * size(input_file, "GiB")) + 20

    command {
        set -e

        # Generate MD5 checksum for the file
        md5sum ~{input_file} > "output_file.md5"
    }

    # Runtime parameters including docker image, memory, CPU, disk, and retries
    runtime {
        docker: "us.gcr.io/broad-gotc-prod/samtools:1.0.0-1.11-1624651616"
        preemptible: 3
        memory: "7 GiB"
        cpu: "1"
        disks: "local-disk " + disk_size + " HDD"
    }

    # Output files from the task
    output {
        File new_md5 = "output_file.md5"
    }
}
