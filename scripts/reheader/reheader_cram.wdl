version 1.0

# Workflow for reheadering a CRAM file.
# The workflow takes an input CRAM file, a reference genome, and changes the sample name in the CRAM header.
workflow reheader_cram {
    input {
        File input_cram         # Input CRAM file to be reheadered
        String old_sample       # Original sample name in the CRAM file
        String new_sample       # New sample name to replace the original
        File ref_fasta          # Reference genome fasta file
        File ref_fasta_index    # Index for the reference genome fasta file
    }

    # Main task call to perform reheadering
    call reheader {
        input:
            input_cram = input_cram,
            old_sample = old_sample, 
            new_sample = new_sample, 
            ref_fasta = ref_fasta,
            ref_fasta_index = ref_fasta_index,
    }
    
    # Output of the workflow includes the new CRAM file, its index, and MD5 checksum.
    output {
        File cram_path = reheader.new_cram
        File crai_path = reheader.new_crai
        File md5_path = reheader.new_md5
    }
}

# Task definition for reheadering a CRAM file.
task reheader {
    input {
        File input_cram         # Input CRAM file to be reheadered
        String old_sample       # Original sample name in the CRAM file
        String new_sample       # New sample name to replace the original
        File ref_fasta          # Reference genome fasta file
        File ref_fasta_index    # Index for the reference genome fasta file
    }

    # Naming convention for new files
    String new_cram = new_sample + ".cram"
    String new_crai = new_sample + ".cram.crai"
    String new_md5 = new_sample + ".cram.md5"
  
    # Calculate the required disk size based on input file sizes
    Int disk_size = ceil((2 * size(input_cram, "GiB")) + size(ref_fasta, "GiB") + size(ref_fasta_index, "GiB")) + 20

    command {
        set -e

        # Extract header from CRAM, modify sample name, and remove @PG lines
        samtools view -H ~{input_cram} > header
        sed --expression='s/SM:~{old_sample}/SM:~{new_sample}/g' header > remapped_header
        grep -v '^@PG' remapped_header > updated_header

        # Reheader the CRAM file with updated header
        samtools reheader -P updated_header ~{input_cram} > ~{new_cram}
    
        # Generate MD5 checksum for the new CRAM file
        md5sum ~{new_cram} > ~{new_md5}
    
        # Prepare REF_CACHE for indexing the CRAM file
        seq_cache_populate.pl -root ./ref/cache ~{ref_fasta}
        export REF_PATH=:
        export REF_CACHE=./ref/cache/%2s/%2s/%s
    
        # Index the new CRAM file
        samtools index ~{new_cram} > ~{new_crai}
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
        File new_cram = "~{new_cram}"
        File new_crai = "~{new_crai}"
        File new_md5 = "~{new_md5}"
    }
}