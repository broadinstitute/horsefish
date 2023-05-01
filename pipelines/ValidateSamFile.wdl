version 1.0

workflow ValidateSamFile {
    input {
        File input_sam_file
    }

    call GenerateQCReport {
        input:
            input_sam_file = input_sam_file
    }
}

# adapted from warp QC tasks for production pipelines
# https://github.com/broadinstitute/warp/blob/4655af47bf3bd4410e28f9b94f6780e782b78c00/tasks/broad/Qc.wdl
task GenerateQCReport {
  input {
    File input_sam_file
    # File? input_bam_index
    String report_filename = "validation_report"
    # File ref_dict
    # File ref_fasta
    # File ref_fasta_index
    # Int? max_output
    # Array[String]? ignore
    # Boolean? is_outlier_data
    Int preemptible_tries = 0
    Int memory_multiplier = 1
    Int additional_disk = 20

    Int disk_size = ceil(size(input_sam_file, "GiB") + additional_disk)
                    # + size(ref_fasta, "GiB") 
                    # + size(ref_fasta_index, "GiB")
                    # + size(ref_dict, "GiB")) + additional_disk
  }

  Int memory_size = ceil(16000 * memory_multiplier)
  Int java_memory_size = memory_size - 1000
  Int max_heap = memory_size - 500

  command {
    java -Xms~{java_memory_size}m -Xmx~{max_heap}m -jar /usr/picard/picard.jar \
      ValidateSamFile \
      INPUT=~{input_sam_file} \
      OUTPUT=~{report_filename} \
      MODE=VERBOSE
    #   REFERENCE_SEQUENCE=~{ref_fasta} \
    #   ~{"MAX_OUTPUT=" + max_output} \
    #   IGNORE=~{default="null" sep=" IGNORE=" ignore} \
    #   MODE=VERBOSE \
    #   ~{default='SKIP_MATE_VALIDATION=false' true='SKIP_MATE_VALIDATION=true' false='SKIP_MATE_VALIDATION=false' is_outlier_data} \
    #   IS_BISULFITE_SEQUENCED=false
  }
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/picard-cloud:2.26.10"
    preemptible: preemptible_tries
    memory: "~{memory_size} MiB"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    File report = "~{report_filename}"
  }
}