version 1.0

workflow ValidateSamFile {
    input {
        File    input_sam_file
        String  report_filename
    }

    call GenerateQCReport {
        input:
            input_sam_file  = input_sam_file,
            report_filename = report_filename
    }

    output {
        File validation_report = GenerateQCReport.report
    }
}

# adapted from warp QC tasks for production pipelines
# https://github.com/broadinstitute/warp/blob/4655af47bf3bd4410e28f9b94f6780e782b78c00/tasks/broad/Qc.wdl
task GenerateQCReport {
  input {
    File    input_sam_file
    String  report_filename
    Int     preemptible_tries = 0
    Int     memory_multiplier = 1
    Int     additional_disk = 20

    Int disk_size = ceil(size(input_sam_file, "GiB") + additional_disk)
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
  }

  runtime {
    docker: "us.gcr.io/broad-gotc-prod/picard-cloud:2.26.10"
    preemptible: preemptible_tries
    memory: "~{memory_size} MiB"
    disks: "local-disk " + disk_size + " HDD"
    continueOnReturnCode: true
  }

  output {
    File report = "~{report_filename}"
  }
}