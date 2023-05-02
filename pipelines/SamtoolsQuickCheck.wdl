version 1.0

workflow SamtoolsQuickCheck {
    input {
        File    input_sam_file
    }

    call GenerateQCReport {
        input:
            input_sam_file  = input_sam_file
    }

    output {
        String validation_report = GenerateQCReport.report
    }
}

task GenerateQCReport {
  input {
    File    input_sam_file
    Int     preemptible_tries = 0
    Int     memory_multiplier = 1
    Int     additional_disk = 20

    Int disk_size = ceil(size(input_sam_file, "GiB") + additional_disk)
  }

  Int memory_size = ceil(16000 * memory_multiplier)

  command {
    samtools quickcheck ~{input_sam_file}
  }
  runtime {
    docker: "us.gcr.io/broad-gotc-prod/samtools:1.0.0-1.11-1624651616"
    preemptible: preemptible_tries
    memory: "~{memory_size} MiB"
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    String report = read_string(stdout())
  }
}