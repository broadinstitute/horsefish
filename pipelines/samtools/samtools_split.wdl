version 1.0

workflow SamtoolsSplit {
    input {
        Array[File]  bams
    }

    scatter (bam in bams) {
        call samtools_split {
        input:
            bam = bam
        }
    }
}

task samtools_split {
    input {
        File  bam
    }

    Int mem = ceil(size(bam, "MiB")) + 100
    Int disk_space = ceil(size(bam, "GiB")) + 50

    command <<<

        samtools split ~{bam} -v

    >>>

    runtime {
        docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.4.3-1564508330"
        disks: "local-disk " + disk_space + " SSD"
        memory: mem + " MB"
    }
}