version 1.0

workflow SamtoolsSplit {
    input {
        Array[File]  bams
        String       output_dir
    }

    scatter (bam in bams) {
        call samtools_split {
        input:
            bam = bam,
            output_dir = output_dir
        }
    }
}

task samtools_split {
    input {
        File  bam
        String  output_dir
    }

    String  sample_name = basename(bam, ".bam")
    Int     mem = ceil(size(bam, "MiB")) + 100
    Int     disk_space = ceil(size(bam, "GiB")) + 50

    command <<<

        # name output files with RG ID - blah blah
        samtools split ~{bam} -f "%!".bam -v
        gsutil cp *.bam gs://~{output_dir}/~{sample_name}/
    >>>

    runtime {
        docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.4.3-1564508330"
        disks: "local-disk " + disk_space + " SSD"
        memory: mem + " MB"
    }
}