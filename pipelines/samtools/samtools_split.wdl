version 1.0

workflow SamtoolsSplit {
    input {
        Array[File] bams
        File        output_map
    }

    scatter (bam in bams) {
        call samtools_split {
        input:
            bam = bam,
            output_map = output_map
        }
    }
}

task samtools_split {
    input {
        File    bam
        File    output_map
    }

    # String  sample_name = basename(bam, ".bam")
    Int     mem = ceil(size(bam, "MiB")) + 100
    Int     disk_space = ceil(size(bam, "GiB")) + 50

    command {
        # # name output files with RG ID - blah blah
        # samtools split ~{bam} -f "%!".bam -v

        cut  -f 1 ~{output_map} | tail -n +2 > readgroups.list
        bamName=$(basename ~{bam})
        while read readgroup; do
            echo $readgroup
            samtools view -b -h -r $readgroup -o $readgroup.$bamName ~{bam} &
        done < readgroups.list

        wait

        bam_names=$(cut  -f 1 ~{output_map} | tail -n +2 | tr "\n" " ")
        samtools merge $bam_names -o $bamName.final.merged.bam
    }


    runtime {
        docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.4.3-1564508330"
        disks: "local-disk " + disk_space + " SSD"
        memory: mem + " MB"
    }

    output {
    Array[File] readgroup_bams = glob("*.bam")
    }
}