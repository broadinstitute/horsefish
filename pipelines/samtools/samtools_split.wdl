version 1.0

workflow SamtoolsSplit {
    input {
        File bam
        File output_map
        Int extra_mem
        Int extra_space
    }
    call samtools_split {
    input:
        bam = bam,
        output_map = output_map,
        extra_mem = extra_mem,
        extra_space = extra_space
    }

    output {
        File output_bam = samtools_split.merged_bam
    }
}

task samtools_split {
    input {
        File    bam
        File    output_map
        Int     extra_mem
        Int     extra_space
    }

    Int     mem = ceil(size(bam, "MiB")) + 150 + extra_mem
    Int     disk_space = ceil(size(bam, "GiB")) + 100 + extra_space
    String  bamName = basename(bam, ".bam")

    command {

        cut -f 1 ~{output_map} | tail -n +2 > readgroups.list
        
        while read readgroup; do
            echo $readgroup
            samtools view -b -h -r $readgroup -o $readgroup.bam ~{bam} &
        done < readgroups.list

        wait

        bam_names=$(cut -f 2 ~{output_map} | tail -n +2 | tr "\n" " ")
        samtools merge ~{bamName}_final_merged.bam $bam_names

    }

    runtime {
        docker: "us.gcr.io/broad-gotc-prod/genomes-in-the-cloud:2.4.3-1564508330"
        disks: "local-disk " + disk_space + " SSD"
        memory: mem + " MB"
    }

    output {
        File    merged_bam = "~{bamName}_final_merged.bam"
    }
}