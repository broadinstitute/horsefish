version 1.0 

workflow Make_FoFn {
    input {
        Array[String] cram_paths
        Array[String] sample_ids
        Array[String] rp_ids
    }
    
    call CreateFoFN {
        input :
            cram_paths = cram_paths,
            sample_ids = sample_ids,
            rp_ids = rp_ids
    }
    output {
        File sample_processing_manifest = CreateFoFN.reprocessing_manifest
    }
}

task CreateFoFN {
    input {
        Array[String] cram_paths
        Array[String] sample_ids
        Array[String] rp_ids

    }
    command {
        echo "collaborator_sample_id cram_path rp_id" >> sample_processing_manifest.txt
        echo "~{sep='\n' sample_ids}"   > sample_ids.txt
        echo "~{sep='\n' cram_paths}"   > cram_paths.txt
        echo "~{sep='\n' rp_ids}"       > rp_ids.txt

        paste -d " " sample_ids.txt cram_paths.txt rp_ids.txt >> sample_processing_manifest.txt
        
    }
    output {
        File reprocessing_manifest = "sample_processing_manifest.txt"
    }
    runtime {
        docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
    }
}
