version 1.0

workflow StartDragenWorkflow {
  input {
    String        research_project
    String        ref_trigger
    String        ref_dragen_config
    String        ref_batch_config
    String        output_bucket
    String        project_id
    String        data_type
    String        dragen_version
    Array[String] cram_paths
    Array[String] sample_ids

  }

  call CreateSampleManifest {
    input:
      cram_paths  = cram_paths,
      sample_ids  = sample_ids
  }

  call CreateDragenConfig {
    input:
      rp                = research_project,
      ref_dragen_config = ref_dragen_config,
      output_bucket     = output_bucket
  }

  call StartDragen {
    input:
      ref_trigger       = ref_trigger,
      ref_dragen_config = ref_dragen_config,
      ref_batch_config  = ref_batch_config,
      project_id        = project_id,
      data_type         = data_type,
      dragen_version    = dragen_version,
      sample_manifest   = CreateSampleManifest.reprocessing_manifest
  }

  output {
    File sample_manifest  = CreateSampleManifest.reprocessing_manifest
  }
}

task CreateSampleManifest {
  input {
    Array[String] cram_paths
    Array[String] sample_ids
  }

  command {

    # generate header
    echo "collaborator_sample_id cram_path" >> sample_processing_manifest.txt

    # write values to file in newline delimited format
    echo "~{sep='\n' sample_ids}"   > sample_ids.txt
    echo "~{sep='\n' cram_paths}"   > cram_paths.txt

    # combine to final
    paste -d " " sample_ids.txt cram_paths.txt >> sample_processing_manifest.txt

  }

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }

  output {
    File reprocessing_manifest = "sample_processing_manifest.txt"
  }
}


task CreateDragenConfig {
  input {
    String ref_dragen_config
    String output_bucket
    String rp
  }

  command <<<
    # write output_dir to ref_dragen_config
    # current: s3://fc-236ab095-52ca-4541-bcaa-795635feccd9/repro_output/COLLAB_SAMPLE_ID/<date>
    # desired: s3://output_bucket/rp/collaborator_sample_id/<date>

    INPUT_FILE=~{ref_dragen_config}
    OUTPUT_BUCKET=~{output_bucket}
    RP=~{rp}
    OUTPUT_PATH=${OUTPUT_BUCKET}/${RP}

    # create tmp file and copy original to tmp
    touch "${INPUT_FILE}.tmp"
    cp "$INPUT_FILE" "${INPUT_FILE}.tmp"

    # now overwrite __OUT_PATH__ with OUTPUT_PATH
    sed 's|__OUT_PATH__|'"$OUTPUT_PATH"'|g;
        ' "${INPUT_FILE}.tmp" > ~{ref_dragen_config}
  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
}


task StartDragen {
  input {
      String  ref_trigger
      String  ref_dragen_config
      String  ref_batch_config
      String  project_id
      String  data_type
      String  dragen_version
      File    sample_manifest
  }

  command <<<

    # copy files to dragen project to trigger batch jobs - per sample
    gsutil cp ~{ref_dragen_config} "gs://~{project_id}-config/"
    gsutil cp ~{ref_batch_config} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{ref_trigger} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{sample_manifest} "gs://~{project_id}-trigger/~{data_type}/input_list/"

  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
}