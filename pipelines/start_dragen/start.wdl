version 1.0

workflow StartDragenWorkflow {
  input {
    File          ref_trigger
    File          ref_dragen_config
    File          ref_batch_config
    String        output_bucket
    String        project_id
    String        data_type
    String        dragen_version
    Array[String] cram_paths
    Array[String] sample_ids
    Array[String] rp
  }

  call CreateSampleManifest {
    input:
      cram_paths  = cram_paths,
      sample_ids  = sample_ids
  }

  call CreateDragenConfig {
    input:
      rp                = rp,
      ref_dragen_config = ref_dragen_config,
      output_bucket     = output_bucket
  }

  call StartDragen {
    input:
      ref_trigger       = ref_trigger,
      dragen_config     = CreateDragenConfig.final_dragen_config,
      ref_batch_config  = ref_batch_config,
      project_id        = project_id,
      data_type         = data_type,
      dragen_version    = dragen_version,
      sample_manifest   = CreateSampleManifest.reprocessing_manifest
  }

  output {
    File sample_manifest  = CreateSampleManifest.reprocessing_manifest
    File dragen_config    = CreateDragenConfig.final_dragen_config
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
    File          ref_dragen_config
    String        output_bucket
    Array[String] rp
  }

  command <<<
    # write output_dir to ref_dragen_config
    # current: s3://fc-236ab095-52ca-4541-bcaa-795635feccd9/repro_output/COLLAB_SAMPLE_ID/<date>
    # desired: s3://output_bucket/rp/collaborator_sample_id/<date>

    OUTPUT_PATH=~{output_bucket}/~{rp}

    #    # create tmp file and copy original to tmp
    #    cp ~{ref_dragen_config} ~{ref_dragen_config}.tmp

    # now overwrite __OUT_PATH__ with OUTPUT_PATH
    sed 's|__OUT_PATH__|'"$OUTPUT_PATH"'|g;
        ' ~{ref_dragen_config} > dragen_config.json
  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
  output {
    File final_dragen_config = "dragen_config.json"
  }
}


task StartDragen {
  input {
      File    ref_trigger
      File    dragen_config
      File    ref_batch_config
      String  project_id
      String  data_type
      String  dragen_version
      File    sample_manifest
  }

  command <<<

    # copy files to dragen project to trigger batch jobs - per sample
    gsutil cp ~{dragen_config} "gs://~{project_id}-config/"
    gsutil cp ~{ref_batch_config} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{ref_trigger} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{sample_manifest} "gs://~{project_id}-trigger/~{data_type}/input_list/"

  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
}