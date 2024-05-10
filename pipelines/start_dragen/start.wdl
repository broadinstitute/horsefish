version 1.0

workflow StartDragenWorkflow {
  input {
    String        research_project
    File          ref_trigger
    File          ref_dragen_config
    File          ref_batch_config
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
      sample_ids  = sample_ids,
      rp          = research_project
  }

  call CreateConfigs {
    input:
      rp                = research_project,
      data_type         = data_type,
      ref_dragen_config = ref_dragen_config,
      ref_batch_config  = ref_batch_config,
      output_bucket     = output_bucket,
      sample_manifest   = CreateSampleManifest.reprocessing_manifest
  }

  call StartDragen {
    input:
      ref_trigger       = ref_trigger,
      dragen_config     = CreateConfigs.final_dragen_config,
      batch_config      = CreateConfigs.final_batch_config,
      project_id        = project_id,
      data_type         = data_type,
      dragen_version    = dragen_version,
      sample_manifest   = CreateSampleManifest.reprocessing_manifest
  }

  output {
    File sample_manifest  = CreateSampleManifest.reprocessing_manifest
    File dragen_config    = CreateConfigs.final_dragen_config
    File batch_config     = CreateConfigs.final_batch_config
  }
}

task CreateSampleManifest {
  input {
    Array[String] cram_paths
    Array[String] sample_ids
    String        rp
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
    File reprocessing_manifest = "~{rp}_sample_processing_manifest.txt"
  }
}


task CreateConfigs {
  input {
    File ref_dragen_config
    File ref_batch_config
    String output_bucket
    String rp
    String data_type
    File sample_manifest
  }

  command <<<
    # writes output_dir to ref_dragen_config
    # writes data_type (cram, bge) to ref_batch_config

    OUTPUT_PATH=~{output_bucket}/~{rp}

    # now overwrite __OUT_PATH__ with OUTPUT_PATH
    sed 's|__OUT_PATH__|'"$OUTPUT_PATH"'|g;
        ' ~{ref_dragen_config} > dragen_config.json

    # now overwrite __DATA_TYPE__ with data_type
    sed 's|__DATA_TYPE__|'"~{data_type}"'|g;
        ' ~{ref_batch_config} > batch_config.json

    # now overwrite __INPUT_LIST__ with reprocessing_manifest
    sed 's|__INPUT_LIST__|'"~{sample_manifest}"'|g;
        ' ~{ref_batch_config} > batch_config.json
  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
  output {
    File final_dragen_config = "dragen_config.json"
    File final_batch_config = "batch_config.json"
  }
}


task StartDragen {
  input {
      File    ref_trigger
      File    dragen_config
      File    batch_config
      String  project_id
      String  data_type
      String  dragen_version
      File    sample_manifest
  }

  command <<<

    # copy files to dragen project to trigger batch jobs - per sample
    gsutil cp ~{dragen_config} "gs://~{project_id}-config/"
    gsutil cp ~{batch_config} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{ref_trigger} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{sample_manifest} "gs://~{project_id}-trigger/~{data_type}/input_list/~{sample_manifest}"

  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
}