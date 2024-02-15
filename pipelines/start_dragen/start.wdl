version 1.0

workflow StartDragenWorkflow {
  input {
    String        ref_trigger
    String        ref_dragen_config
    String        ref_batch_config
    String        ref_input
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

  call StartDragen {
    input:
      ref_trigger       = ref_trigger,
      ref_dragen_config = ref_dragen_config,
      ref_batch_config  = ref_batch_config,
      ref_input         = ref_input,
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

task StartDragen {
  input {
      String  ref_trigger
      String  ref_dragen_config
      String  ref_batch_config
      String  ref_input
      String  project_id
      String  data_type
      String  dragen_version
      File    sample_manifest
  }

  command <<<

    # copy files to dragen project to trigger batch jobs - per sample
    gsutil cp ~{ref_dragen_config} "gs://~{project_id}-config/"
    gsutil cp ~{ref_batch_config} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{ref_input} "gs://~{project_id}-trigger/~{data_type}/input_list/"
    gsutil cp ~{ref_trigger} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
    gsutil cp ~{sample_manifest} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"

  >>>

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }

}
