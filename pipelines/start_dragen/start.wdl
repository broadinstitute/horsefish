version 1.0

workflow StartDragenWorkflow {
  input {
      String ref_trigger
      String ref_dragen_config
      String ref_batch_config
      String ref_input
      String project_id
      String data_type
      String dragen_version
      Array[String] cram_paths
      Array[String] sample_ids

  }
  call StartDragen {
    input:
        ref_trigger = ref_trigger,
        ref_dragen_config = ref_dragen_config,
        ref_batch_config = ref_batch_config,
        ref_input = ref_input,
        project_id = project_id,
        data_type = data_type,
        dragen_version = dragen_version,
        cram_paths = cram_paths,
        sample_ids = sample_ids
  }
  output {
  }
}

task StartDragen {
  input {
      String ref_trigger
      String ref_dragen_config
      String ref_batch_config
      String ref_input
      String project_id
      String data_type
      String dragen_version
      Array[String] cram_paths
      Array[String] sample_ids
  }

  command <<<
      gsutil cp ~{ref_dragen_config} "gs://~{project_id}-config/"
      gsutil cp ~{ref_batch_config} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
      gsutil cp ~{ref_input} "gs://~{project_id}-trigger/~{data_type}/input_list/"
      gsutil cp ~{ref_trigger} "gs://~{project_id}-trigger/~{data_type}/~{dragen_version}/"
  >>>
  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
  output {
  }
}
