version 1.0

workflow StartDragenWorkflow {
  input {
      String ref_trigger
      String ref_dragen_config
      String ref_batch_config
      String ref_input
      String project_id
      String data_type
      String version
      Array[String] cram_paths
      Array[String] sample_ids

  }
  call StartDragen {
    input:
        ref_trigger = ref_trigger,
        ref_dragen_config = ref_dragen_config,
        ref_batch_config = ref_batch_config,
        ref_input = ref_input,
        PROJECT_ID = project_id,
        DATA_TYPE = data_type,
        VERSION = version,
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
      String PROJECT_ID
      String DATA_TYPE
      String VERSION
      Array[String] cram_paths
      Array[String] sample_ids
  }

  command <<<
      gsutil cp ~{ref_dragen_config} "gs://~{PROJECT_ID}-config/"
      gsutil cp ~{ref_batch_config} "gs://~{PROJECT_ID}-trigger/~{DATA_TYPE}/~{VERSION}/"
      gsutil cp ~{ref_input} "gs://~{PROJECT_ID}-trigger/~{DATA_TYPE}/input_list/"
      gsutil cp ~{ref_trigger} "gs://~{PROJECT_ID}-trigger/~{DATA_TYPE}/${VERSION}/"
  >>>
  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
  output {
  }
}
