version 1.0

workflow StartDragenWorkflow {
  input {
      File ref_trigger
      File ref_dragen_config
      File ref_batch_config
      File ref_input
      String trigger_path
      String dragen_config_path
      String batch_config_path
      String input_path
      Array[String] cram_paths
      Array[String] sample_ids

  }
  call StartDragen {
    input:
        ref_trigger = ref_trigger,
        ref_dragen_config = ref_dragen_config,
        ref_batch_config = ref_batch_config,
        ref_input = ref_input,
        trigger = trigger_path,
        dragen_config = dragen_config_path,
        batch_config = batch_config_path,
        input_path = input_path,
        cram_paths = cram_paths,
        sample_ids = sample_ids
  }
  output {
  }
}

task StartDragen {
  input {
      File ref_trigger
      File ref_dragen_config
      File ref_batch_config
      File ref_input
      String trigger
      String dragen_config
      String batch_config
      String input_path
      Array[String] cram_paths
      Array[String] sample_ids
  }

  command <<<
      gsutil cp ~{ref_dragen_config} ~{dragen_config}
      gsutil cp ~{ref_batch_config} ~{batch_config}
      gsutil cp ~{ref_input} ~{input_path}
      gsutil cp ~{ref_trigger} ~{trigger}
  >>>
  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk:305.0.0"
  }
  output {
  }
}

    # need to create input txt file of this format:
    # collaborator_sample_id	cram_file_ref
    # ROS_008_18Y03160_D1	gs://<bucket>/ROS_008_18Y03160_D1.cram
    # ROS_015_19Y05447_D1	gs://<bucket>/ROS_015_19Y05447_D1.cram
    # ROS_010_10X03711_D1	gs://<bucket>/ROS_010_10X03711_D1.cram
