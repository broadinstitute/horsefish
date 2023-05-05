version 1.0

workflow RunShellScript {
  input {
    String shell_commands
  }
    
  call run_shell_script {
    input: 
      shell_commands  = shell_commands
  }

  # Outputs that will be retained when execution is complete  
  output {
    File log_file = run_shell_script.log_file
    File input_script = run_shell_script.commands
    File monitor_log  = run_shell_script.monitoring_log
  } 
}

# TASK DEFINITIONS
task run_shell_script {
  input {
    String  shell_commands
    Int     disk_size = 50
    Boolean use_ssd = false  
    Int     cpu = 4
    Int     memory = 8
    Int?    preemptible_attempts
    String  docker = "google/cloud-sdk:latest"

    File    monitoring_script = "gs://fc-51792410-8543-49ba-ad3f-9e274900879f/cromwell_monitoring_script2.sh"
  }  

  command {
    set -e -o pipefail
    bash ~{monitoring_script} > monitoring.log &
    # determine if input is url to script or single string bash command
    regex='(https?|ftp|file)://[-[:alnum:]\+&@#/%?=~_|!:,.;]*[-[:alnum:]\+&@#/%=~_|]'

    # url=~{shell_commands}
    if [[ "${shell_commands}" =~ $regex ]]
    then
      echo -e "Entering bash SCRIPT block."
      curl "${shell_commands}" > shell_script.sh
      chmod +x shell_script.sh
      bash shell_script.sh 2>&1 | tee log.txt
    else
      echo -e "Entering bash COMMAND block."
      echo -e "~{shell_commands}" > shell_script.sh
      chmod +x shell_script.sh
      bash shell_script.sh 2>&1 | tee log.txt
    fi
  }
  runtime {
    docker: docker
    memory: memory + " GiB"
    cpu: cpu
    disks: "local-disk " + disk_size + if use_ssd then " SSD" else " HDD"
    preemptible: select_first([preemptible_attempts, 3])
  }
  output {
    File log_file       = "log.txt"
    File commands       = "shell_script.sh"
    File monitoring_log = "monitoring.log"
  }
}