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

    String  monitoring_script = "gs://broad-dsde-methods-tbrookin/cromwell_monitoring_script2.sh"
  }  

  command {
    set -e -o pipefail

    # regex to determine if input string is valid URL
    regex='(https?|ftp|file)://[-[:alnum:]\+&@#/%?=~_|!:,.;]*[-[:alnum:]\+&@#/%=~_|]'

    # monitoring script
    if [[ "${monitoring_script}" =~ $regex ]]
    then
      echo -e "Monitoring script from URL source."
      curl "${monitoring_script}" > monitoring_script.sh
      chmod +x monitoring_script.sh
      bash monitoring_script.sh > monitoring.log &
    else
      echo -e "Monitoring script from STRING/FILE source."
      echo -e "~{monitoring_script}" > monitoring_script.sh
      chmod +x monitoring_script.sh
      bash monitoring_script.sh > monitoring.log &
    fi
  
    # user input shell commands
    if [[ "${shell_commands}" =~ $regex ]]
    then
      echo -e "Shell commands from URL source."
      curl "${shell_commands}" > shell_script.sh
      chmod +x shell_script.sh
      bash shell_script.sh 2>&1 | tee log.txt
    else
      echo -e "Shell commands from STRING/FILE source."
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