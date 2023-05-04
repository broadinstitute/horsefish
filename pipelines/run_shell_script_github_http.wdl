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
  }  

  command {
    # determine if input is url to script or single string bash command
    regex='(https?|ftp|file)://[-[:alnum:]\+&@#/%?=~_|!:,.;]*[-[:alnum:]\+&@#/%=~_|]'
    if [[ $shell_commands =~ $regex ]]
    then
      curl ~{shell_commands} > shell_script.sh
      chmod +x shell_script.sh
      ./shell_script.sh 2>&1 | tee log.txt
    else
      # remove bash
      ~{shell_commands} 2>&1 | tee log.txt
    fi

    # write out contents of user input to file
    echo "$(<$shell_commands)" > executed_commands.txt
  }
  runtime {
    docker: docker
    memory: memory + " GiB"
    cpu: cpu
    disks: "local-disk " + disk_size + if use_ssd then " SSD" else " HDD"
    preemptible: select_first([preemptible_attempts, 3])
  }
  output {
    File log_file   = "log.txt"
    File commands    = "executed_commands.txt"
  }
}