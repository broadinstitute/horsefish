version 1.0

workflow RunShellScript {
  input {
    File shell_script
  }
    
  call run_shell_script {
    input: 
      shell_script  = shell_script
  }

  # Outputs that will be retained when execution is complete  
  output {
    File log_file = run_shell_script.log_file
  } 
}

# TASK DEFINITIONS

task run_shell_script {
  input {
    File    shell_script
    Int     disk_size = 50
    Int     cpu = 4
    Int     memory = 8
    String  docker = "google/cloud-sdk:latest"
  }  

  command {
    chmod +x ~{shell_script}
    bash ~{shell_script} 2>&1 | tee log.txt
  }
  runtime {
    docker: docker
    memory: memory + " GiB"
    cpu: cpu
    disks: "local-disk " + disk_size + " HDD"
  }
  output {
    File log_file = "log.txt"
  }
}