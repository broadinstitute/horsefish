version 1.0

# Ingests data into TDR
#

workflow IngestToTDR {

  input {
    File samples
    String tdrid
    String instance_type
    Boolean verbose = False
  }
  
  call Ingest {
    input:
      path = samples,
      tdrid = tdrid,
      instance_type = instance_type
      verbose = verbose
  }
  output{
    File err = Ingest.message
    File stdout = Ingest.stdmessage
  }
}

task Ingest {
  input {
    File path
    String tdrid
    String instance_type
    Boolean verbose
  }
  
  command {
      python3 /scripts/bacterial_genomics/ingest.py  -f ${path} -d ${tdrid} -i ${instance_type} -v ${verbose}
  }
  output{
     File message = read_string(stderr())
     File stdmessage = read_string(stdout())
  }

  runtime {
    docker: "docker.io/broadinstitute/horsefish:bacterialingest_v1.0.9-beta"
    continueOnReturnCode: true
  }
}

