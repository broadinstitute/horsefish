version 1.0

# Ingests data into TDR
#

workflow IngestToTDR {

  input {
    File samples
    String tdrid
    String instance_type
  }
  
  call Ingest {
    input:
      path = samples,
      tdrid = tdrid,
      instance_type = instance_type
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
  }
  
  command {
      python3 /scripts/bacterial_genomics/ingest.py  -f ${path} -d ${tdrid} -i ${instance_type}
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

