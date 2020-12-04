version 1.0

# Example WDL for testing run via Cloud Function.

workflow MyWorkflowName {
  input {
    String aStringParameter
    Array[String] aStringArrayParameter
    Int aNumericParameter
    String aCloudStorageFilePath
  }

  call MyTaskName {
    input:
      aStringParameter = aStringParameter,
      aStringArrayParameter = aStringArrayParameter,
      aNumericParameter = aNumericParameter,
      aCloudStorageFilePath = aCloudStorageFilePath
  }
}

#-------------------------------------------------------------------------------
task MyTaskName {
  input {
    String aStringParameter
    Array[String] aStringArrayParameter
    Int aNumericParameter
    String aCloudStorageFilePath
  }

  String statusOutputFilename = "workflow_status.txt"

  command {
    set -o xtrace
    # For any command failures in this script, return the error.
    set -o errexit
    set -o pipefail
    set -o nounset

    # Make a file with all params.
    echo aStringParameter: "${aStringParameter}" > results.txt
    echo aStringArrayParameter: "${sep=',' aStringArrayParameter}" >> results.txt
    echo aNumericParameter: "${aNumericParameter}" >> results.txt
    echo aCloudStorageFilePath: "${aCloudStorageFilePath}" >> results.txt

    echo As part of the test, run wordcount on the file that triggered the workflow. >> results.txt
    gsutil cat "${aCloudStorageFilePath}" | wc >> results.txt

    # Place details for the workflow provenance in the workspace bucket.
    cp results.txt "${statusOutputFilename}"
  }

  output {
    File statusOutputFile = "${statusOutputFilename}"
  }

  runtime {
    docker: "gcr.io/google.com/cloudsdktool/cloud-sdk"
  }
}
