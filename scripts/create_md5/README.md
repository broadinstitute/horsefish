# Create MD5 Workflow (create_md5)

### Overview
The create_md5 workflow is designed for generating MD5 checksum files for any type of files. It allows users to create an MD5 checksum for file validation purposes. This workflow is particularly useful in data integrity assurance across file transfers and storage.

### Workflow Description
The workflow takes an input file and generates its respective MD5 checksum file. This operation relies on standard utilities available in most environments that support shell commands, specifically using md5sum. The output of the workflow includes the generated MD5 checksum file.

### Inputs
	•	input_file: The file for which the MD5 checksum is to be generated.

### Outputs
	•	md5_path: The path to the generated MD5 checksum file.

### Contact
Juan Pablo Ramos Barroso at the Broad Institute’s Data Science Platform. Reachable at jramosba@broadinstitute.org or through GitHub @jpramosbarroso
