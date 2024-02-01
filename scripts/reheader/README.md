# Reheader CRAM Workflow (reheader_cram)

### Overview
The reheader_cram workflow is designed for reheadering CRAM files. It allows users to modify the sample name in the header of a CRAM file using a reference genome. This workflow is particularly useful in genomic data processing where sample names need to be updated or corrected.

### Workflow Description
The workflow takes an input CRAM file and replaces the old sample name in the CRAM header with a new sample name. It relies on SAMtools for manipulating the CRAM file's header. The output of the workflow includes the reheadered CRAM file, its index (CRAI), and an MD5 checksum file for validation.

### Inputs
	•	input_cram: The CRAM file to be reheadered.
	•	old_sample: The original sample name in the CRAM file's header.
	•	new_sample: The new sample name to be written into the CRAM file's header.
	•	ref_fasta: Reference genome FASTA file. Necessary for indexing the CRAM file and ensuring cost efficiency.
	•	ref_fasta_index: Index for the reference genome FASTA file.

### Outputs
	•	cram_path: The path to the new CRAM file with the updated header.
	•	crai_path: The path to the index file of the new CRAM file.
	•	md5_path: The path to the MD5 checksum file for the new CRAM file.

### Cost Efficiency
Using the ref_fasta and ref_fasta_index is essential for cost efficiency. These inputs help optimize the indexing process and reduce computational resources.

### Reference Genomes on Terra
If you do not have the reference genome files (ref_fasta and ref_fasta_index) readily available, Terra provides a variety of reference genomes. You can find these under the "Reference Data" tab in the "Data" section of Terra. Utilizing these pre-existing resources can save time and ensure consistency in genomic analysis.

### Usage
The workflow is designed to be run through Terra — it can be easily imported through the Dockstore integrated functionality. Assuming that the data table will only contain instances of either the old or the new sample, it will be necessary for users to add a column with the missing sample data, run the workflow, and then delete whichever column is no longer necessary. The old CRAM, CRAI, and MD5 files will also have to be manually deleted. This is to ensure that all files are modified correctly according to your needs — giving you the opportunity to verify the workflow’s results before moving forward.

### Contact
Juan Pablo Ramos Barroso at the Broad Institute’s Data Science Platform. Reachable at jramosba@broadinstitute.org or through GitHub @jpramosbarroso
