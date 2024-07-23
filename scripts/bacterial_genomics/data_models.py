""" Datable models to be ingested into TDR. Key values represent table names and key values are column names """

" Data models that are created from the sample entity"

isolates_instance = {
    "tables": [
        {
            "name": "Sample",
            "columns": ["entity:sample_id", "sent_to_broad", "broad_ship_date", "sent_to_dzd_pre-p01", "dzd_pre-p01_ship_date", "sent_to_dzd_for_p01", "uci_regrowth",
               "uci_regrowth_outcome", "uci_id", "raw_reads1", "raw_reads2", "dzd_plate", "dzd_batch", "dzd_id", "dzd_rack", "dzd_rack_row",
               "dzd_rack_column", "dzd_rack_position", "dzd_glycerol_rack_code", "mbs", "duplication_rate", "gc", "fail_contam", "fail_mb", "swab_id", "resident_id", 
               "culture_id", "stock_id"
               ],
            # Renames column names
            "rename": 
                { "entity:sample_id": "sample_id",
                 "sent_to_dzd_pre-p01": "sent_to_dzd_pre_p01",
                 "dzd_pre-p01_ship_date": "dzd_pre_p01_ship_date"
                 }
        },     
        {
            "name": "Culture",
            "columns": ["culture_id", "organism", "swab_id"]
        },
        {
            "name": "Swab",
            "columns": ["swab_id", "site", "resident_id"]
         },
        {
            "name":  "Stock",
            "columns": ["stock_id", "species", "morphotype_indicator", "freezer_box_name", "freezer_box_number", "freezer_box_column", "freezer_box_row"],
        },
        {
            "name": "Resident",
            "columns": ["resident_id", "record_id", "project", "nh_code", "room_bed", "CLEAN_object_code", "CLEAN_phase", "CLEAN_sweep", "room_type"],
        },
        {
            "name": "isolate",
             "columns": [ "dzd_id", "raw_reads1", "raw_reads2", "read1_clean", "read2_clean", "entity:sample_id"],
            "rename": { "entity:sample_id": "sample_id"}
        },
    ],   
}

" Metadata that are created from the plate swipe entity"
plate_swipes_instance = {
    "tables": [
        {
            "name": "plate_swipe",
            "columns": [ "amrfinderplus_all_report", "amrfinderplus_amr_classes", "amrfinderplus_amr_core_genes", 
                    "amrfinderplus_amr_plus_genes", "amrfinderplus_amr_report", "amrfinderplus_amr_subclasses", "amrfinderplus_db_version", 
                    "amrfinderplus_stress_genes", "amrfinderplus_stress_report", "amrfinderplus_version", "amrfinderplus_virulence_genes", 
                    "amrfinderplus_virulence_report", "analysis_date", "assembly_fasta", "assembly_length", "assembly_mean_coverage", 
                    "average_read_length", "bakta_gbff", "bakta_gff3", "bakta_summary", "bakta_tsv", "bakta_version", "bbduk_docker", 
                    "bedtools_docker", "bedtools_version", "bracken_report", "bracken_version", "contig_number", "culture_id", "duplication_rate", 
                    "dzd_batch", "dzd_id", "dzd_plate", "fail_mb", "fastq_scan_docker", "fastq_scan_version", "freezer_box_column", "freezer_box_number", 
                    "freezer_box_row", "gc", "gemstone_wf_version", "kraken2_docker", "kraken2_percent_human", "kraken2_report", "kraken2_version", 
                    "largest_contig", "matched_isolate_genera", "mbs", "metaspades_docker", "metaspades_version", "metawrap_binning_flags", "metawrap_contigs", 
                    "metawrap_docker", "metawrap_fasta", "metawrap_n_bins", "metawrap_stats", "metawrap_version", "minimap2_docker", "minimap2_version", 
                    "mob_recon_chromosome_fasta", "mob_recon_docker", "mob_recon_plasmid_fastas", "mob_recon_results", "mob_recon_version", "mob_typer_results", 
                    "ncbi_scrub_docker", "nh_code", "num_reads_clean1", "num_reads_clean2", "num_reads_clean_pairs", "num_reads_raw1", "num_reads_raw2", 
                    "num_reads_raw_pairs", "organism", "percent_coverage", "percentage_mapped_reads", "pilon_docker", "pilon_version", "quast_docker", 
                    "quast_version", "raw_reads1", "raw_reads2", "read1_clean", "read1_dehosted", "read1_mapped", "read1_unmapped", "read2_clean", 
                    "read2_dehosted", "read2_mapped", "read2_unmapped", "record_id", "resident_id", "samtools_docker", "samtools_version", "sent_to_broad", 
                    "sent_to_dzd_for_p01", "sent_to_dzd_pre-p01", "site", "species", "stock_id", "strainge_docker", "strainge_pe_wf_analysis_date", 
                    "strainge_pe_wf_version", "strainge_version", "straingr_concat_fasta", "straingr_read_alignment", "straingr_report", "straingr_variants", 
                    "straingst_found_db", "straingst_kmerized_reads", "straingst_selected_db", "straingst_statistics", "straingst_strains", "swab_id", 
                    "trimmomatic_docker", "trimmomatic_version", "entity:sample_id"
                    ],
            # Renames column names
            "rename":                     
                { "entity:sample_id": "sample_id",
                    "sent_to_dzd_pre-p01": "sent_to_dzd_pre_p01"}
        }
    ]
}
