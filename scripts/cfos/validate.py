"""Validate dataframes with validators."""
from pandas_schema import*
from pandas_schema.validation import*

DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA = ps.Schema([Column("donor_id", [null_validation]),
                                                   Column("hasDonorAge", [IsDtypeValidation(int)]),
                                                   Column("has_phenotypic_sex", [InListValidation(["Male", "Female", "Intersex"])]),
                                                   Column("age_at_biopsy", [IsDtypeValidation(int)]),
                                                   Column("hasOrganismType", [InListValidation(["C elegans", "Canis lupus familiaris (dog)",
                                                                                                "Ciona intestinalis (Ascidian)", "Danio rerio (zebrafish)",
                                                                                                "Drosophila melanogaster (fruit fly)", "Homo sapiens (human)",
                                                                                                "Mus musculus (mouse)", "Oryctolagus cuniculus (rabbit)",
                                                                                                "Paracentrotus lividus (Rock sea urchin)", "Rattus norvegicus (rat)",
                                                                                                "Sphaerechinus granularis (Violet sea urchin)"])]),
                                                   # TODO: are there a list of sites, or is this a free form field
                                                   Column("hasAnatomicalSite", [None]),
                                                   Column("neuropathology", [InListValidation(["Abeta", "AbetaTau", "None reported"])]),
                                                   Column("biosample_id", [None]),
                                                   Column("hasBioSampleType", [InListValidation(["CellLine", "Derived_In vitro differentiated", "Derived_Induced pluripotent stem cell",
                                                                                                 "Derived_Organoid", "BodyFluid_Amniotic fluid", "Blood_Buffy coat", "Blood_Erythrocyte",
                                                                                                 "Blood_Leukocyte", "Blood_PBMC", "Blood_Plasma", "Blood_Platelet", "Blood_Serum",
                                                                                                 "Blood_Whole Blood", "BodyFluid_BreastMilk", "BodyFluid_Cerebrospinal fluid",
                                                                                                 "BodyFluid_Saliva", "BodyFluid_Semen", "BodyFluid_Synovial fluid", "BodyFluid_Urine",
                                                                                                 "BodyFluid_Vaginal secretions", "Cell free DNA", "Cell_BCell", "Cell_Lymphocyte",
                                                                                                 "Cell_Monocyte", "Cell_T cell", "Primary Culture", "Stool", "Tissue"])]),
                                                   Column("MMSE_Biopsy_value", [IsDtypeValidation(int)]),
                                                   Column("MMSE_Biopsy_atDonorAge", [IsDtypeValidation(int)]),
                                                   Column("MMSE_final_value", [IsDtypeValidation(int)]),
                                                   Column("MMSE_final_atDonorAge", [IsDtypeValidation(int)]),
                                                   Column("APOE_value", [IsDtypeValidation(int)]),
                                                   Column("APOE_atDonorAge", [IsDtypeValidation(int)]),
                                                   Column("DataModality", [InListValidation(["Epigenomic", "Epigenomic_3D Contact Maps", "Epigenomic_DNABinding",
                                                                                             "Epigenomic_DNABinding_HistoneModificationLocation", "Epigenomic_DNABinding_TranscriptionFactorLocation",
                                                                                             "Epigenomic_DNAChromatinAccessibility", "Epigenomic_DNAMethylation",
                                                                                             "Epigenomic_RNABinding", "Genomic", "Genomic_Assembly", "Genomic_Exome",
                                                                                             "Genomic_Genotyping_Targeted", "Genomic_WholeGenome", "Imaging_Electrophysiology",
                                                                                             "Imaging_Microscopy", "Medical imaging _CTScan", "Medical imaging _Electrocardiogram",
                                                                                             "Medical imaging _MRI", "Medical imaging _Xray", "Metabolomic", "Microbiome",
                                                                                             "Metagenomic", "Proteomic", "Transcriptomic", "SpatialTranscriptomics",
                                                                                             "Trascriptomic_Targeted", "Trascriptomic_NonTargeted"])]),
                                                   Column("summary_file_path", [MatchesPatternValidation(r"^gs://*$")]),
                                                   Column("features_file_path", [MatchesPatternValidation(r"^gs://*$")]),
                                                   Column("matrix_file_path", [MatchesPatternValidation(r"^gs://*$")]),
                                                   Column("barcode_file_path", [MatchesPatternValidation(r"^gs://*$")]),
                                                   Column("biosample_id_of_file", [MatchesPatternValidation(r"^gs://*$")]),
                                                   Column("library_id", [None]),
                                                   Column("UMI_threshold", [IsDtypeValidation(int)]),
                                                   Column("follow_up_years", [IsDtypeValidation(int)]),
                                                   Column("has_disease", [InListValidation(["y", "n"])]),
                                                   Column("comment", [None])
                ])

# column values cannot be null
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

# file path columns must start with "gs://"









