"""Validate dataframes with validators."""
from pandas_schema import*
from pandas_schema.validation import*
import re

# validators
# column values cannot be null
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

# validator options
has_organism_type_options = ["C elegans", "Canis lupus familiaris (dog)", "Ciona intestinalis (Ascidian)",
                             "Danio rerio (zebrafish)", "Drosophila melanogaster (fruit fly)", 
                             "Homo sapiens (human)", "Mus musculus (mouse)", "Oryctolagus cuniculus (rabbit)",
                             "Paracentrotus lividus (Rock sea urchin)", "Rattus norvegicus (rat)",
                             "Sphaerechinus granularis (Violet sea urchin)"]
has_phenotypic_sex_options = ["Male", "Female", "Intersex"]
neuropathology_options = ["Abeta", "AbetaTau", "None reported"]
has_biosample_type_options = ["CellLine", "Derived_In vitro differentiated", "Derived_Induced pluripotent stem cell",
                              "Derived_Organoid", "BodyFluid_Amniotic fluid", "Blood_Buffy coat", "Blood_Erythrocyte",
                              "Blood_Leukocyte", "Blood_PBMC", "Blood_Plasma", "Blood_Platelet", "Blood_Serum",
                              "Blood_Whole Blood", "BodyFluid_BreastMilk", "BodyFluid_Cerebrospinal fluid",
                              "BodyFluid_Saliva", "BodyFluid_Semen", "BodyFluid_Synovial fluid", "BodyFluid_Urine",
                              "BodyFluid_Vaginal secretions", "Cell free DNA", "Cell_BCell", "Cell_Lymphocyte",
                              "Cell_Monocyte", "Cell_T cell", "Primary Culture", "Stool", "Tissue"]
data_modality_options = ["Epigenomic", "Epigenomic_3D Contact Maps", "Epigenomic_DNABinding",
                         "Epigenomic_DNABinding_HistoneModificationLocation", "Epigenomic_DNABinding_TranscriptionFactorLocation",
                         "Epigenomic_DNAChromatinAccessibility", "Epigenomic_DNAMethylation", "Epigenomic_RNABinding", "Genomic",
                         "Genomic_Assembly", "Genomic_Exome", "Genomic_Genotyping_Targeted", "Genomic_WholeGenome",
                         "Imaging_Electrophysiology", "Imaging_Microscopy", "Medical imaging _CTScan", "Medical imaging _Electrocardiogram",
                         "Medical imaging _MRI", "Medical imaging _Xray", "Metabolomic", "Microbiome", "Metagenomic", "Proteomic",
                         "Transcriptomic", "SpatialTranscriptomics", "Trascriptomic_Targeted", "Trascriptomic_NonTargeted"]
has_disease_options = ["y", "n"]

# schema validator definition
# TODO: set required columns
# validation schema by specific column name across any and all possible datasets - not specific to just a single dataset
DATA_TABLE_VALIDATE_AND_FORMAT_SCHEMA = Schema([Column("donor_id", null_validation),
                                                Column("hasDonorAge", [IsDtypeValidation(int)]),
                                                Column("has_phenotypic_sex", [InListValidation(has_phenotypic_sex_options)]),
                                                Column("age_at_biopsy", [IsDtypeValidation(int)]),
                                                Column("hasOrganismType", [InListValidation(has_organism_type_options)]),
                                                # TODO: are there a list of sites, or is this a free form field
                                                #    Column("hasAnatomicalSite", [None])
                                                Column("neuropathology", [InListValidation(neuropathology_options)]),
                                                Column("BioSample_id", [IsDistinctValidation()]),
                                                Column("hasBioSampleType", [InListValidation(has_biosample_type_options)]),
                                                Column("MMSE_Biopsy_value", [IsDtypeValidation(int)]),
                                                Column("MMSE_Biopsy_atDonorAge", [IsDtypeValidation(int)]),
                                                Column("MMSE_Final_value", [IsDtypeValidation(int)]),
                                                Column("MMSE_Final_atDonorAge", [IsDtypeValidation(int)]),
                                                Column("APOE_value", [IsDtypeValidation(int)]),
                                                Column("APOE_atDonorAge", [IsDtypeValidation(int)]),
                                                Column("DataModality", [InListValidation(data_modality_options)]),
                                                # file path columns must start with "gs://"
                                                # TODO: check starts with not just for pattern
                                                Column("summary_file_path", [MatchesPatternValidation("gs://")]),
                                                Column("features_file_path", [MatchesPatternValidation("gs://")]),
                                                Column("matrix_file_path", [MatchesPatternValidation("gs://")]),
                                                Column("barcode_file_path", [MatchesPatternValidation("gs://")]),
                                                Column("biosample_id_of_file", null_validation),
                                                Column("library_id", null_validation),
                                                Column("UMI_threshold", [IsDtypeValidation(int)]),
                                                Column("follow_up_years", [IsDtypeValidation(int)]),
                                                Column("has_disease", [InListValidation(has_disease_options)])
                                                   #    Column("comment", [None])
                                            ])








