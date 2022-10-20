version 1.0

workflow DeliverPrstoAnVIL {
    meta {
        description: "Gather all deliverable files for selected PRS samples, rename, and deliver to final AnVIL data delivery workspace."
    }
    String pipeline_version = "1.0.1"

    input {
        Array[String]   prs_entity_ids
        
        # source workspace - staging
        String          src_workspace_name
        String          src_workspace_namespace

        # destination workspace - anvil delivery workspace
        String          dest_workspace_name
        String          dest_workspace_namespace
        String          dest_workspace_bucket_id

        # requester pays
        String?         project_id
    }

    call DeliverPrsDatatoAnVILWorkspace {
        input:
        prs_entity_ids              = prs_entity_ids,
        src_workspace_name          = src_workspace_name,
        src_workspace_namespace     = src_workspace_namespace,
        dest_workspace_name         = dest_workspace_name,
        dest_workspace_namespace    = dest_workspace_namespace,
        dest_workspace_bucket_id    = dest_workspace_bucket_id,
        project_id                  = project_id
    }

    output {
        File prs_scores_tsv = DeliverPrsDatatoAnVILWorkspace.prs
        File samples_tsv    = DeliverPrsDatatoAnVILWorkspace.samples
        File arrays_tsv     = DeliverPrsDatatoAnVILWorkspace.arrays
    }
}

task DeliverPrsDatatoAnVILWorkspace {
    input {
        Array[String]   prs_entity_ids
        String          src_workspace_name
        String          src_workspace_namespace
        String          dest_workspace_name
        String          dest_workspace_namespace
        String          dest_workspace_bucket_id
        String?         project_id
    }

    command {
        python3 /scripts/emerge/stage_prs_anvil_deliverable_data.py -f ${write_lines(prs_entity_ids)} \
                                                                    -sw ~{src_workspace_name} \
                                                                    -sn ~{src_workspace_namespace} \
                                                                    -dw ~{dest_workspace_name} \
                                                                    -dn ~{dest_workspace_namespace} \
                                                                    -db ~{dest_workspace_bucket_id} \
                                                                    ~{"-p=" + project_id}
    }

    runtime {
        docker: "broadinstitute/horsefish"
    }

    output {
        File arrays     = "arrays.tsv"
        File samples    = "samples.tsv"
        File prs        = "prs_scores.tsv"
    }
}