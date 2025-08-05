version 1.0

workflow github_proof_of_concept {
    input {
        String repo_url
    }

    call clone_and_list { input: repo_url = repo_url }
    
    output {
        File repo_listing = clone_and_list.repo_listing
    }
}

task clone_and_list {
    input {
        String repo_url
    }

    command <<<
        set -eux
        git clone ~{repo_url} repo
        ls -l repo > listing.txt
    >>>

    output {
        File repo_listing = "listing.txt"
    }

    runtime {
        docker: "alpine/git"
    }
}
