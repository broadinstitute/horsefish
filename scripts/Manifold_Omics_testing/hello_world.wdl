version 1.0

workflow hello_world {
    call say_hello
    output {
        String message = say_hello.out
    }
}

task say_hello {
    command <<<
        echo "Hello world WDL running on Omics using GitHub!"
    >>>
    output {
        String out = read_string(stdout())
    }
    runtime {
        docker: "amazonlinux:2"
    }
}
