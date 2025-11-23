fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_protos(
            &["./protobufs/counter.proto", "./protobufs/raft.proto"],
            &["./protobufs"],
        )
        .expect("Failed to compile protobuf files");

    Ok(())
}
