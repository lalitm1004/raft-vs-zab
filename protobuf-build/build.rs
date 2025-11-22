fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_protos(&["./protobufs/counter_service.proto"], &["./protobufs"])
        .expect("Failed to compile protobuf files");

    Ok(())
}
