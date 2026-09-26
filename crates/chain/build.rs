fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a vendored protoc so builds don't need a system install.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["proto/service.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
