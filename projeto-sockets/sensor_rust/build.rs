fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate from the same schema used by every sensor and the gateway.
    // A vendored protoc also makes the local build independent of system packages.
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    prost_build::compile_protos(&["../common/messages.proto"], &["../common"])?;
    println!("cargo:rerun-if-changed=../common/messages.proto");
    println!("cargo:rerun-if-env-changed=PROTOC");
    Ok(())
}
