fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("proto/provider.proto")?;
    println!("cargo:rerun-if-changed=proto/provider.proto");
    Ok(())
}
