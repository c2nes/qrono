fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    tonic_build::configure()
        .file_descriptor_set_path(format!("{}/qrono.pb.bin", out_dir))
        .compile(&["proto/qrono.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
