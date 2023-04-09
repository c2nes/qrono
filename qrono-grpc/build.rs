fn main() {
    tonic_build::configure()
        .file_descriptor_set_path("../qrono_descriptor.pb.bin")
        .compile(&["proto/qrono.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
