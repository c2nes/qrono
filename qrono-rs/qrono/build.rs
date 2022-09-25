fn main() {
    tonic_build::configure()
        .out_dir("src/grpc/generated")
        .file_descriptor_set_path("src/grpc/generated/qrono.bin")
        .compile(&["proto/qrono.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
