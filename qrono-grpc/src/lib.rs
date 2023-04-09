tonic::include_proto!("qrono");

pub static DESCRIPTOR: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/qrono.pb.bin"));
