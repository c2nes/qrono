tonic::include_proto!("qrono");

pub static DESCRIPTOR: &'static [u8] = include_bytes!(concat!(env!("OUT_DIR"), "/qrono.pb.bin"));
