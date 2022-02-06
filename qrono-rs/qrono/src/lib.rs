pub mod data;
pub mod encoding;
pub mod ops;
pub mod segment;

pub mod hash;
pub use qrono_channel as channel;
pub use qrono_promise as promise;
pub mod redis;
pub mod scheduler;
pub mod service;
pub mod wal;

pub mod id_generator;
pub mod io;
pub mod timer;
pub mod working_set;

pub mod path {
    use std::path::{Path, PathBuf};

    pub fn with_temp_suffix<P: AsRef<Path>>(path: P) -> PathBuf {
        let path = path.as_ref().to_path_buf();
        match path.file_name() {
            Some(name) => {
                let mut name = name.to_os_string();
                name.push(".temp");
                path.with_file_name(name)
            }
            None => path,
        }
    }
}
