use std::path::{Path, PathBuf};

pub(crate) fn with_temp_suffix<P: AsRef<Path>>(path: P) -> PathBuf {
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

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    #[test]
    fn test() {
        assert_eq!(
            PathBuf::from("hello/world.temp"),
            super::with_temp_suffix("hello/world")
        );

        assert_eq!(
            PathBuf::from("hello/world.txt.temp"),
            super::with_temp_suffix("hello/world.txt")
        );

        assert_eq!(
            PathBuf::from("hello.temp"),
            super::with_temp_suffix("hello")
        );

        assert_eq!(
            PathBuf::from("hello.temp"),
            super::with_temp_suffix("hello/")
        );
    }
}
