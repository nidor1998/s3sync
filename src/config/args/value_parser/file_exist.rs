use std::path::PathBuf;

pub fn is_file_exist(file_path: &str) -> Result<String, String> {
    let file_path = PathBuf::from(file_path);

    if file_path.exists() && file_path.is_file() {
        Ok(file_path.to_string_lossy().to_string())
    } else {
        Err(format!("File does not exist: {}", file_path.display()))
    }
}
