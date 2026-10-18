//! Media is typed content. No provider source may encode or parse a media
//! text marker such as `[IMAGE:..]` or `[FILE:..]`.

use std::path::Path;

const FORBIDDEN: &[&str] = &[
    "[IMAGE:",
    "[FILE:",
    "[DOCUMENT:",
    "[VIDEO:",
    "[AUDIO:",
    "[VOICE:",
    "[Attachment:",
];

fn production_sources(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read provider dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            production_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs")
            && !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with("_tests.rs"))
        {
            out.push(path);
        }
    }
}

#[test]
fn provider_sources_carry_no_media_text_markers() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/providers");
    let mut files = Vec::new();
    production_sources(&root, &mut files);
    assert!(files.len() > 20, "scanned the provider tree");
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read source");
        for marker in FORBIDDEN {
            assert!(
                !text.contains(marker),
                "{} still encodes the text marker {marker}",
                file.display()
            );
        }
    }
}
