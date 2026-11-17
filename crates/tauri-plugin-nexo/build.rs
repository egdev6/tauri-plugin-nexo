fn main() {
    println!("cargo:rerun-if-changed=plugin-executable.manifest");

    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows")
        && std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("msvc")
    {
        // Workspace feature unification can make this package's test/example
        // executables import Common Controls v6 functions via Tauri/Wry.
        // Canonicalization supplies the extended UNC form required by mt.exe.
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("plugin-executable.manifest")
            .canonicalize()
            .expect("resolve plugin executable manifest");
        println!("cargo:rustc-link-arg=/MANIFEST:EMBED");
        println!("cargo:rustc-link-arg=/MANIFESTINPUT:{}", manifest.display());
    }
}
