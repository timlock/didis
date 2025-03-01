use std::env;
use std::path::PathBuf;
use std::process::Command;
use bindgen;

fn main() {
    if cfg!(target_os = "linux") {
        generate_io_uring_bindings()
    } else if cfg!(target_os = "macos") {
        generate_kqueue_bindings()
    }
}

fn generate_kqueue_bindings() {
    let bindings = bindgen::Builder::default()
        .header("wrapper_kqueue.h")
        .allowlist_function("kevent")
        .allowlist_function("kqueue")
        .allowlist_var("EVFILT_.*")
        .allowlist_var("EV_.*")
        .generate()
        .map_err(|err| println!("{:?}", err))
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");

}

fn generate_io_uring_bindings() {
    println!("cargo:rustc-link-search=native=/usr/lib");
    println!("cargo:rustc-link-lib=dylib=uring");
    println!("cargo:rerun-if-changed=wrapper_io_uring.h");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    let extern_c_path = env::temp_dir().join("bindgen").join("extern.c");

    // Generate bindings using command-line bindgen
    let bindgen_output = Command::new("bindgen")
        .arg("--experimental")
        .arg("--wrap-static-fns")
        .arg("wrapper_io_uring.h")
        .arg("--output")
        .arg(out_path.join("bindings.rs"))
        .output()
        .expect("Failed to generate bindings");

    if !bindgen_output.status.success() {
        panic!(
            "Could not generate bindings:\n{}",
            String::from_utf8_lossy(&bindgen_output.stderr)
        );
    }

    // Compile the generated wrappers
    let gcc_output = Command::new("gcc")
        .arg("-c")
        .arg("-fPIC")
        .arg("-I/usr/include")
        .arg("-I.")
        .arg(&extern_c_path)
        .arg("-o")
        .arg(out_path.join("extern.o"))
        .output()
        .expect("Failed to compile C code");

    if !gcc_output.status.success() {
        panic!(
            "Failed to compile C code:\n{}",
            String::from_utf8_lossy(&gcc_output.stderr)
        );
    }

    // Create a static library for the wrappers
    let ar_output = Command::new("ar")
        .arg("crus")
        .arg(out_path.join("libextern.a"))
        .arg(out_path.join("extern.o"))
        .output()
        .expect("Failed to create static library");

    if !ar_output.status.success() {
        panic!(
            "Failed to create static library:\n{}",
            String::from_utf8_lossy(&ar_output.stderr)
        );
    }

    // Tell cargo where to find the new library
    println!("cargo:rustc-link-search=native={}", out_path.display());
    println!("cargo:rustc-link-lib=static=extern");
}
