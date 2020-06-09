fn main() {
    // The `refinery` library does not automatically detect new files in the migrations directory
    println!("cargo:rerun-if-changed=migrations");
}
