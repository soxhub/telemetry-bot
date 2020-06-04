fn main() {
    // The `refinery` library does not automatically detect new files in the db/migrations directory
    println!("cargo:rerun-if-changed=db/migrations");
}
