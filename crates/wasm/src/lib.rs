//! The WebAssembly binding of the kvgit core, loaded by the TypeScript
//! package.

use wasm_bindgen::prelude::*;

/// The version of the core, which is the version of the package.
#[wasm_bindgen]
pub fn version() -> String {
    kvgit_core::version().to_string()
}
