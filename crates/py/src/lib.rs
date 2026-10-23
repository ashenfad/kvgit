//! The Python binding of the kvgit core, imported as `kvgit._core`.

use pyo3::prelude::*;

#[pymodule]
mod _core {
    use pyo3::prelude::*;

    /// The version of the extension, which is the version of the package.
    #[pyfunction]
    fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}
