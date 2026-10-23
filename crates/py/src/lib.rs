//! The Python binding of the kvgit core, imported as `kvgit._core`.

use pyo3::prelude::*;

#[pymodule]
mod _core {
    use pyo3::prelude::*;

    /// The version of the core, which is the version of the package.
    #[pyfunction]
    fn version() -> &'static str {
        kvgit_core::version()
    }
}
