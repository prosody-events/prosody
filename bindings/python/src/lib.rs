use pyo3::prelude::*;

#[pyfunction]
fn hello() -> String {
    "hello Python".to_string()
}

#[pymodule]
fn prosody(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;

    Ok(())
}
