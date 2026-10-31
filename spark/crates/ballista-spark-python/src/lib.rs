//! Embedded Python for Spark-enabled Ballista processes.
//!
//! The Spark front end and Spark operators call into Python (PySpark UDFs,
//! Python data sources), so every process that plans or runs Spark plans
//! embeds a Python interpreter. Ported from Sail's CLI entry point.
//!
//! Call [`bootstrap`] first thing in `main`, before any threads exist (in
//! particular before starting a Tokio runtime): it sets an environment
//! variable and initializes the interpreter.

use std::ffi::NulError;

use pyo3::Python;
use pyo3::ffi::{Py_Main, PyUnicode_AsWideCharString, PyUnicode_FromString};
use sail_common::config::{CliConfig, CliConfigEnv};

/// Prepares embedded Python for this process.
///
/// Python's `multiprocessing` starts helper processes with `sys.executable`,
/// which for an embedded interpreter is this binary. Such child processes see
/// `SAIL_INTERNAL__RUN_PYTHON` and run as a plain Python interpreter instead,
/// so this function never returns in them.
pub fn bootstrap() -> Result<(), Box<dyn std::error::Error>> {
    let config = CliConfig::load()?;
    if config.run_python {
        run_python_interpreter();
    }
    // SAFETY: called at process start, before any other threads exist.
    unsafe { std::env::set_var(CliConfigEnv::RUN_PYTHON, "true") };
    Python::initialize();
    Ok(())
}

/// Runs this process as the Python interpreter with the process arguments.
fn run_python_interpreter() -> ! {
    let args = std::env::args();
    let argc = args.len() as i32;
    let Ok(mut argv) = args
        .map(|arg| {
            let arg = std::ffi::CString::new(arg)?;
            // SAFETY: `arg` is a valid NUL-terminated string; Python copies it.
            let arg = unsafe {
                let obj = PyUnicode_FromString(arg.as_ptr());
                PyUnicode_AsWideCharString(obj, std::ptr::null_mut())
            };
            Ok(arg)
        })
        .collect::<Result<Vec<_>, NulError>>()
    else {
        eprintln!("Error: null bytes found in command line argument strings");
        std::process::exit(1);
    };
    argv.push(std::ptr::null_mut());
    // SAFETY: `argv` is a NULL-terminated array of wide strings owned by Python.
    let code = unsafe { Py_Main(argc, argv.as_mut_ptr()) };
    std::process::exit(code)
}
