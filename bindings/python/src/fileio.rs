// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub use ::iceberg::io as icebergiocore;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use crate::error::to_py_err;
use pyo3_async_runtimes::tokio::future_into_py;

#[pyclass]
pub struct FileMetadata {
    core: icebergiocore::FileMetadata,
}

#[pymethods]
impl FileMetadata {
    #[getter]
    pub fn size(&self) -> u64 {
        self.core.size
    }
}

#[pyclass]
pub struct InputFile {
    core: icebergiocore::InputFile,
}

#[pymethods]
impl InputFile {
    #[new]
    #[pyo3(signature = (path))]
    pub fn new(path: String) -> PyResult<Self> {
        let file_io = icebergiocore::FileIOBuilder::new_fs_io().build().unwrap();

        Ok(InputFile {
            core: file_io.new_input(&path).map_err(to_py_err)?
        })
    }

    pub fn location(&self) -> String {
        self.core.location().to_string()
    }

    pub fn exists<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        future_into_py(py, async move {
            this.exists().await.map_err(to_py_err)
        })
    }

    pub fn read<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        future_into_py(py, async move {
            let res = this.read().await.map_err(to_py_err)?.to_vec();
            Python::with_gil(|py| {
                let py_bytes = PyBytes::new(py, &res);
                Ok(py_bytes.to_object(py))
            })
        })
    }

    pub fn metadata<'p>(&'p self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let this = self.core.clone();
        future_into_py(py, async move {
            Ok(FileMetadata {
                core: this.metadata().await.map_err(to_py_err)?
            })
        })
    }
}


pub fn register_module(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let this = PyModule::new(py, "fileio")?;

    this.add_class::<InputFile>()?;

    m.add_submodule(&this)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item("pyiceberg_core.fileio", this)
}
