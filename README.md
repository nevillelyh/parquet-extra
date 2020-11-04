parquet-extra
=============

[![Build Status](https://img.shields.io/github/workflow/status/nevillelyh/parquet-extra/CI)](https://github.com/nevillelyh/parquet-extra/actions?query=workflow%3ACI)
[![codecov.io](https://codecov.io/github/nevillelyh/parquet-extra/coverage.svg?branch=master)](https://codecov.io/github/nevillelyh/parquet-extra?branch=master)
[![GitHub license](https://img.shields.io/github/license/nevillelyh/parquet-extra.svg)](./LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/me.lyh/parquet-avro_2.13.svg)](https://maven-badges.herokuapp.com/maven-central/me.lyh/parquet-avro_2.13)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-brightgreen.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

A collection of [Apache Parquet](http://parquet.apache.org/) add-on modules.

- `parquet-avro` - Scala macros for generating column projections and filter predicates from lambda functions.
- `parquet-tensorflow` - [TensorFlow](https://www.tensorflow.org/) `Example` read/write support.
- `parquet-types` - Scala case class read/write support powered by [Magnolia](https://github.com/propensive/magnolia).

## License

Copyright 2019 Neville Li.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
