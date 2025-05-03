
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)](http://unlicense.org/)

# tpcc-generator

A tool to create input for the popular [TPC-C](http://www.tpc.org/tpcc/) database benchmark.

This project is meant to help people wanting to run TPC-C but not use the official implementation that provides C code in a pdf file and makes use of [embedded SQL](https://en.wikipedia.org/wiki/Embedded_SQL).
Instead, we strive for a clean, easy to extend implementation in C++.
We generate portable [csv](https://en.wikipedia.org/wiki/Comma-separated_values) files which can be loaded into any database system.

## Features

- Simple _Makefile_ project with no external dependencies.
- Stable random: you get the same input data no matter where you run it.
- __Missing:__ Date fields are dependent on current time and can only be written as time_since_epoch.

## Run

With the following command you can create TPC-C input with _5_ warehouses. All files are stored in the my_tpcc_input directory. Note: The directory has to exist before starting the generator. But, no worries, if does not exists the generator will complain.

```bash
make
mkdir my_tpcc_input
./tpcc-generator 5 my_tpcc_input
```

Sample output:
```
I am loading TPCC data for 5 warehouses, hold on ..

Generating warehouse .. ok !
Generating districts .. ok !
Generating customers and their history .. ok !
Generating items .. ok !
Generating stocks .. ok !
Generating orders .. ok !

.. data generation completed successfully :)
```

Licence
-------
You are free to choose any of the above licences when using the source code.
However, I encourage you in a non binding way to follow the [blessing from the SQLite folks](https://github.com/sqlite/sqlite/blob/master/LICENSE.md):

```
May you do good and not evil.
May you find forgiveness for yourself and forgive others.
May you share freely, never taking more than you give.
```

Authors
-------
Alexander van Renen
