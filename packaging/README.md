# starlink-grpc-tools Core Module

This project packages up the `starlink_grpc` module from the [starlink-grpc-tools](https://github.com/sparky8512/starlink-grpc-tools) project and exports it as an installable package for use by other projects. It is not needed to install this project in order to use the scripts in starlink-grpc-tools, as those have their own copy of `starlink_grpc.py`.

`starlink_grpc.py` is the only part of the scripts in starlink-grpc-tools that is designed to have a stable enough interface to be directly callable from other projects without having to go through a clunky command line interface. It provides the low(er) level core functionality available via the [gRPC](https://grpc.io/) service implemented on the Starlink user terminal.

# Installation

The most recently published version of this project can be installed by itself using pip:
```shell script
pip install starlink-grpc-core
```
However, it is really meant to be installed as a dependency by other projects.

# Usage

The installation process places the `starlink_grpc.py` module in the top-level of your Python lib directory or virtual environment, so it can be used simply by doing:
```python
import starlink_grpc
```
and then calling whatever functions you need. For details, see the doc strings in `starlink_grpc.py`.

# Examples

For example usage, see calling scripts in the [starlink-grpc-tools](https://github.com/sparky8512/starlink-grpc-tools) project, most of which are hopelessly convoluted, but some of which show simple usage of the `starlink_grpc` functions.

