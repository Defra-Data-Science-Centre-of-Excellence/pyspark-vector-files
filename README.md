# PySpark Vector Files

Read vector files into a Spark DataFrame with geometry encoded as WKB.

## Install

### Within a Databricks notebook

```sh
%pip install git+https://github.com/Defra-Data-Science-Centre-of-Excellence/pyspark_vector_files
```

### From the command line

```sh
python -m pip install git+https://github.com/Defra-Data-Science-Centre-of-Excellence/pyspark_vector_files
```

## Local development

To ensure compatibility with [Databricks Runtime 9.1 LTS](https://docs.databricks.com/release-notes/runtime/9.1.html), this package was developed on a Linux machine running the `Ubuntu 20.04 LTS` operating system using `Python 3.8.8`, `GDAL 3.4.0`, and `spark 3.1.2`.

### Install `Python 3.8.8` using [pyenv](https://github.com/pyenv/pyenv)

See the `pyenv-installer`'s [Installation / Update / Uninstallation](https://github.com/pyenv/pyenv-installer#installation--update--uninstallation) instructions.

Install Python 3.8.8 globally:

```sh
pyenv install 3.8.8
```

Then install it locally in the repository you're using:

```sh
pyenv local 3.8.8
```

### Install `GDAL 3.4.0`

Add the [UbuntuGIS unstable Private Package Archive (PPA)](https://launchpad.net/~ubuntugis/+archive/ubuntu/ubuntugis-unstable)
and update your package list:

```sh
sudo add-apt-repository ppa:ubuntugis/ubuntugis-unstable \
    && sudo apt-get update
```

Install `gdal 3.4.0`, I found I also had to install python3-gdal (even though
I'm going to use poetry to install it in a virtual environment later) to
avoid version conflicts:

```sh
sudo apt-get install -y gdal-bin=3.4.0+dfsg-1~focal0 \
    libgdal-dev=3.4.0+dfsg-1~focal0 \
    python3-gdal=3.4.0+dfsg-1~focal0
```

Verify the installation:

```sh
ogrinfo --version
# GDAL 3.4.0, released 2021/11/04
```

### Install `poetry 1.1.13`

See poetry's [osx / linux / bashonwindows install instructions](https://python-poetry.org/docs/#osx--linux--bashonwindows-install-instructions)

### Clone this repository

```sh
git clone https://github.com/Defra-Data-Science-Centre-of-Excellence/pyspark_vector_files.git
```

### Install dependencies using `poetry`

```sh
poetry install
```
