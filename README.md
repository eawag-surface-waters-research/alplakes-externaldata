# Alplakes External Data

[![License: MIT][mit-by-shield]][mit-by] ![Python][python-by-shield]

This is a repository for Eawag SURF external data access, initially developed for the ALPLAKES project.

## Getting started

### 1. Clone repository
```console
git clone https://github.com/eawag-surface-waters-research/alplakes-externaldata.git
```

### 2. Install virtual environment
Replace example filesystem path with correct path.
```console
conda create --name externaldata python=3.9.14
conda activate externaldata
conda install --file requirements.txt
```

### 3. Run commands

For a full list of options run
```console
python src/main.py -h
```

#### Download BAFU Hydrodaten
```console
python src/main.py -s bafu_hydrodata -f {{ filesystem path }} -k {{ ssh_key path }}
```
#### Download MeteoSwiss COSMO
```console
python src/main.py -s meteoswiss_cosmo -f {{ filesystem path }} -p {{ ftp password }}
```




[mit-by]: https://opensource.org/licenses/MIT
[mit-by-shield]: https://img.shields.io/badge/License-MIT-g.svg
[python-by-shield]: https://img.shields.io/badge/Python-3.9-g

