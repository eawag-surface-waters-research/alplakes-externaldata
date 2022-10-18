# -*- coding: utf-8 -*-
import sys
import argparse
from meteoswiss import cosmo
from bafu import hydrodata


def main(params):
    setups = ["meteoswiss_cosmo", "bafu_hydrodata"]
    if params["source"] == "meteoswiss_cosmo":
        cosmo(params["filesystem"], params["password"])
    elif params["source"] == "bafu_hydrodata":
        hydrodata(params["filesystem"], params["key"])
    else:
        raise Exception("Currently only the following simulations are supported: {}".format(setups))


if __name__ == "__main__":
    if sys.version_info[0:2] != (3, 9):
        raise Exception('Requires python 3.9')
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', '-s', help="Data source [meteoswiss_cosmo, bafu_hydrodata]", type=str)
    parser.add_argument('--filesystem', '-f', help="Path to local storage filesystem", type=str,)
    parser.add_argument('--password', '-p', help="FTP password", type=str, default=False)
    parser.add_argument('--key', '-k', help="Path to ssh key file", type=str, default=False)
    args = parser.parse_args()
    main(vars(args))
