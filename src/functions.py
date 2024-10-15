import os
import re
import sys
import math
import shutil
import xarray
import zipfile
import logging
import traceback
from datetime import datetime, timedelta


def list_nested_dir(root):
    f = []
    for path, subdirs, files in os.walk(root):
        for name in files:
            f.append(os.path.join(path, name))
    return f


def progressbar(x, y):
    ''' progressbar for the pysftp
    '''
    bar_len = 60
    filled_len = math.ceil(bar_len * x / float(y))
    percents = math.ceil(100.0 * x / float(y))
    bar = '=' * filled_len + '-' * (bar_len - filled_len)
    filesize = f'{math.ceil(y/1024):,} KB' if y > 1024 else f'{y} byte'
    sys.stdout.write(f'[{bar}] {percents}% {filesize}\r')
    sys.stdout.flush()


def unzip_combine(path):
    if ".zip" in path:
        temp_folder = path.replace(".zip", "")
        if os.path.exists(temp_folder):
            shutil.rmtree(temp_folder)
        os.makedirs(temp_folder)
        try:
            with zipfile.ZipFile(path, 'r') as zip_ref:
                zip_ref.extractall(temp_folder)
            ds = xarray.open_mfdataset(temp_folder+'/*.nc', combine='by_coords')
            ds.to_netcdf(path.replace(".zip", ".nc"))
            ds.close()
            shutil.rmtree(temp_folder)
            os.unlink(path)
        except:
            shutil.rmtree(temp_folder)
            if os.path.exists(path.replace(".zip", ".nc")):
                os.unlink(path.replace(".zip", ".nc"))
            raise ValueError("Failed to unzip and combine {}".format(path))
    else:
        raise ValueError("Path is not a zip file: {}".format(path))


class logger(object):
    def __init__(self, name, path="logs"):
        self.name = name + datetime.now().strftime("_%Y%m%d_%H%M%S") + ".txt"
        self.path = os.path.join(path, self.name)
        self.stage = 1
        if not os.path.exists(path):
            os.makedirs(path)

    def info(self, string, indent=0):
        logging.info(string)
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + string
        print(out)
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def initialise(self, string):
        out = "****** " + string + " ******"
        print('\033[1m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def warning(self, string, indent=0):
        logging.warning(string)
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + "WARNING: " + string
        print('\033[93m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def error(self, string, error, indent=0):
        out = datetime.now().strftime("%H:%M:%S.%f") + (" " * 3 * (indent + 1)) + "ERROR: " + string
        print('\033[91m' + out + '\033[0m')
        print(error)
        with open(self.path, "a") as file:
            file.write(out + "\n")
            file.write("\n")
            traceback.print_exc(file=file)

    def end(self, string):
        out = "****** " + string + " ******"
        print('\033[92m' + out + '\033[0m')
        with open(self.path, "a") as file:
            file.write(out + "\n")

    def subprocess(self, process, error=""):
        failed = False
        while True:
            output = process.stdout.readline()
            out = output.strip()
            print(out)
            if error != "" and error in out:
                failed = True
            with open(self.path, "a") as file:
                file.write(out + "\n")
            return_code = process.poll()
            if return_code is not None:
                for output in process.stdout.readlines():
                    out = output.strip()
                    print(out)
                    with open(self.path, "a") as file:
                        file.write(out + "\n")
                break
        return failed

    def newline(self):
        print("")
        with open(self.path, "a") as file:
            file.write("\n")


def split_string(s):
    list = []
    slice_start = 0
    bracket_count = 0
    braces_count = 0
    for i in range(len(s)):
        if s[i] == "{":
            braces_count = braces_count + 1
        elif s[i] == "}":
            braces_count = braces_count - 1
        if s[i] == "[":
            bracket_count = bracket_count + 1
        elif s[i] == "]":
            bracket_count = bracket_count - 1
        elif s[i] == ",":
            if bracket_count == 0 and braces_count == 0:
                list.append(s[slice_start:i].strip())
                slice_start = i+1
    list.append(s[slice_start:len(s)].strip())
    return list


def parse_dict_string(data):
    dict = {}
    data = data[data.find('{'):data.rfind('}') + 1]
    for pair in split_string(data.strip()[1:-1].strip()):
        parts = pair.split(":", 1)
        if len(parts) == 2:
            key = parts[0]
            value = parts[1].strip()
            if value[0] == "{":
                dict[key] = parse_dict_string(value)
            elif value[0] == "[":
                dict[key] = parse_list_string(value)
            else:
                dict[key] = value.replace('"', '')
    return dict

def parse_list_string(data):
    list = []
    for value in data.strip()[1:-1].strip().split(","):
        value = value.strip()
        if value[0] == "{":
            list.append(parse_dict_string(value))
        elif value[0] == "[":
            list.append(parse_list_string(value))
        else:
            list.append(value.replace('"', ''))
    return list
