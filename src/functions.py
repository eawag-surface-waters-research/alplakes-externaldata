import os
import re
import sys
import glob
import math
import time
import shutil
import xarray
import hashlib
import netCDF4
import zipfile
import logging
import requests
import traceback
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

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

            nc_files = sorted(glob.glob(os.path.join(temp_folder, '*.nc')))
            if not nc_files:
                raise ValueError(f"No NetCDF files found in {temp_folder}")

            datasets = [xarray.open_dataset(f) for f in nc_files]
            ds = xarray.concat(datasets, dim='time', data_vars='all').load()
            for d in datasets:
                d.close()
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


def split_date_range(start_date, end_date, period, unit='days'):
    """
    Splits a date range into chunks of a given period.

    :param start_date: Start of the range (datetime object)
    :param end_date: End of the range (datetime object)
    :param period: Length of each chunk
    :param unit: Unit of the period ('days', 'months', 'years')
    :return: List of tuples (chunk_start, chunk_end)
    """
    chunks = []
    current_start = start_date

    while current_start < end_date:
        # Determine the next period's start date based on the unit
        if unit == 'days':
            next_period_start = current_start + timedelta(days=period)
        elif unit == 'months':
            next_period_start = current_start + relativedelta(months=period)
        elif unit == 'years':
            next_period_start = current_start + relativedelta(years=period)
        else:
            raise ValueError("Invalid unit. Use 'days', 'months', or 'years'.")

        # Ensure the end of the chunk does not exceed the overall end_date
        chunk_end = min(next_period_start, end_date)
        chunks.append((current_start, chunk_end))

        # Update the current start for the next chunk
        current_start = chunk_end

    return chunks


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

def merge_dfs(left, right):
    return pd.merge(left, right, on='time', how='outer')

def create_stac_session(workers):
    """Retries temporary API errors. Downloads have their own retries, as a retry needs a new link."""
    retry = Retry(total=5, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=None)
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=workers))
    return session


def stac_time(value):
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def redact_url(text):
    """Removes the query string of URLs, as pre-signed links must not reach the logs."""
    return re.sub(r"(https?://[^\s?'\"]+)\?[^\s'\")]*", r"\1?<redacted>", str(text))


def horizon_to_hours(horizon):
    match = re.fullmatch(r"P(\d+)DT(\d+)H(\d+)M(\d+)S", horizon)
    if match is None or int(match[3]) != 0 or int(match[4]) != 0:
        raise ValueError("Unexpected forecast horizon: {}".format(horizon))
    return int(match[1]) * 24 + int(match[2])


def stac_search(session, stac_url, collection, filters):
    """Returns all items matching a STAC search. The next link only holds a cursor, to be merged into the request."""
    body = {"collections": [collection], "limit": 100, **filters}
    items = []
    while True:
        response = session.post(stac_url + "/search", json=body, timeout=120)
        response.raise_for_status()
        page = response.json()
        items.extend(page["features"])
        following = [link for link in page.get("links", []) if link["rel"] == "next"]
        if len(following) == 0:
            return items
        body = {**body, **following[0]["body"]}


def stac_forecast_filters(ref_time, variable, perturbed):
    return {"forecast:reference_datetime": stac_time(ref_time), "forecast:variable": variable,
            "forecast:perturbed": perturbed}


def list_stac_forecast(session, stac_url, collection, ref_time, variables, leads):
    """Returns {(variable, perturbed, lead): item}, raising unless every file of the run is published."""
    items = {}
    for variable in variables:
        for perturbed in (False, True):
            filters = stac_forecast_filters(ref_time, variable, perturbed)
            for item in stac_search(session, stac_url, collection, filters):
                key = (variable, perturbed, horizon_to_hours(item["properties"]["forecast:horizon"]))
                # Republished files appear as extra items with a later creation time
                if key not in items or item["properties"]["created"] > items[key]["properties"]["created"]:
                    items[key] = item
    expected = [(v, p, lead) for v in variables for p in (False, True) for lead in leads]
    missing = [key for key in expected if key not in items]
    if len(missing) > 0:
        raise ValueError("Run {} is incomplete, {} of {} files are missing, e.g. {}".format(
            stac_time(ref_time), len(missing), len(expected), missing[0]))
    return {key: items[key] for key in expected}


def stac_asset_href(item):
    assets = list(item["assets"].values())
    if len(assets) != 1:
        raise ValueError("Expected one asset in item {}, found {}".format(item["id"], len(assets)))
    return assets[0]["href"]


def stream_to_file(session, url, path):
    """Downloads to a temporary file, renaming it once its checksum has been verified where one is provided."""
    part = path + ".part"
    digest = hashlib.sha256()
    with session.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        expected = response.headers.get("x-amz-meta-sha256")
        with open(part, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                digest.update(chunk)
    if expected is not None and expected.lower() != digest.hexdigest():
        raise ValueError("SHA-256 mismatch for {}".format(os.path.basename(path)))
    os.replace(part, path)


def download_with_retries(session, get_url, path, attempts=3, wait=30):
    """Each attempt asks get_url for a new link. Errors are redacted, as they can quote the pre-signed link."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    message = None
    for attempt in range(attempts):
        if attempt > 0:
            time.sleep(wait)
        try:
            stream_to_file(session, get_url(attempt), path)
            return
        except (requests.RequestException, OSError, ValueError, KeyError, StopIteration) as e:
            message = redact_url(e)
    raise ValueError("Failed to download {}: {}".format(os.path.basename(path), message)) from None


def download_stac_asset(session, stac_url, collection, name, path):
    """Downloads a file that belongs to the collection rather than to a forecast."""

    def get_url(attempt):
        response = session.get("{}/collections/{}/assets".format(stac_url, collection), timeout=120)
        response.raise_for_status()
        return next(a["href"] for a in response.json()["assets"] if a["id"] == name)

    download_with_retries(session, get_url, path)


def grib_forecast_path(folder, variable, perturbed, lead):
    return os.path.join(folder, variable, "{}_{}.grib2".format(lead, "perturbed" if perturbed else "control"))


def download_stac_forecast_file(session, stac_url, collection, ref_time, key, item, folder):
    variable, perturbed, lead = key

    def get_url(attempt):
        if attempt == 0:
            return stac_asset_href(item)
        filters = stac_forecast_filters(ref_time, variable, perturbed)
        filters["forecast:horizon"] = item["properties"]["forecast:horizon"]
        found = stac_search(session, stac_url, collection, filters)
        return stac_asset_href(max(found, key=lambda i: i["properties"]["created"]))

    download_with_retries(session, get_url, grib_forecast_path(folder, variable, perturbed, lead))


def download_stac_forecast(session, stac_url, collection, ref_time, items, folder, workers, log):
    """Stops at the first failed file, as the run cannot be processed without it."""
    with ThreadPoolExecutor(max_workers=workers) as executor:
        tasks = [executor.submit(download_stac_forecast_file, session, stac_url, collection, ref_time, key, item, folder)
                 for key, item in items.items()]
        try:
            for i, task in enumerate(tasks, start=1):
                task.result()
                if i % 200 == 0:
                    log.info("{} of {} files downloaded".format(i, len(tasks)), indent=1)
        except Exception:
            executor.shutdown(wait=False, cancel_futures=True)
            raise


def read_grib_messages(path):
    """Yields the keys used here and the values of each message, with missing data as NaN."""
    import eccodes  # Imported here so that the other sources do not depend on it
    with open(path, "rb") as f:
        while True:
            handle = eccodes.codes_grib_new_from_file(f)
            if handle is None:
                return
            try:
                eccodes.codes_set(handle, "stepUnits", "h")
                values = eccodes.codes_get_values(handle).astype("f8")
                if eccodes.codes_get(handle, "bitmapPresent"):
                    values[values == eccodes.codes_get(handle, "missingValue")] = np.nan
                try:
                    statistic = eccodes.codes_get(handle, "typeOfStatisticalProcessing")
                except eccodes.KeyValueNotFoundError:
                    statistic = None
                yield {"date": eccodes.codes_get(handle, "dataDate"),
                       "time": eccodes.codes_get(handle, "dataTime"),
                       "member": eccodes.codes_get(handle, "perturbationNumber"),
                       "start": eccodes.codes_get(handle, "startStep"),
                       "end": eccodes.codes_get(handle, "endStep"),
                       "statistic": statistic,
                       "grid": eccodes.codes_get_string(handle, "uuidOfHGrid"),
                       "parameter": (eccodes.codes_get(handle, "discipline"),
                                     eccodes.codes_get(handle, "parameterCategory"),
                                     eccodes.codes_get(handle, "parameterNumber")),
                       "values": values}
            finally:
                eccodes.codes_release(handle)


def read_icon_grid(path):
    """Returns (grid uuid, lat, lon) in degrees. Found by parameter, as their short names depend on the eccodes version."""
    fields = {m["parameter"]: m for m in read_grib_messages(path)}
    if (0, 191, 1) not in fields or (0, 191, 2) not in fields:
        raise ValueError("Could not find CLAT and CLON in {}".format(path))
    clat, clon = fields[(0, 191, 1)], fields[(0, 191, 2)]
    if clat["grid"] != clon["grid"]:
        raise ValueError("CLAT and CLON are on different grids")
    lat, lon = clat["values"], clon["values"]
    if np.nanmax(np.abs(lat)) <= np.pi:
        lat, lon = np.degrees(lat), np.degrees(lon)
    if np.isnan(lat).any() or np.isnan(lon).any() or lat.min() < 40 or lat.max() > 53 or lon.min() < -3 or lon.max() > 20:
        raise ValueError("CLAT and CLON are outside the expected domain")
    return clat["grid"], lat, lon


def read_icon_members(folder, variable, lead, ref_time, grid, size, statistic, members):
    """Returns (members, cells) for a lead time, ordered by member as the order in the files is random."""
    found = {}
    name = "{} at +{} h".format(variable, lead)
    for perturbed in (False, True):
        for m in read_grib_messages(grib_forecast_path(folder, variable, perturbed, lead)):
            if (m["date"], m["time"]) != (int(ref_time.strftime("%Y%m%d")), ref_time.hour * 100):
                raise ValueError("{}: wrong reference time {} {}".format(name, m["date"], m["time"]))
            if m["end"] != lead:
                raise ValueError("{}: expected to end at step {}, found {}".format(name, lead, m["end"]))
            if m["grid"] != grid or len(m["values"]) != size:
                raise ValueError("{}: not on the grid of the constants file".format(name))
            if m["statistic"] != statistic or (statistic is not None and m["start"] != 0):
                raise ValueError("{}: expected statistic {} from step 0, found statistic {} from step {}".format(
                    name, statistic, m["statistic"], m["start"]))
            if m["member"] in found:
                raise ValueError("{}: member {} appears twice".format(name, m["member"]))
            found[m["member"]] = m["values"]
    if sorted(found) != members:
        raise ValueError("{}: expected members {}, found {}".format(name, members, sorted(found)))
    return np.stack([found[member] for member in members])


def relative_humidity(t_2m, td_2m):
    """Returns relative humidity (%), using the constants that best reproduce RELHUM_2M in the existing products."""

    def saturation(t):
        return np.exp(17.2693882 * (t - 273.16) / (t - 35.86))

    return np.minimum(100 * saturation(td_2m) / saturation(t_2m), 100)


def hourly_mean(average, previous_average, lead):
    """Returns the mean over the hour ending at lead, given averages since the forecast start."""
    return np.maximum(lead * average - (lead - 1) * previous_average, 0)


def hourly_total(accumulation, previous_accumulation):
    """Returns the total over the hour ending at lead, given accumulations since the forecast start."""
    return np.maximum(accumulation - previous_accumulation, 0)


def create_icon_members_netcdf(path, ref_time, lat, lon, leads, members, outputs, source, history):
    dataset = netCDF4.Dataset(path, "w", format="NETCDF4")
    dataset.Conventions = "CF-1.6"
    dataset.institution = "MeteoSwiss (NMC Switzerland)"
    dataset.source = source
    dataset.history = history
    dataset.createDimension("time", len(leads))
    dataset.createDimension("epsd_1", len(members))
    dataset.createDimension("cell", len(lat))

    variable = dataset.createVariable("time", "i8", ("time",))
    variable.standard_name = "time"
    variable.long_name = "time"
    variable.units = "hours since {}".format(ref_time.strftime("%Y-%m-%d %H:%M:%S"))
    variable.calendar = "proleptic_gregorian"
    variable[:] = leads

    variable = dataset.createVariable("epsd_1", "i4", ("epsd_1",))
    variable.long_name = "Perturbation number of EPS system"
    variable.ensemble_size = np.int32(len(members))
    variable[:] = members

    for name, long_name, units, values in (("lon_1", "longitude", "degrees_east", lon),
                                           ("lat_1", "latitude", "degrees_north", lat)):
        variable = dataset.createVariable(name, "f4", ("cell",), zlib=True)
        variable.standard_name = long_name
        variable.long_name = long_name
        variable.units = units
        variable[:] = values

    for name, attributes in outputs.items():
        variable = dataset.createVariable(name, "f4", ("time", "epsd_1", "cell"), zlib=True,
                                          fill_value=np.finfo("f4").min, chunksizes=(1, len(members), len(lat)))
        variable.setncatts(attributes)
        variable.coordinates = "lon_1 lat_1"
    return dataset


def process_icon_members(folder, ref_time, output, grid, lat, lon, leads, members, statistics, outputs, source, history, log):
    """Writes the product one lead time at a time, renaming it only once every message has passed the checks."""
    fill_value = np.finfo("f4").min
    part = output + ".part"
    dataset = create_icon_members_netcdf(part, ref_time, lat, lon, leads, members, outputs, source, history)
    try:
        previous_radiation, previous_precipitation = None, None
        for index, lead in enumerate(leads):
            read = {v: read_icon_members(folder, v, lead, ref_time, grid, len(lat), statistics.get(v), members)
                    for v in ["T_2M", "TD_2M", "U_10M", "V_10M", "ASWDIR_S", "ASWDIFD_S", "CLCT", "TOT_PREC"]}
            radiation = read["ASWDIR_S"] + read["ASWDIFD_S"]
            precipitation = read["TOT_PREC"]
            missing = np.full(radiation.shape, np.nan)
            fields = {"T_2M": read["T_2M"], "U": read["U_10M"], "V": read["V_10M"], "CLCT": read["CLCT"],
                      "RELHUM_2M": relative_humidity(read["T_2M"], read["TD_2M"]),
                      # As in the existing products, these describe the hour ending at the lead, so have no value at +0 h
                      "GLOB": missing if lead == 0 else hourly_mean(radiation, previous_radiation, lead),
                      "TOT_PREC": missing if lead == 0 else hourly_total(precipitation, previous_precipitation)}
            for name, values in fields.items():
                dataset[name][index] = np.where(np.isnan(values), fill_value, values).astype("f4")
            previous_radiation, previous_precipitation = radiation, precipitation
            if lead % 24 == 0:
                log.info("Processed up to +{} h".format(lead), indent=1)
    except Exception:
        dataset.close()
        os.remove(part)
        raise
    dataset.close()
    os.replace(part, output)


def delete_old_runs(folder, pattern, now, days, log):
    """Deletes files whose name matches pattern and whose date, its first group as %Y_%m_%d, is older than days."""
    for name in sorted(os.listdir(folder)):
        match = re.match(pattern, name)
        if match is None:
            continue
        ref_time = datetime.strptime(match[1], "%Y_%m_%d").replace(tzinfo=timezone.utc)
        if now - ref_time > timedelta(days=days):
            path = os.path.join(folder, name)
            log.info("Deleting {}".format(name))
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
