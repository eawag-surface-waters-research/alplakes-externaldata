import os
import re
import time
import json
import requests
import numpy as np
import pandas as pd
import zipfile
import tempfile
from io import BytesIO
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from functions import logger, parse_dict_string, split_date_range

def dwd_meteodata(data_folder):
    """
    Download Meteodata from DWD
    https://opendata.dwd.de/

    Add new stations
    1. Find closest station https://alplakes-eawag.s3.eu-central-1.amazonaws.com/static/dwd/dwd_stations.json
    2. Add station to stations list
    3. Set historical to True
    4. Upload data to API
    5. Edit FastAPI list of stations
    """

    historical = False

    parameter_dict = {
        "air_temperature": {
            "url": "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/recent/10minutenwerte_TU_{:05}_akt.zip",
            "parameters": ["TT_10", "RF_10"]
        },
        "wind": {
            "url": "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/10minutenwerte_wind_{:05}_akt.zip",
            "parameters": ["DD_10", "FF_10"]
        },
        "precipitation": {
            "url": "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/recent/10minutenwerte_nieder_{:05}_akt.zip",
            "parameters": ["RWS_10"]
        },
        "global_radiation": {
            "url": "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/solar/recent/10minutenwerte_SOLAR_{:05}_akt.zip",
            "parameters": ["GS_10"]
        }
    }
    stations = [
        {"id": "2559", "parameters": ["wind","precipitation","global_radiation","air_temperature"]},
        {"id": "3857", "parameters": ["precipitation","air_temperature"]},
        {"id": "15214", "parameters": ["wind"]},
        {"id": "1550", "parameters": ["wind","precipitation","global_radiation","air_temperature"]},
        {"id": "2319", "parameters": ["precipitation","air_temperature"]},
        {"id": "2708", "parameters": ["precipitation","air_temperature"]},
        {"id": "2290", "parameters": ["wind","precipitation","global_radiation","air_temperature"]},
        {"id": "3307", "parameters": ["precipitation","air_temperature"]},
        {"id": "217", "parameters": ["precipitation","air_temperature"]},
        {"id": "5538", "parameters": ["wind","precipitation","global_radiation","air_temperature"]},
        {"id": "15520", "parameters": ["wind"]},
        {"id": "856", "parameters": ["wind","precipitation","global_radiation","air_temperature"]},
        {"id": "2573", "parameters": ["wind"]},
        {"id": "19856", "parameters": ["wind", "precipitation", "air_temperature"]},
    ]
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from DWD")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "dwd/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    for station in stations:
        log.info("Downloading data for station {}".format(station["id"]))

        data = {parameter: False for parameter in station["parameters"]}

        if historical:
            log.info("Accessing complete historical record")

            for parameter in station["parameters"]:
                url = parameter_dict[parameter]["url"].split("recent/")[0] + "historical"
                response = requests.get(url)
                if response.status_code == 200:
                    for line in response.text.splitlines():
                        if "10minutenwerte_" in line and "_{:05}_".format(int(station["id"])) in line:
                            zip_url = url + "/" + line.split('<a href="')[1].split('">')[0]
                            response = requests.get(zip_url)
                            if response.status_code == 200:
                                with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
                                    with tempfile.TemporaryDirectory() as temp_dir:
                                        zip_ref.extractall(temp_dir)
                                        text_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]
                                        if text_files:
                                            text_file_path = f"{temp_dir}/{text_files[0]}"
                                            df = pd.read_csv(text_file_path, delimiter=";")
                                            df["time"] = pd.to_datetime(df['MESS_DATUM'], format='%Y%m%d%H%M', utc=True)
                                            df = df[["time"] + parameter_dict[parameter]["parameters"]]
                                            df.replace(-999, np.nan, inplace=True)
                                            if isinstance(data[parameter], pd.DataFrame):
                                                merged_df = pd.concat([data[parameter], df])
                                                data[parameter] = merged_df.sort_values(by='time')
                                            else:
                                                data[parameter] = df
                                        else:
                                            raise ValueError("Text file not found")
                            else:
                                raise ValueError("Status code not valid")

        try:
            for parameter in station["parameters"]:
                zip_url = parameter_dict[parameter]["url"].format(int(station["id"]))
                response = requests.get(zip_url)
                if response.status_code == 200:
                    with zipfile.ZipFile(BytesIO(response.content), 'r') as zip_ref:
                        with tempfile.TemporaryDirectory() as temp_dir:
                            zip_ref.extractall(temp_dir)
                            text_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]
                            if text_files:
                                text_file_path = f"{temp_dir}/{text_files[0]}"
                                df = pd.read_csv(text_file_path, delimiter=";")
                                df["time"] = pd.to_datetime(df['MESS_DATUM'], format='%Y%m%d%H%M', utc=True)
                                df = df[["time"] + parameter_dict[parameter]["parameters"]]
                                df.replace(-999, np.nan, inplace=True)
                                if isinstance(data[parameter], pd.DataFrame):
                                    merged_df = pd.concat([data[parameter], df])
                                    data[parameter] = merged_df.sort_values(by='time')
                                else:
                                    data[parameter] = df
                            else:
                                raise ValueError("Text file not found")
                else:
                    raise ValueError("Status code not valid")

            df = data[station["parameters"][0]]
            for key in station["parameters"][1:]:
                df = pd.merge(df, data[key], on='time', how='outer')

            for year in range(df['time'].min().year, df['time'].max().year + 1):
                station_year_file = os.path.join(parent, station["id"], "{}.csv".format(year))
                station_year_data = df[df['time'].dt.year == year]
                if not os.path.exists(station_year_file):
                    log.info("Saving file new file {}.".format(station_year_file), indent=1)
                    os.makedirs(os.path.dirname(station_year_file), exist_ok=True)
                    station_year_data.to_csv(station_year_file, index=False)
                else:
                    df_existing = pd.read_csv(station_year_file)
                    df_existing['time'] = pd.to_datetime(df_existing['time'])
                    combined = pd.concat([df_existing, station_year_data])
                    combined = combined.drop_duplicates(subset=['time'], keep='last')
                    combined = combined.sort_values(by='time')
                    combined.to_csv(station_year_file, index=False)
        except:
            log.info("FAILED", indent=1)
            if station["id"] not in failed:
                failed.append(station["id"])

    if len(failed) > 0:
        raise ValueError("Failed to download at least one time period from: {}".format(", ".join(failed)))
