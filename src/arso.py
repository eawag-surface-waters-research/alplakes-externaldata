import os
import json
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from functions import logger, parse_dict_string

def arso_meteodata(data_folder):
    """
    Download Meteodata from Arso
    https://meteo.arso.gov.si/met/en/app/webmet/#webmet==8Sdwx2bhR2cv0WZ0V2bvEGcw9ydlJWblR3LwVnaz9SYtVmYh9iclFGbt9SaulGdugXbsx3cs9mdl5WahxXYyNGapZXZ8tHZv1WYp5mOnMHbvZXZulWYnwCchJXYtVGdlJnOn0UQQdSf;
    """
    stations = [
        {"id": "2213", "parameters": ["12", "26", "21", "15", "23", "27", "18"]}
    ]
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Arso")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "arso/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    current_date = datetime.now()
    last_update = current_date - timedelta(weeks=2)
    url = "https://meteo.arso.gov.si/webmet/archive/data.xml?lang=en&vars={}&group=halfhourlyData0&type=halfhourly&id={}&d1={}&d2={}"

    for station in stations:
        log.info("Downloading data for station {}".format(station["id"]))
        u = url.format(",".join(station["parameters"]), station["id"], last_update.strftime("%Y-%m-%d"), current_date.strftime("%Y-%m-%d"))
        response = requests.get(u)
        if response.status_code == 200:
            try:
                raw_data = str(response.content)
                data = parse_dict_string(raw_data)
                keys = data["params"].keys()
                d = {key: [] for key in keys}
                d["time"] = []
                for key in data["points"]["_"+station["id"]].keys():
                    d["time"].append(datetime(1800,1,1) + timedelta(seconds=int(key.replace("_", "")) * 60))
                    for k in keys:
                        if k in data["points"]["_"+station["id"]][key]:
                            d[k].append(float(data["points"]["_"+station["id"]][key][k]))
                        else:
                            d[k].append(np.nan)
                df = pd.DataFrame(d)
                for key in keys:
                    df = df.rename(columns={key: data["params"][key]["pid"]})
                cols = ["time"] + sorted([col for col in df.columns if col != "time"])
                df = df[cols]
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
                failed.append(station["id"])
        else:
            failed.append(station["id"])

    if len(failed) > 0:
        raise ValueError("Failed to download and process: {}".format(", ".join(failed)))


