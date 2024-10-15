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
    failed = []

    log = logger("meteodata", path=os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, "logs"))
    log.initialise("Download Meteodata from Arso")

    log.info("Ensure data folder exists.")
    parent = os.path.join(data_folder, "arso/meteodata")
    if not os.path.exists(parent):
        os.makedirs(parent)

    stations = [
        {"id": "2213"}
    ]

    url = "https://meteo.arso.gov.si/webmet/archive/data.xml?lang=en&vars=12,26,21,15,23,27,18&group=halfhourlyData0&type=halfhourly&id={}&d1={}&d2={}"
    current_date = datetime.now()
    one_week_ago = current_date - timedelta(weeks=1)

    for station in stations:
        u = url.format(station["id"], one_week_ago.strftime("%Y-%m-%d"), current_date.strftime("%Y-%m-%d"))
        response = requests.get(u)
        if response.status_code == 200:
            raw_data = str(response.content)
            data = parse_dict_string(raw_data)
            keys = data["params"].keys()
            print(data["params"])
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
            print(df)
        else:
            failed.append(station["id"])

    if len(failed) > 0:
        raise ValueError("Failed to download and process: {}".format(", ".join(failed)))


