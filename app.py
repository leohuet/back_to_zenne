import threading
import time
from datetime import datetime, timedelta, timezone
import json
import os
from flask import Flask, render_template, request, redirect, url_for, jsonify
from pythonosc import udp_client, osc_bundle_builder, osc_message_builder
from base64 import b64encode
import requests
import pandas as pd
from dotenv import find_dotenv, load_dotenv
import math

dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

# app and config parameters
app = Flask(__name__)
CONFIG_FILE = "config.json"
base_config = {
    "install_duration": 120,
    "ip_address": "127.0.0.1",
    "ableton_port": 8001,
    "madmapper_port": 9001,
    "addresses": [
        {
            "name": "drogenbos_level",
            "osc_address": "/drogenbos/1",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": "realtime",
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "viangros_temp",
            "osc_address": "/viangros/1",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": 10,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "viangros_conduct",
            "osc_address": "/viangros/2",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": 30,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "viangros_ph",
            "osc_address": "/viangros/3",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": "none",
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "quaidaa_level",
            "osc_address": "/quaidaa/1",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": "realtime",
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "quaidaa_flowrate",
            "osc_address": "/quaidaa/2",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": "realtime",
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "veterinaires_oxygen",
            "osc_address": "/veterinaires/1",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": 7,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "buda_flowrate",
            "osc_address": "/buda/1",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": "realtime",
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "senneout_temp",
            "osc_address": "/senneout/1",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": 10,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "senneout_conduct",
            "osc_address": "/senneout/2",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": 30,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "senneout_ph",
            "osc_address": "/senneout/3",
            "min_ableton": 0.0,
            "max_ableton": 1.0,
            "min_mad": 0.0,
            "max_mad": 1.0,
            "from": "none",
            "to_ableton": False,
            "to_mad": False
        }
    ]
}
data_names = {
    "drogenbos": ["level"],
    "viangros": ["temp"],
    "viangros2": ["conduct", "acidity"],
    "quaidaa": ["d_level", "g_level", "d_flowrate", "g_flowrate"],
    "veterinaires": ["oxygen"],
    "buda": ["flowrate"],
    "senneOUT": ["temp"],
    "senneOUT2": ["conduct", "acidity"]

}
data_names1 = {
    "drogenbos": ["level"],
    "viangros": ["temp"],
    "viangros2": ["conduct", "acidity"],
    "quaidaa": ["d_level", "d_flowrate"],
    "veterinaires": ["oxygen"],
    "buda": ["flowrate"],
    "senneOUT": ["temp"],
    "senneOUT2": ["conduct", "acidity"]

}
local_config = {}
# only get new json data when the modification time changed
old_getmtime = 1000

# OSC parameters
abletonOSC_port = 11000
osc_thread = None
osc_running = False

# --- Authentification ---
user = os.getenv("FLOWBRU_USER")
password = os.getenv('FLOWBRU_PASS')
message = f"{user}:{password}"
message_bytes = message.encode('ascii')
base64_bytes = b64encode(message_bytes)
base64_message = base64_bytes.decode('ascii')
my_headers = {"Authorization": "Basic " + base64_message}

# rolling average variables
size_mean = 2
drogenbos_for_mean = []
viangros_for_mean = []
quaidaa_for_mean = []
pluie_for_mean = []
for i in range(size_mean):
    drogenbos_for_mean.append(0.5)
    viangros_for_mean.append(0.5)
    quaidaa_for_mean.append(0.5)
    pluie_for_mean.append(0.5)


def get_relative_dates():
    # retrieve all dates for API calls
    date_config = load_config()
    fmt = "%Y%m%d%H%M"
    fmt_day = "%Y%m%d0000"
    now = datetime.now(timezone.utc) - timedelta(minutes=30)
    times = []

    # retrieve time duration for each call from the config file
    for index, address in enumerate(date_config['addresses']):
        if address['from'] == "none" or address['from'] == "realtime":
            continue
        times.append((now - timedelta(days=address['from'])).strftime(fmt_day))
        print(times)

    install_min_dur = math.ceil(date_config["install_duration"] / 60)

    # relative to now dates
    minutes_date = (now - timedelta(minutes=install_min_dur+1)).strftime(fmt)
    hours_date = (now - timedelta(hours=install_min_dur)).strftime(fmt)
    days_date = (now - timedelta(days=install_min_dur)).strftime(fmt_day)
    weeks_date = (now - timedelta(weeks=1)).strftime(fmt_day)
    months_date = (now - timedelta(days=30)).strftime(fmt_day)

    date_end = now.strftime(fmt_day)
    current_datehmin = now.strftime(fmt)

    return {
        "minutes": minutes_date,
        "hours": hours_date,
        "days": days_date,
        "weeks": weeks_date,
        "months": months_date,
        "date_end": date_end,
        "current_date_h_min": current_datehmin,
        "viangros": times[0],
        "viangros2": times[1],
        "veterinaires": times[2],
        "senneOUT": times[3],
        "senneOUT2": times[4]
    }


def get_sites(dates_dict):
    return [
        {
            "name": "drogenbos",
            "uid": "4A5190B93E170A87",
            "histdata": "histdata0",
            "channel": '"level"',
            "from": dates_dict["hours"],
            "until": dates_dict["current_date_h_min"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "viangros",
            "uid": "4B8483DD3257BAD9",
            "histdata": "histdata0",
            "channel": '"temp"',
            "from": dates_dict["viangros"],
            "until": dates_dict["date_end"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "viangros2",
            "uid": "4B8483DD3257BAD9",
            "histdata": "histdata0",
            "channel": '"conduct", "ph"',
            "from": dates_dict["viangros2"],
            "until": dates_dict["date_end"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "quaidaa",
            "uid": "9DD946B760E34493",
            "histdata": "histdata0",
            "channel": '"ch1", "ch9", "ch0", "ch8"',
            "from": dates_dict["minutes"],
            "until": dates_dict["current_date_h_min"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "veterinaires",
            "uid": "4A26A91BCA0FE58C",
            "histdata": "histdata0",
            "channel": '"xch4"',
            "from": dates_dict["veterinaires"],
            "until": dates_dict["date_end"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "buda",
            "uid": "A35E8E5A539949A7",
            "histdata": "histdata5",
            "channel": '"ch0"',
            "from": dates_dict["hours"],
            "until": dates_dict["current_date_h_min"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "senneOUT",
            "uid": "4B845F9C7151AC54",
            "histdata": "histdata0",
            "channel": '"temp"',
            "from": dates_dict["senneOUT"],
            "until": dates_dict["date_end"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },
        {
            "name": "senneOUT2",
            "uid": "4B845F9C7151AC54",
            "histdata": "histdata0",
            "channel": '"conduct", "ph"',
            "from": dates_dict["senneOUT2"],
            "until": dates_dict["date_end"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },

    ]

def rolling_average(data_array, new_value):
    global size_mean
    data_array.append(new_value)
    if len(data_array) > size_mean:
        data_array.pop(0)
    return sum(data_array) / len(data_array)


def correct_raw_date(date_str):
    date_str = str(date_str).strip()
    date_str = ''.join(filter(str.isdigit, date_str))
    # cut if too long
    if len(date_str) > 12:
        date_str = date_str[:12]
    # complete with zeros if too short
    while len(date_str) < 12:
        date_str += '0'
    return date_str


def process_request(site, response, local=False):
    try:
        # if call was successful
        if not local and response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            # if df is not empty
            if len(df) > 0:
                print("saving to JSON")
                with open(f"{site['name']}.json", "w") as f:
                    json.dump(data, f, indent=4)
            else:
                with open(f"{site['name']}.json", "r") as f:
                    data = json.load(f)
        else:
            with open(f"{site['name']}.json", "r") as f:
                data = json.load(f)

        df = pd.DataFrame(data)
        # cleaning timestamp
        if 't' in df.columns:
            df['timestamp'] = pd.to_datetime(df['t'], unit='s')
            df = df.drop(columns=['t'])

        df.rename(columns={0: "date"}, inplace=True)
        # for each data column, process and normalize
        for j in range(1, len(df.columns)):
            df[j] = pd.to_numeric(df[j], errors='coerce')
            if len(df[j]) > 0:
                if max(df[j]) == min(df[j]):
                    df[j] = df[j] / max(df[j]) / 2
                else:
                    df[j] = (df[j] - min(df[j])) / (max(df[j]) - min(df[j]))
            df = df[df[j].notna()]
            df[j] = df[j].round(2)
            df.rename(columns={j: data_names[site['name']][j - 1]}, inplace=True)

        df['site'] = site['name']

        # date correction
        df['date'] = df['date'].apply(correct_raw_date)
        df = df[df['date'].notna()]
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M', errors='coerce')

        dataframes[site["name"]] = df
        print(f"{site['name'].capitalize()} - {len(df)} lignes")
        print(df.head(), '\n')
    except Exception as e:
        print(f"Erreur JSON ou DataFrame pour {site['name']}: {e}")


def retrieve_data():
    # --- Data retrieving for each site ---
    for site in sites:
        url = f'https://www.flowbru.eu/api/1/customers/{CID}/sites/{site["uid"]}/{site["histdata"]}?json={{"select":[{site["channel"]}],"from":"{site["from"]}","until":"{site["until"]}"}} '
        print(url)
        response = 0
        try:
            response = requests.get(url, headers=my_headers)
        except:
            print("Error occured while calling API, getting data from local JSON.")

        if response:
            process_request(site, response, local=False)
        else:
            print(f"Erreur HTTP pour {site['name']}")
            process_request(site, response, local=True)


def get_last_data():
    # get new data for realtime
    new_dates_dict = get_relative_dates()
    sites[3]["from"] = new_dates_dict["minutes"]
    sites[3]["until"] = new_dates_dict["current_date_h_min"]
    url = f'https://www.flowbru.eu/api/1/customers/{CID}/sites/{sites[3]["uid"]}/{sites[3]["histdata"]}?json={{"select":[{sites[3]["channel"]}],"from":"{sites[3]["from"]}","until":"{sites[3]["until"]}"}} '
    print(url)
    response = 0
    try:
        response = requests.get(url, headers=my_headers)
    except:
        print("Error occured while calling API, getting data from local JSON.")

    if response:
        process_request(sites[3], response, local=False)
    else:
        print(f"Erreur HTTP pour {sites[3]['name']}")
        process_request(sites[3], response, local=True)


# === GESTION DE LA CONFIG ===
def load_config():
    global base_config
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        save_config(base_config)
        return base_config


def save_config(config_dict):
    global mad_client, ableton_client
    with open(CONFIG_FILE, "w") as f:
        json.dump(config_dict, f, indent=4)
    IP = config_dict["ip_address"]
    MAD_PORT = int(config_dict["madmapper_port"])
    ABLETON_PORT = int(config_dict["ableton_port"])
    # Clients OSC
    mad_client = udp_client.SimpleUDPClient(IP, MAD_PORT)
    ableton_client = udp_client.SimpleUDPClient(IP, ABLETON_PORT)


# === THREAD OSC ===
def osc_sender():
    global mad_client, ableton_client, old_getmtime, local_config, osc_running
    print("OSC thread démarré.")
    d_sec_time_index = 0
    sec_time_index = 0
    min_time_index = 0
    restart = False
    frequency = 10

    # if config file changed, get the new config
    if os.path.getmtime(CONFIG_FILE) != old_getmtime:
        old_getmtime = os.path.getmtime(CONFIG_FILE)
        cfg = load_config()
        local_config = cfg

    # for each site, compute the interpolation
    # if interpol > 1, it means there is less data than the duration of the loop
    # if interpol < 1, it means there is too much data and some will be skipped
    for site in sites:
        site["df"] = dataframes.get(site["name"])
        if site["name"] == "quaidaa":
            site["interpol"] = math.floor(local_config["install_duration"] / (len(site["df"])-1))
        elif len(site["df"]) < local_config["install_duration"]:
            site["interpol"] = math.floor(local_config["install_duration"] / len(site["df"]))
        elif len(site["df"]) < (frequency * local_config["install_duration"]):
            site["interpol"] = 1 / math.floor(len(site["df"]) / local_config["install_duration"])
        else:
            site["interpol"] = 1 / math.floor(len(site["df"]) / local_config["install_duration"])
        print(f'{site["name"]}, {site["interpol"]}')

    ableton_control.send_message('/live/song/start_playing', None)
    start_time = int(time.strftime('%H')) * 3600 + int(time.strftime('%M')) * 60 + int(time.strftime('%S'))

    while osc_running:
        current_time = int(time.strftime('%H')) * 3600 + int(time.strftime('%M')) * 60 + int(time.strftime('%S'))
        if os.path.getmtime(CONFIG_FILE) != old_getmtime:
            old_getmtime = os.path.getmtime(CONFIG_FILE)
            cfg = load_config()
            local_config = cfg

        # restart at the end of the loop
        if restart:
            print("restart")
            restart = False
            d_sec_time_index = 0
            sec_time_index = 0
            min_time_index = 0
            get_last_data()
            time.sleep(5)

            ableton_control.send_message('/live/song/stop_playing', None)
            time.sleep(1)
            ableton_control.send_message('/live/song/stop_playing', None)
            addr_index = 0
            new_ableton_bundle = osc_bundle_builder.OscBundleBuilder(osc_bundle_builder.IMMEDIATELY)
            new_mad_bundle = osc_bundle_builder.OscBundleBuilder(osc_bundle_builder.IMMEDIATELY)
            # reset OSC sends by sending first data of the next loop
            for site in sites:
                for data in data_names1[site["name"]]:
                    addr = local_config["addresses"][addr_index]
                    osc_addr = addr["osc_address"]
                    restart_val = float(site["df"][data][0])
                    msg = osc_message_builder.OscMessageBuilder(address=osc_addr)
                    msg1 = osc_message_builder.OscMessageBuilder(address=osc_addr)
                    if addr["to_ableton"]:
                        vmin, vmax = float(addr["min_ableton"]), float(addr["max_ableton"])
                        restart_new_val = restart_val * (vmax - vmin) + vmin
                        msg.add_arg(restart_new_val)
                        new_ableton_bundle.add_content(msg.build())
                    elif addr["to_mad"]:
                        vmin, vmax = float(addr["min_mad"]), float(addr["max_mad"])
                        restart_new_val = restart_val * (vmax - vmin) + vmin
                        msg1.add_arg(restart_new_val)
                        new_mad_bundle.add_content(msg1.build())
                    # print(f"[THREAD] Envoi {osc_addr} = {new_val}")
                    addr_index += 1
            new_ableton_bundle = new_ableton_bundle.build()
            if new_ableton_bundle.size > 16:
                ableton_client.send(new_ableton_bundle)
            new_mad_bundle = new_mad_bundle.build()
            if new_mad_bundle.size > 16:
                mad_client.send(new_mad_bundle)

            for site in sites:
                site["df"] = dataframes.get(site["name"])
                if site["name"] == "quaidaa":
                    site["interpol"] = math.floor(local_config["install_duration"] / (len(site["df"]) - 1))
                elif len(site["df"]) < local_config["install_duration"]:
                    site["interpol"] = math.floor(local_config["install_duration"] / len(site["df"]))
                elif len(site["df"]) < (frequency * local_config["install_duration"]):
                    site["interpol"] = 1 / math.floor(len(site["df"]) / local_config["install_duration"])
                else:
                    site["interpol"] = 1 / math.floor(len(site["df"]) / (frequency * local_config["install_duration"]))
                print(f'{site["name"]}, {site["interpol"]}')

            time.sleep(2)
            ableton_control.send_message('/live/song/start_playing', None)
            start_time = int(time.strftime('%H')) * 3600 + int(time.strftime('%M')) * 60 + int(time.strftime('%S'))

        # compute index for each dataframe based on the interpol value
        for site in sites:
            if site["name"] == "quaidaa":
                site["index"] = min(min_time_index + 1, (len(site["df"]) - 1))
            elif site["interpol"] < 1:
                site["index"] = min(sec_time_index*math.pow(site["interpol"], -1) + int((d_sec_time_index / (frequency / math.pow(site["interpol"], -1)))) % math.pow(site["interpol"], -1), (len(site["df"]) - 1))
            elif site["interpol"] == 1 and len(site["df"]) > local_config["install_duration"]:
                site["index"] = d_sec_time_index
            elif site["interpol"] == 1 and len(site["df"]) <= local_config["install_duration"]:
                site["index"] = sec_time_index + 1
            elif site["interpol"] > 1:
                site["index"] = min(math.floor(sec_time_index / site["interpol"]) + 1, len(site["df"]) - 1)

        addr_index = 0
        ableton_bundle = osc_bundle_builder.OscBundleBuilder(osc_bundle_builder.IMMEDIATELY)
        mad_bundle = osc_bundle_builder.OscBundleBuilder(osc_bundle_builder.IMMEDIATELY)
        # send each data
        # if interpol > 1, compute linear interpolation
        for site in sites:
            for data in data_names1[site["name"]]:
                addr = local_config["addresses"][addr_index]
                osc_addr = addr["osc_address"]
                if site["interpol"] < 1:
                    val = site["df"][data][site["index"]]
                elif site["interpol"] == 1 and len(site["df"]) > local_config["install_duration"]:
                    val = site["df"][data][site["index"]]
                elif site["interpol"] >= 1:
                    val = site["df"][data][site["index"]-1] * (((site["interpol"]*frequency-1) - (d_sec_time_index % (site["interpol"]*frequency))) /
                            (site["interpol"]*frequency-1)) + site["df"][data][site["index"]] * ((d_sec_time_index % (site["interpol"]*frequency)) / (site["interpol"]*frequency-1))
                else:
                    val = 0.5
                msg = osc_message_builder.OscMessageBuilder(address=osc_addr)
                msg1 = osc_message_builder.OscMessageBuilder(address=osc_addr)
                if addr["to_ableton"]:
                    vmin, vmax = float(addr["min_ableton"]), float(addr["max_ableton"])
                    new_val = val * (vmax - vmin) + vmin
                    msg.add_arg(new_val)
                    ableton_bundle.add_content(msg.build())
                if addr["to_mad"]:
                    vmin, vmax = float(addr["min_mad"]), float(addr["max_mad"])
                    new_val = val * (vmax - vmin) + vmin
                    msg1.add_arg(new_val)
                    mad_bundle.add_content(msg1.build())
                # print(f"[THREAD] Envoi {osc_addr} = {new_val}")
                addr_index += 1
        ableton_bundle = ableton_bundle.build()
        if ableton_bundle.size > 16:
            ableton_client.send(ableton_bundle)
        mad_bundle = mad_bundle.build()
        if mad_bundle.size > 16:
            mad_client.send(mad_bundle)

        time.sleep(1 / frequency)
        # if end of loop, restart
        # else, compute index for 1/frequency seconds, 1 second, 1 minute
        if current_time >= (start_time + local_config["install_duration"] - 1):
            restart = True
        else:
            d_sec_time_index = (d_sec_time_index + 1) % (local_config["install_duration"] * frequency)
            sec_time_index = math.floor(d_sec_time_index / frequency)
            min_time_index = math.floor(sec_time_index / 60)


# === INTERFACE FLASK ===
@app.route("/", methods=["GET", "POST"])
def index():
    conf = load_config()
    # if POST, retrieve all data and change the config json
    # raise error if issues with data
    if request.method == "POST":
        error = False
        response = {}
        conf["install_duration"] = int(request.form["install_duration"])
        conf["ableton_port"] = int(request.form["ableton_port"])
        conf["madmapper_port"] = int(request.form["madmapper_port"])
        if conf["ableton_port"] == conf["madmapper_port"]:
            error = True
            response = {
                "error": "Bad request",
                "message": "OSC port already in use.",
                "status": 400
            }
        new_addresses = []
        n = int(request.form["count"])
        # rebuild array with all addresses and parameters
        for k in range(n):
            if float(request.form[f"min_ableton_{k}"]) >= float(request.form[f"max_ableton_{k}"]):
                error = True
                response = {
                    "error": "Bad request",
                    "message": f'{request.form[f"name_{k}"]} min value is superior to max value.',
                    "status": 400
                }
            if float(request.form[f"min_mad_{k}"]) >= float(request.form[f"max_mad_{k}"]):
                error = True
                response = {
                    "error": "Bad request",
                    "message": f'{request.form[f"name_{k}"]} min value is superior to max value.',
                    "status": 400
                }
            new_addresses.append({
                "name": request.form[f"name_{k}"],
                "osc_address": request.form[f"osc_address_{k}"],
                "min_ableton": float(request.form[f"min_ableton_{k}"]),
                "max_ableton": float(request.form[f"max_ableton_{k}"]),
                "min_mad": float(request.form[f"min_mad_{k}"]),
                "max_mad": float(request.form[f"max_mad_{k}"]),
                "from": int(request.form[f"from_{k}"]),
                "to_ableton": request.form.get(f"to_ableton_{k}") == "1",
                "to_mad": request.form.get(f"to_mad_{k}") == "1"
            })
            if new_addresses[k]["from"] == -1:
                new_addresses[k]["from"] = "realtime"
            elif new_addresses[k]["from"] == -2:
                new_addresses[k]["from"] = "none"
        conf["addresses"] = new_addresses
        if not error:
            save_config(conf)
            return redirect(url_for("index"))
        else:
            return jsonify(response, 400)
    return render_template("index.html", config=conf)


@app.route("/reset_config", methods=["POST"])
def reset_config():
    print("Reset config to default")
    save_config(base_config)
    return "", 204


@app.route("/start_osc", methods=["POST"])
def start_osc():
    global osc_thread, osc_running, dates_dict, sites, dataframes, config
    # Dates et formats de base
    dates_dict = get_relative_dates()
    # --- Sites & channels ---
    sites = get_sites(dates_dict)
    # --- DataFrames init ---
    dataframes = {}

    retrieve_data()

    config = load_config()

    time.sleep(1)

    if not osc_running:
        osc_running = True
        osc_thread = threading.Thread(target=osc_sender, daemon=True)
        osc_thread.start()
    return redirect(url_for("index"))


@app.route("/stop_osc", methods=["POST"])
def stop_osc():
    global osc_running
    osc_running = False
    ableton_control.send_message('/live/song/stop_playing', None)
    return redirect(url_for("index"))


@app.route("/test_osc/<int:osc_index>", methods=["POST"])
def test_osc(osc_index):
    global ableton_client, mad_client
    cfg = load_config()
    try:
        addr = cfg["addresses"][osc_index]
    except IndexError:
        return jsonify({"error": "index invalide"}), 400

    ableton_min, ableton_max = float(addr["min_ableton"]), float(addr["max_ableton"])
    mad_min, mad_max = float(addr["min_mad"]), float(addr["max_mad"])
    osc_addr = addr["osc_address"]

    ableton_val = (ableton_min + ableton_max) / 2
    mad_val = (mad_min + mad_max) / 2
    if addr["to_ableton"]:
        ableton_client.send_message(osc_addr, ableton_val)
        print(f"[TEST] to ableton, {osc_addr} = {ableton_val}")
    if addr["to_mad"]:
        mad_client.send_message(osc_addr, mad_val)
        print(f"[TEST] to mad, {osc_addr} = {mad_val}")
    if not addr["to_ableton"] and not addr["to_mad"]:
        print("Select an OSC output to send a test!")
        return jsonify({"error": "Select an OSC output to send a test!", "address": osc_addr})
    else:
        return jsonify({"sent": True, "address": osc_addr})


if __name__ == "__main__":
    CID = '9D9E9DE1F0E437A6'
    # Dates et formats de base
    dates_dict = get_relative_dates()
    # --- Sites & channels ---
    sites = get_sites(dates_dict)
    # --- DataFrames init ---
    dataframes = {}

    retrieve_data()

    config = load_config()

    # OSC clients
    ip = config["ip_address"]
    mad_port = int(config["madmapper_port"])
    ableton_port = int(config["ableton_port"])
    mad_client = udp_client.SimpleUDPClient(ip, mad_port)
    ableton_client = udp_client.SimpleUDPClient(ip, ableton_port)
    ableton_control = udp_client.SimpleUDPClient(ip, abletonOSC_port)
    ableton_control.send_message('/live/song/stop_playing', None)

    # run Flask
    app.run(host="0.0.0.0", port=8000, debug=False)
