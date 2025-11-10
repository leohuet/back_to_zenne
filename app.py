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
    "ip_address": "127.0.0.1",
    "ableton_port": 8001,
    "madmapper_port": 9001,
    "addresses": [
        {
            "name": "drogenbos_level",
            "osc_address": "/drogenbos/1",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "viangros_temp",
            "osc_address": "/viangros/1",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "viangros_conduct",
            "osc_address": "/viangros/2",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "viangros_ph",
            "osc_address": "/viangros/3",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "quaidaa_level",
            "osc_address": "/quaidaa/1",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "quaidaa_flowrate",
            "osc_address": "/quaidaa/2",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "veterinaires_oxygen",
            "osc_address": "/veterinaires/1",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "buda_flowrate",
            "osc_address": "/buda/1",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "senneout_temp",
            "osc_address": "/senneout/1",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "senneout_conduct",
            "osc_address": "/senneout/2",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        },
        {
            "name": "senneout_ph",
            "osc_address": "/senneout/3",
            "min_value": 0.0,
            "max_value": 1.0,
            "to_ableton": False,
            "to_mad": False
        }
    ]
}
local_config = {}
old_getmtime = 1000

# OSC parameters
abletonOSC_port = 11000
osc_thread = None
osc_running = False

install_duration = 2

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
    global install_duration
    fmt = "%Y%m%d%H%M"
    fmt_day = "%Y%m%d0000"
    now = datetime.now(timezone.utc) - timedelta(minutes=30)

    # relative to now dates
    minutes_ago = now - timedelta(minutes=install_duration+1)
    hours_ago = now - timedelta(hours=install_duration)
    days_ago = now - timedelta(days=install_duration)
    weeks_ago = now - timedelta(weeks=1)
    months_ago = now - timedelta(days=30)

    # date format
    minutes_date = minutes_ago.strftime(fmt)
    hours_date = hours_ago.strftime(fmt)
    days_date = days_ago.strftime(fmt_day)
    weeks_date = weeks_ago.strftime(fmt_day)
    months_date = months_ago.strftime(fmt_day)
    date_end = now.strftime(fmt_day)
    current_datehmin = now.strftime(fmt)

    return {
        "minutes": minutes_date,
        "hours": hours_date,
        "days": days_date,
        "weeks": weeks_date,
        "months": months_date,
        "date_end": date_end,
        "current_date_h_min": current_datehmin
    }


def moyenne_glissante(data_array, new_value):
    global size_mean
    data_array.append(new_value)
    if len(data_array) > size_mean:
        data_array.pop(0)
    return sum(data_array) / len(data_array)


def corriger_date_brute(date_str):
    # Fonction pour corriger les dates
    date_str = str(date_str).strip()
    date_str = ''.join(filter(str.isdigit, date_str))
    # Tronquer si trop long
    if len(date_str) > 12:
        date_str = date_str[:12]
    # Compléter avec des zéros à droite si trop court
    while len(date_str) < 12:
        date_str += '0'
    return date_str


def process_request(site, response, local=False):
    try:
        if not local and response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
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
        for j in range(1, len(df.columns)):
            df[j] = pd.to_numeric(df[j], errors='coerce')
            if len(df[j]) > 0:
                if max(df[j]) == min(df[j]):
                    df[j] = df[j] / max(df[j])
                else:
                    df[j] = (df[j] - min(df[j])) / (max(df[j]) - min(df[j]))
            df = df[df[j].notna()]
            df[j] = df[j].round(2)
            df.rename(columns={j: data_names[site['name']][j - 1]}, inplace=True)

        df['site'] = site['name']

        # data correction
        df['date'] = df['date'].apply(corriger_date_brute)
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

    for site in sites:
        site["df"] = dataframes.get(site["name"])
        if site["name"] == "quaidaa":
            site["interpol"] = math.floor((60 * install_duration) / (len(site["df"])-1))
        elif len(site["df"]) < (60 * install_duration):
            site["interpol"] = math.floor((60 * install_duration) / len(site["df"]))
        elif len(site["df"]) < (60 * frequency * install_duration):
            site["interpol"] = 1 / math.floor(len(site["df"]) / (60 * install_duration))
        else:
            site["interpol"] = 1 / math.floor(len(site["df"]) / (60 * frequency * install_duration))
        print(f'{site["name"]}, {site["interpol"]}')

    ableton_control.send_message('/live/song/start_playing', None)
    start_time = int(time.strftime('%H')) * 3600 + int(time.strftime('%M')) * 60 + int(time.strftime('%S'))
    while osc_running:
        current_time = int(time.strftime('%H')) * 3600 + int(time.strftime('%M')) * 60 + int(time.strftime('%S'))
        if os.path.getmtime(CONFIG_FILE) != old_getmtime:
            old_getmtime = os.path.getmtime(CONFIG_FILE)
            cfg = load_config()
            local_config = cfg

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
            for site in sites:
                for data in data_names1[site["name"]]:
                    addr = local_config["addresses"][addr_index]
                    vmin, vmax = float(addr["min_value"]), float(addr["max_value"])
                    osc_addr = addr["osc_address"]
                    val = site["df"][data][0]
                    new_val = val * (vmax - vmin) + vmin
                    msg = osc_message_builder.OscMessageBuilder(address=osc_addr)
                    msg.add_arg(new_val)
                    if addr["to_ableton"]:
                        new_ableton_bundle.add_content(msg.build())
                    elif addr["to_mad"]:
                        new_mad_bundle.add_content(msg.build())
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
                    site["interpol"] = math.floor((60 * install_duration) / (len(site["df"]) - 1))
                elif len(site["df"]) < (60 * install_duration):
                    site["interpol"] = math.floor((60 * install_duration) / len(site["df"]))
                elif len(site["df"]) < (60 * frequency * install_duration):
                    site["interpol"] = 1 / math.floor(len(site["df"]) / (60 * install_duration))
                else:
                    site["interpol"] = 1 / math.floor(len(site["df"]) / (60 * frequency * install_duration))
                print(f'{site["name"]}, {site["interpol"]}')

            time.sleep(2)
            ableton_control.send_message('/live/song/start_playing', None)
            start_time = int(time.strftime('%H')) * 3600 + int(time.strftime('%M')) * 60 + int(time.strftime('%S'))

        for site in sites:
            if site["name"] == "quaidaa":
                site["index"] = min(min_time_index + 1, (len(site["df"]) - 1))
            elif site["interpol"] < 1:
                site["index"] = min(sec_time_index*math.pow(site["interpol"], -1) + int((d_sec_time_index / (frequency / math.pow(site["interpol"], -1)))) % math.pow(site["interpol"], -1), (len(site["df"]) - 1))
            elif site["interpol"] == 1 and len(site["df"]) > (60*install_duration):
                site["index"] = d_sec_time_index
            elif site["interpol"] == 1 and len(site["df"]) <= (60*install_duration):
                site["index"] = sec_time_index + 1
            elif site["interpol"] > 1:
                site["index"] = min(math.floor(sec_time_index / site["interpol"]) + 1, len(site["df"]) - 1)

        addr_index = 0
        ableton_bundle = osc_bundle_builder.OscBundleBuilder(osc_bundle_builder.IMMEDIATELY)
        mad_bundle = osc_bundle_builder.OscBundleBuilder(osc_bundle_builder.IMMEDIATELY)
        for site in sites:
            for data in data_names1[site["name"]]:
                addr = local_config["addresses"][addr_index]
                vmin, vmax = float(addr["min_value"]), float(addr["max_value"])
                osc_addr = addr["osc_address"]
                if site["interpol"] < 1:
                    val = site["df"][data][site["index"]]
                elif site["interpol"] == 1 and len(site["df"]) > (60*install_duration):
                    val = site["df"][data][site["index"]]
                elif site["interpol"] >= 1:
                    val = site["df"][data][site["index"]-1] * (((site["interpol"]*frequency-1) - (d_sec_time_index % (site["interpol"]*frequency))) /
                            (site["interpol"]*frequency-1)) + site["df"][data][site["index"]] * ((d_sec_time_index % (site["interpol"]*frequency)) / (site["interpol"]*frequency-1))
                else:
                    val = 0.5
                new_val = val*(vmax-vmin)+vmin
                msg = osc_message_builder.OscMessageBuilder(address=osc_addr)
                if addr["to_ableton"]:
                    ableton_bundle.add_content(msg.build())
                elif addr["to_mad"]:
                    mad_bundle.add_content(msg.build())
                # print(f"[THREAD] Envoi {osc_addr} = {new_val}")
                addr_index += 1

        ableton_bundle = ableton_bundle.build()
        if ableton_bundle.size > 16:
            ableton_client.send(ableton_bundle)
        mad_bundle = mad_bundle.build()
        if mad_bundle.size > 16:
            mad_client.send(mad_bundle)

        time.sleep(1 / frequency)
        if current_time >= (start_time + (60 * install_duration) - 1):
            restart = True
        else:
            d_sec_time_index = (d_sec_time_index + 1) % (60 * install_duration * frequency)
            sec_time_index = math.floor(d_sec_time_index / frequency)
            min_time_index = math.floor(sec_time_index / 60)


# === INTERFACE FLASK ===
@app.route("/", methods=["GET", "POST"])
def index():
    conf = load_config()
    if request.method == "POST":
        conf["ip_address"] = request.form["ip_address"]
        conf["ableton_port"] = int(request.form["ableton_port"])
        conf["madmapper_port"] = int(request.form["madmapper_port"])
        new_addresses = []
        n = int(request.form["count"])
        for k in range(n):
            new_addresses.append({
                "name": request.form[f"name_{k}"],
                "osc_address": request.form[f"osc_address_{k}"],
                "min_value": float(request.form[f"min_value_{k}"]),
                "max_value": float(request.form[f"max_value_{k}"]),
                "to_ableton": request.form.get(f"to_ableton_{k}") == "1",
                "to_mad": request.form.get(f"to_mad_{k}") == "1"
            })
        conf["addresses"] = new_addresses
        save_config(conf)
        return redirect(url_for("index"))
    return render_template("index.html", config=conf)


@app.route("/reset_config", methods=["POST"])
def reset_config():
    print("Reset config to default")
    save_config(base_config)
    return "", 204


@app.route("/start_osc", methods=["POST"])
def start_osc():
    global osc_thread, osc_running
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

    vmin, vmax = float(addr["min_value"]), float(addr["max_value"])
    osc_addr = addr["osc_address"]

    val = (vmax + vmin) / 2
    if addr["to_ableton"]:
        ableton_client.send_message(osc_addr, val)
        print(f"[TEST] to ableton, {osc_addr} = {val}")
    elif addr["to_mad"]:
        mad_client.send_message(osc_addr, val)
        print(f"[TEST] to mad, {osc_addr} = {val}")
    else:
        print("Select an OSC output to send a test!")

    return jsonify({"sent": True, "address": osc_addr, "value": val})


if __name__ == "__main__":
    # Open Ableton Live
    # os.system('open "/Users/poire/Desktop/CODE/testQuitAbleton/test Project/test2.als"')
    # time.sleep(20)
    # Informations d'authentification
    CID = '9D9E9DE1F0E437A6'
    # Dates et formats de base
    dates_dict = get_relative_dates()

    # --- Sites & canaux ---
    sites = [
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
            "from": dates_dict["days"],
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
            "from": dates_dict["months"],
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
            "from": dates_dict["weeks"],
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
            "from": dates_dict["days"],
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
            "from": dates_dict["months"],
            "until": dates_dict["date_end"],
            "interpol": 0.0,
            "df": None,
            "index": 0
        },

    ]

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

    # --- Stockage des DataFrames ---
    dataframes = {}

    retrieve_data()

    config = load_config()
    ip = config["ip_address"]
    mad_port = int(config["madmapper_port"])
    ableton_port = int(config["ableton_port"])

    # Clients OSC
    mad_client = udp_client.SimpleUDPClient(ip, mad_port)
    ableton_client = udp_client.SimpleUDPClient(ip, ableton_port)
    ableton_control = udp_client.SimpleUDPClient(ip, abletonOSC_port)
    ableton_control.send_message('/live/song/stop_playing', None)

    # Lance Flask
    app.run(host="0.0.0.0", port=8000, debug=False)
