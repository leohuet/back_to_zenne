import threading
import time
from datetime import datetime, timedelta, timezone
import json
import os
from flask import Flask, render_template, request, redirect, url_for
from pythonosc import udp_client
from base64 import b64encode
import requests
import pandas as pd
from dotenv import find_dotenv, load_dotenv


dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

app = Flask(__name__)
CONFIG_FILE = "config.json"
old_getmtime = 1000

osc_thread = None
osc_running = False

ip = '127.0.0.1'
td_port = 9001
ableton_port = 8001

local_config = {
    "ip_address": "127.0.0.1",
    "td_port": 9001,
    "ableton_port": 8001,
    "osc_address": "/value",
    "min_value": 0.0,
    "max_value": 100.0
}

install_duration = 2

# --- Authentification ---
user = os.getenv("FLOWBRU_USER")
password = os.getenv('FLOWBRU_PASS')
message = f"{user}:{password}"
message_bytes = message.encode('ascii')
base64_bytes = b64encode(message_bytes)
base64_message = base64_bytes.decode('ascii')
my_headers = {"Authorization": "Basic " + base64_message}


size_mean = 2
drogenbos_for_mean = []
quaidaa_for_mean = []
pluie_for_mean = []
for i in range(size_mean):
    drogenbos_for_mean.append(0.5)
    quaidaa_for_mean.append(0.5)
    pluie_for_mean.append(0.5)


def get_relative_dates():
    global install_duration
    fmt = "%Y%m%d%H%M"
    fmt_day = "%Y%m%d0000"
    now = datetime.now(timezone.utc) - timedelta(minutes=30)

    # Périodes relatives
    minutes_ago = now - timedelta(minutes=install_duration)
    hours_ago = now - timedelta(hours=install_duration)
    days_ago = now - timedelta(days=install_duration)
    weeks_ago = now - timedelta(weeks=install_duration)
    months_ago = now - timedelta(days=30 * install_duration)  # approximation : 1 mois = 30j

    # Formats des sorties
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


# Fonction pour corriger les dates
def corriger_date_brute(date_str):
    date_str = str(date_str).strip()
    date_str = ''.join(filter(str.isdigit, date_str))  # Enlever tout sauf les chiffres

    # Tronquer si trop long
    if len(date_str) > 12:
        date_str = date_str[:12]

    # Compléter avec des zéros à droite si trop court
    while len(date_str) < 12:
        date_str += '0'

    return date_str


def retrieve_data():
    # --- Récupération des données pour chaque site ---
    for site in sites:
        url = f'https://www.flowbru.eu/api/1/customers/{CID}/sites/{site["uid"]}/{site["histdata"]}?json={{"select":[{site["channel"]}],"from":"{site["from"]}","until":"{site["until"]}"}} '
        print(url)
        response = requests.get(url, headers=my_headers)

        if response.status_code == 200:
            try:
                data = response.json()
                df = pd.DataFrame(data)  # adapte ici si la structure est différente
                # Nettoyage : convertir le timestamp s'il existe
                if 't' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['t'], unit='s')
                    df = df.drop(columns=['t'])  # On garde que "timestamp", plus lisible

                df.rename(columns={0: "date"}, inplace=True)
                for j in range(1, len(df.columns)):
                    df[j] = pd.to_numeric(df[j], errors='coerce')
                    # df[j] = (df[j] - min(df[j])) / (max(df[j]) - min(df[j]))
                    df = df[df[j].notna()]
                    df[j] = df[j].round(2)
                    df.rename(columns={j: data_names[site['name']][j-1]}, inplace=True)

                # Ajouter le nom du site en colonne (optionnel mais utile pour concat)
                df['site'] = site['name']

                # Correction des dates
                df['date'] = df['date'].apply(corriger_date_brute)
                df = df[df['date'].notna()]
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M', errors='coerce')
                dataframes[site["name"]] = df
                print(f"{site['name'].capitalize()} - {len(df)} lignes")
                print(df.head(), '\n')
            except Exception as e:
                print(f"Erreur JSON ou DataFrame pour {site['name']}: {e}")
                # print(response.text)
        else:
            print(f"Erreur HTTP pour {site['name']}: {response.status_code}")
            print(response.text)


def get_last_data():
    new_dates_dict = get_relative_dates()

    sites[1]["from"] = new_dates_dict["minutes"]
    sites[1]["from"] = new_dates_dict["current_date_h_min"]

    # --- Récupération des données pour chaque site ---
    for site in sites:
        if site['name'] != "quaidaa":
            continue
        url = f'https://www.flowbru.eu/api/1/customers/{CID}/sites/{site["uid"]}/{site["histdata"]}?json={{"select":[{site["channel"]}],"from":"{site["from"]}","until":"{site["until"]}"}} '
        print(url)
        response = requests.get(url, headers=my_headers)

        if response.status_code == 200:
            try:
                data = response.json()
                df = pd.DataFrame(data)  # adapte ici si la structure est différente
                # Nettoyage : convertir le timestamp s'il existe
                if 't' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['t'], unit='s')
                    df = df.drop(columns=['t'])  # On garde que "timestamp", plus lisible

                df.rename(columns={0: "date"}, inplace=True)
                for j in range(1, len(df.columns)):
                    df[j] = pd.to_numeric(df[j], errors='coerce')
                    df = df[df[j].notna()]
                    df[j] = df[j].round(2)
                    df.rename(columns={j: data_names[site['name']][j - 1]}, inplace=True)

                # Ajouter le nom du site en colonne (optionnel mais utile pour concat)
                df['site'] = site['name']

                # Correction des dates
                df['date'] = df['date'].apply(corriger_date_brute)
                df = df[df['date'].notna()]
                df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M', errors='coerce')
                dataframes[site["name"]] = df
                print(f"{site['name'].capitalize()} - {len(df)} lignes")
                print(df.head(), '\n')
            except Exception as e:
                print(f"Erreur JSON ou DataFrame pour {site['name']}: {e}")
                # print(response.text)
        else:
            print(f"Erreur HTTP pour {site['name']}: {response.status_code}")
            print(response.text)


# === GESTION DE LA CONFIG ===
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        base_config = {
            "ip_address": "127.0.0.1",
            "td_port": 9001,
            "ableton_port": 8001,
            "osc_address": "/value",
            "min_value": 0.0,
            "max_value": 100.0
        }
        save_config(base_config)
        return base_config


def save_config(config_dict):
    global td_client, ableton_client
    with open(CONFIG_FILE, "w") as f:
        json.dump(config_dict, f, indent=4)
    IP = config_dict["ip_address"]
    TD_PORT = int(config_dict["td_port"])
    ABLETON_PORT = int(config_dict["ableton_port"])
    # Clients OSC
    td_client = udp_client.SimpleUDPClient(IP, TD_PORT)
    ableton_client = udp_client.SimpleUDPClient(IP, ABLETON_PORT)


# === THREAD OSC ===
def osc_sender():
    global td_client, ableton_client, old_getmtime, local_config, df, osc_running
    print("OSC thread démarré.")
    time_index = 0
    restart = False

    df_drogenbos = dataframes.get("drogenbos")
    df_quaidaa = dataframes.get("quaidaa")
    df_buda = dataframes.get("buda")

    len_drogenbos = len(df_drogenbos)
    len_quaidaa = len(df_quaidaa)
    len_buda = len(df_buda)

    drogenbos_interpol = (60 * install_duration) / len_drogenbos
    quaidaa_interpol = (60 * install_duration) / len_quaidaa
    buda_interpol = (60 * install_duration) / len_buda
    print(drogenbos_interpol)
    print(quaidaa_interpol)
    print(buda_interpol)

    while osc_running:
        if os.path.getmtime(CONFIG_FILE) != old_getmtime:
            old_getmtime = os.path.getmtime(CONFIG_FILE)
            config = load_config()  # recharge les paramètres à chaque boucle

            osc_address1 = config["osc_address1"]
            osc_address2 = config["osc_address2"]
            osc_address3 = config["osc_address3"]
            min_val = float(config["min_value"])
            max_val = float(config["max_value"])

            local_config = config
        else:
            osc_address1 = local_config["osc_address1"]
            osc_address2 = local_config["osc_address2"]
            osc_address3 = local_config["osc_address3"]
            min_val = float(local_config["min_value"])
            max_val = float(local_config["max_value"])

        if time_index == 0 and restart:
            print("restart")
            get_last_data()
            time.sleep(5)

        new_index = int(time_index/5) - 1
        if new_index < 0:
            new_index = 0



        drogenbos_level_data = df_drogenbos['level'][time_index]
        drogenbos_mapped_data = (drogenbos_level_data * (max_val - min_val) + min_val) / 100

        quaidaa_level_data = df_quaidaa['d_level'][time_index] * (((quaidaa_interpol-1) - (time_index % quaidaa_interpol))
                            / (quaidaa_interpol-1)) + df_quaidaa['d_level'][time_index + 1] * ((time_index % quaidaa_interpol) / (quaidaa_interpol-1))
        quaidaa_mapped_data = (quaidaa_level_data * (max_val - min_val) + min_val) / 100

        buda_flowrate_data = df_buda['flowrate'][new_index] * ((4 - (time_index % 5)) / 4) + df_buda['flowrate'][new_index + 1] * ((time_index % 5) / 4)
        buda_mapped_data = buda_flowrate_data

        ableton_client.send_message(osc_address1, drogenbos_mapped_data)
        ableton_client.send_message(osc_address2, buda_mapped_data)
        ableton_client.send_message(osc_address3, quaidaa_mapped_data)

        # print(f"Envoi OSC {osc_address1}: {buda_mapped_data}")
        print(f"Envoi OSC {osc_address2}: {quaidaa_mapped_data}")

        time.sleep(1)
        if time_index == (60 * 1) - 1:
            restart = True
        time_index = (time_index + 1) % (60 * 1)


# === INTERFACE FLASK ===
@app.route("/", methods=["GET", "POST"])
def index():
    config = load_config()
    if request.method == "POST":
        config["ip_address"] = request.form["ip_address"]
        config["td_port"] = int(request.form["td_port"])
        config["ableton_port"] = int(request.form["ableton_port"])
        config["osc_address"] = request.form["osc_address"]
        config["min_value"] = float(request.form["min_value"])
        config["max_value"] = float(request.form["max_value"])
        save_config(config)
        return redirect(url_for("index"))
    return render_template("index.html", config=config)


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
    return redirect(url_for("index"))


if __name__ == "__main__":
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
            "until": dates_dict["current_date_h_min"]
        },
        {
            "name": "quaidaa",
            "uid": "9DD946B760E34493",
            "histdata": "histdata0",
            "channel": '"ch1", "ch9", "ch0", "ch8"',
            "from": dates_dict["minutes"],
            "until": dates_dict["current_date_h_min"]
        },
        {
            "name": "veterinaires",
            "uid": "4A26A91BCA0FE58C",
            "histdata": "histdata0",
            "channel": '"xch4"',
            "from": dates_dict["weeks"],
            "until": dates_dict["date_end"]
        },
        {
            "name": "buda",
            "uid": "A35E8E5A539949A7",
            "histdata": "histdata5",
            "channel": '"ch0"',
            "from": dates_dict["hours"],
            "until": dates_dict["current_date_h_min"]
        },
        {
            "name": "senneOUT",
            "uid": "4B845F9C7151AC54",
            "histdata": "histdata0",
            "channel": '"temp", "conduct", "ph"',
            "from": dates_dict["months"],
            "until": dates_dict["date_end"]
        },

    ]

    data_names = {
        "drogenbos": ["level"],
        "quaidaa": ["d_level", "g_level", "d_flowrate", "g_flowrate"],
        "veterinaires": ["oxygen"],
        "buda": ["flowrate"],
        "senneOUT": ["temperature", "acidity", "conductivity"]

    }

    # --- Stockage des DataFrames ---
    dataframes = {}

    retrieve_data()

    config = load_config()
    ip = config["ip_address"]
    td_port = int(config["td_port"])
    ableton_port = int(config["ableton_port"])
    # Clients OSC
    td_client = udp_client.SimpleUDPClient(ip, td_port)
    ableton_client = udp_client.SimpleUDPClient(ip, ableton_port)

    # Lance Flask
    app.run(host="0.0.0.0", port=8000, debug=False)
