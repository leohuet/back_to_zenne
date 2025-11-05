import threading
import time
from datetime import datetime, timedelta
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

# Informations d'authentification
CID = '9D9E9DE1F0E437A6'
presentday = datetime.now()
format_presentday = presentday.strftime('%Y%m%d')
yesterday = (presentday - timedelta(1)).strftime('%Y%m%d')
date = f'{yesterday}0000'
hours = 24 - install_duration
hours_date = f'{yesterday}{hours}00'
days_date = (presentday - timedelta(install_duration)).strftime('%Y%m%d')
days_date = f'{days_date}0000'
week_date = (presentday - timedelta(7)).strftime('%Y%m%d')
week_date = f'{week_date}0000'
months = (int(format_presentday[4:6]) - install_duration) % 12
months_date = f'{format_presentday[0:4]}0{months}{format_presentday[6:]}'
date_end = f'{format_presentday}0000'

# --- Sites & canaux ---
sites = [
    {
        "name": "drogenbos",
        "uid": "4A5190B93E170A87",
        "histdata": "histdata0",
        "channel": '"level"',
        "from": hours_date,
        "until": date_end
    },
    {
        "name": "quaidaa",
        "uid": "9DD946B760E34493",
        "histdata": "histdata0",
        "channel": '"ch1"',
        "from": hours_date,
        "until": date_end
    },
    {
        "name": "veterinaires",
        "uid": "4A26A91BCA0FE58C",
        "histdata": "histdata0",
        "channel": '"xch4"',
        "from": week_date,
        "until": date_end
    },
    {
        "name": "buda",
        "uid": "A35E8E5A539949A7",
        "histdata": "histdata5",
        "channel": '"ch0"',
        "from": hours_date,
        "until": date_end
    },
    {
        "name": "senneOUT",
        "uid": "4B845F9C7151AC54",
        "histdata": "histdata0",
        "channel": '"temp", "conduct", "ph"',
        "from": months_date,
        "until": date_end
    },

]

data_names = {
    "drogenbos": ["level", "level", "level"],
    "quaidaa": ["level", "level", "level"],
    "veterinaires": ["oxygen", "level", "level"],
    "buda": ["flowrate", "level", "level"],
    "senneOUT": ["temperature", "acidity", "conductivity"]

}

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
                df.rename(columns={1: data_names[site['name']][0]}, inplace=True)
                df.rename(columns={2: data_names[site['name']][1]}, inplace=True)
                df.rename(columns={3: data_names[site['name']][2]}, inplace=True)
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
    global td_client, ableton_client, old_getmtime, local_config, df
    print("OSC thread démarré.")
    time_index = 0
    print("sending first hour")
    while True:
        if os.path.getmtime(CONFIG_FILE) != old_getmtime:
            old_getmtime = os.path.getmtime(CONFIG_FILE)
            config = load_config()  # recharge les paramètres à chaque boucle

            osc_address1 = config["osc_address1"]
            osc_address2 = config["osc_address2"]
            min_val = float(config["min_value"])
            max_val = float(config["max_value"])

            local_config = config
        else:
            osc_address1 = local_config["osc_address1"]
            osc_address2 = local_config["osc_address2"]
            min_val = float(local_config["min_value"])
            max_val = float(local_config["max_value"])

        df_drogenbos = dataframes.get("drogenbos")
        df_quaidaa = dataframes.get("quaidaa")
        df_buda = dataframes.get("buda")
        buda_flowrate_data = df_buda['flowrate'][int(time_index/5)] * ((4-(time_index % 5))/4) + df_buda['flowrate'][int(time_index/5) + 1] * ((time_index % 5)/4)
        buda_mapped_data = buda_flowrate_data

        drogenbos_level_data = df_drogenbos['level'][time_index]
        drogenbos_mapped_data = (drogenbos_level_data * (max_val - min_val) + min_val) / 100
        drogenbos_mapped_data = moyenne_glissante(drogenbos_for_mean, drogenbos_mapped_data)

        quaidaa_level_data = df_quaidaa['level'][time_index]
        quaidaa_mapped_data = (quaidaa_level_data * (max_val - min_val) + min_val) / 100
        quaidaa_mapped_data = moyenne_glissante(quaidaa_for_mean, quaidaa_mapped_data)

        ableton_client.send_message(osc_address1, buda_mapped_data)
        ableton_client.send_message(osc_address2, quaidaa_mapped_data)

        print(f"Envoi OSC {osc_address1}: {buda_mapped_data}")
        # print(f"Envoi OSC {osc_address2}: {quaidaa_mapped_data}")

        time.sleep(1)

        time_index += 1
        if time_index == 120:
            break


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


if __name__ == "__main__":
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

    # Démarre le thread OSC
    osc_thread = threading.Thread(target=osc_sender, daemon=True)
    osc_thread.start()

    # Lance Flask
    app.run(host="0.0.0.0", port=8000, debug=False)
