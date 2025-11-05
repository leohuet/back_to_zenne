import threading
import time
from datetime import datetime, timedelta
import random
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

# Informations d'authentification
CID = '9D9E9DE1F0E437A6'
presentday = datetime.now()
format_presentday = presentday.strftime('%Y%m%d')
yesterday = (presentday - timedelta(1)).strftime('%Y%m%d')
date = f'{yesterday}0000'
date_end = f'{format_presentday}0000'
# --- Sites & canaux ---
sites = [
    {
        "name": "drogenbos",
        "uid": "4A5190B93E170A87",
        "histdata": "histdata0",
        "channel": '"level"'
    },
    {
        "name": "quaidaa",
        "uid": "9DD946B760E34493",
        "histdata": "histdata0",
        "channel": '"ch1"'
    },
    {
        "name": "veterinaires",
        "uid": "4A26A91BCA0FE58C",
        "histdata": "histdata0",
        "channel": '"xch4"'
    },
    {
        "name": "buda",
        "uid": "A35E8E5A539949A7",
        "histdata": "histdata5",
        "channel": '"ch0"'
    },
    {
        "name": "senneOUT",
        "uid": "4B845F9C7151AC54",
        "histdata": "histdata0",
        "channel": '"temp", "conduct", "ph"'
    },

]

data_names = {
    "drogenbos": ["level", "level", "level"],
    "quaidaa": ["level", "level", "level"],
    "veterinaires": ["oxygen", "level", "level"],
    "buda": ["flowrate", "level", "level"],
    "senneOUT": ["temperature", "acidity", "conductivity"]

}

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
    # On ajoute la nouvelle valeur à la fin
    data_array.append(new_value)
    # On garde seulement les 'size_mean' dernières valeurs
    if len(data_array) > size_mean:
        data_array.pop(0)
    # On calcule la moyenne
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


# === GESTION DE LA CONFIG ===
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        config = {
            "ip_address": "127.0.0.1",
            "td_port": 9001,
            "ableton_port": 8001,
            "osc_address": "/value",
            "min_value": 0.0,
            "max_value": 100.0
        }
        save_config(config)
        return config


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

        drogenbos_level_data = df_drogenbos['level'][time_index]
        drogenbos_mapped_data = (drogenbos_level_data * (max_val - min_val) + min_val) / 100
        drogenbos_mapped_data = moyenne_glissante(drogenbos_for_mean, drogenbos_mapped_data)

        quaidaa_level_data = df_quaidaa['level'][time_index]
        quaidaa_mapped_data = (quaidaa_level_data * (max_val - min_val) + min_val) / 100
        quaidaa_mapped_data = moyenne_glissante(quaidaa_for_mean, quaidaa_mapped_data)

        ableton_client.send_message(osc_address1, drogenbos_mapped_data)
        ableton_client.send_message(osc_address2, quaidaa_mapped_data)

        print(f"Envoi OSC {osc_address1}: {drogenbos_mapped_data}")
        print(f"Envoi OSC {osc_address2}: {quaidaa_mapped_data}")
        time.sleep(1)
        time_index += 1


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
    # --- Authentification ---
    user = os.getenv("FLOWBRU_USER")
    password = os.getenv('FLOWBRU_PASS')
    message = f"{user}:{password}"
    message_bytes = message.encode('ascii')
    base64_bytes = b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    my_headers = {"Authorization": "Basic " + base64_message}

    # --- Stockage des DataFrames ---
    dataframes = {}

    # --- Récupération des données pour chaque site ---
    for site in sites:
        url = f'https://www.flowbru.eu/api/1/customers/{CID}/sites/{site["uid"]}/{site["histdata"]}?json={{"select":[{site["channel"]}],"from":"{date}","until":"{date_end}"}} '
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
                # Renommer la colonne de valeur pour qu’elle corresponde au site
                value_col = site["channel"]
                df.rename(columns={0: "date"}, inplace=True)
                for i in range(1, len(df.columns)):
                    df[i] = pd.to_numeric(df[i], errors='coerce')
                    df = df[df[i].notna()]
                    df[i] = df[i].round(2)
                df.rename(columns={1: data_names[site['name']][0]}, inplace=True)
                df.rename(columns={2: data_names[site['name']][1]}, inplace=True)
                df.rename(columns={3: data_names[site['name']][2]}, inplace=True)
                # Ajouter le nom du site en colonne (optionnel mais utile pour concat)
                df['site'] = site['name']
                """if value_col == "flowrate":
                    # Forcer la conversion en float
                    df['flow_m3s'] = pd.to_numeric(df['flow_m3s'], errors='coerce')
                    # Nettoyage du champ flow_m3s
                    df['flow_m3s'] = df['flow_m3s'].astype(str).str.replace(',', '.', regex=False)
                    df['flow_m3s'] = pd.to_numeric(df['flow_m3s'], errors='coerce')
                    # Supprimer les lignes vides ou NaN
                    df = df[df['flow_m3s'].notna()]
                    # Conversion spécifique pour Drogenbos
                    df['flow_m3s'] = df.apply(
                        lambda row: row['flow_m3s'] / 1000 if row['site'] == 'drogenbos' else row['flow_m3s'], axis=1)
                    # Arrondi à 3 décimales
                    df['flow_m3s'] = df['flow_m3s'].round(2)
                elif site['name'] == "drogenbos":
                    df['level'] = pd.to_numeric(df['level'], errors='coerce')
                    df = df[df['level'].notna()]
                    df['level'] = df['level'].round(2)
                    df['level'] = (df['level'] - min(df['level'])) / (max(df['level']) - min(df['level']))
                elif site['name'] == "viangros":
                    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
                    df = df[df['temperature'].notna()]
                    df['temperature'] = df['temperature'].round(2)
                    df['ph'] = pd.to_numeric(df['ph'], errors='coerce')
                    df = df[df['ph'].notna()]
                    df['ph'] = df['ph'].round(2)
                    df['conductivity'] = pd.to_numeric(df['conductivity'], errors='coerce')
                    df = df[df['conductivity'].notna()]
                    df['conductivity'] = df['conductivity'].round(2)
                elif site["name"] == "quaidaa":
                    df['level'] = pd.to_numeric(df['level'], errors='coerce')
                    df = df[df['level'].notna()]
                    df['level'] = df['level'].round(2)
                    df['level'] = (df['level'] - min(df['level'])) / (max(df['level']) - min(df['level']))"""

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
