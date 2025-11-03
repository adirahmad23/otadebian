import paho.mqtt.client as mqtt
import requests
import zipfile
import io
import os
import sys
import json
import logging
from packaging.version import parse as parse_version
import time

# --- KONFIGURASI PERANGKAT ---
THINGSBOARD_HOST = "103.164.213.46"
ACCESS_TOKEN = "47pglrbuhwtygbtqf1ld"

# Direktori tempat file program akan diinstal/diperbarui
INSTALL_DIR = os.path.dirname(os.path.abspath(__file__))
VERSION_FILE = os.path.join(INSTALL_DIR, "VERSION.txt")
PYTHON_EXECUTABLE = sys.executable

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- FUNGSI UTILITY ---

def get_local_version():
    """Membaca versi saat ini dari file VERSION.txt."""
    try:
        with open(VERSION_FILE, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return "1.0.0" # Default awal jika file belum ada

def send_fw_status(client, state, version=None):
    """Mengirim status firmware (fw_state) dan versi (fw_current_version) ke ThingsBoard."""
    payload = {"fw_state": state}
    if version:
        payload["fw_current_version"] = version
    
    # QoS 1 memastikan pengiriman pesan yang andal
    client.publish('v1/devices/me/telemetry', json.dumps(payload), qos=1)
    logging.info(f"Mengirim status: {state}, Versi: {version}")

# --- FUNGSI UTAMA OTA ---

def perform_update(client, url, target_version):
    """Menjalankan proses pengunduhan, ekstraksi, dan restart OTA."""
    send_fw_status(client, "DOWNLOADING", get_local_version())
    
    try:
        logging.info(f"Memulai unduhan dari: {url}")
        
        # 1. Unduh File ZIP (GitHub Archive URL)
        file_response = requests.get(url, stream=True, timeout=300)
        file_response.raise_for_status() # Cek kode status HTTP (misalnya 404, 500)

        # 2. Ekstraksi dan Instalasi
        logging.info("Mengunduh selesai. Memulai ekstraksi...")
        z = zipfile.ZipFile(io.BytesIO(file_response.content))

        # Nama folder root dari Archive URL: [REPO_NAME]-[TAG_VERSION] (misal: otadebian-1.0.1)
        root_folder = z.namelist()[0].split('/')[0]

        for member in z.namelist():
            if member.endswith('/'):
                continue
            
            # Buat path relatif baru, menghilangkan folder root untuk mengekstrak konten di direktori saat ini
            target_path = os.path.join(INSTALL_DIR, member.replace(f'{root_folder}/', '', 1))
            
            # Lewati file VERSION.txt jika sudah ada, atau buat pengecualian lain
            # if target_path == VERSION_FILE: continue 

            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            
            # Tulis konten file
            with z.open(member) as source, open(target_path, "wb") as target:
                target.write(source.read())

        # 3. Perbarui File Versi
        with open(VERSION_FILE, 'w') as f:
            f.write(target_version)
        
        send_fw_status(client, "UPDATED", target_version)
        logging.info(f"Pembaruan file berhasil ke {target_version}. Melakukan restart...")
        
        # Beri waktu ThingsBoard menerima status UPDATED sebelum restart
        time.sleep(2) 
        
        # 4. Restart Program
        # os.execv mengganti proses saat ini, memastikan program baru dimuat
        os.execv(PYTHON_EXECUTABLE, [PYTHON_EXECUTABLE] + sys.argv) 
        
    except Exception as e:
        logging.error(f"Update GAGAL: {e}")
        # Kirim status kegagalan
        send_fw_status(client, "FAILED", get_local_version())

# --- MQTT CALLBACKS ---

def on_connect(client, userdata, flags, rc):
    """Dipanggil ketika koneksi ke broker berhasil."""
    if rc == 0:
        logging.info("Terkoneksi ke ThingsBoard.")
        # Berlangganan ke topik shared attributes (untuk menerima perintah OTA)
        client.subscribe('v1/devices/me/attributes/share') 
        
        # Kirim versi saat ini ke ThingsBoard saat startup
        send_fw_status(client, "NONE", get_local_version())
    else:
        logging.error(f"Gagal terkoneksi, kode: {rc}")

def on_message(client, userdata, msg):
    """Dipanggil ketika pesan dari broker diterima."""
    if msg.topic == 'v1/devices/me/attributes/share':
        try:
            data = json.loads(msg.payload.decode())
            logging.info(f"Perintah diterima: {data}")

            fw_version_str = data.get('fw_version')
            fw_url = data.get('fw_url')
            
            local_version_str = get_local_version()
            
            # Perbandingan versi menggunakan pustaka packaging
            if fw_version_str and fw_url and parse_version(fw_version_str) > parse_version(local_version_str):
                logging.warning(f"Pembaruan tersedia: {local_version_str} -> {fw_version_str}")
                perform_update(client, fw_url, fw_version_str)
            else:
                logging.info("Versi ThingsBoard sama atau lebih rendah. Tidak ada pembaruan.")

        except Exception as e:
            logging.error(f"Gagal memproses pesan: {e}")

# --- PROGRAM UTAMA ---

if __name__ == "__main__":
    # Inisialisasi MQTT
    mqtt_client = mqtt.Client(client_id=ACCESS_TOKEN)
    mqtt_client.username_pw_set(username=ACCESS_TOKEN)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect(THINGSBOARD_HOST, 1883, 60)
    except Exception as e:
        logging.critical(f"Koneksi MQTT GAGAL: {e}")
        sys.exit(1)
        
    logging.info(f"Program berjalan. Versi lokal: {get_local_version()}")
    
    # Loop utama (menjaga koneksi dan mendengarkan)
    mqtt_client.loop_forever()