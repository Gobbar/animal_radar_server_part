import sqlite3
import uuid
import json
import time

def init_db():
    conn = sqlite3.connect("animal_pos.db")
    select_month_data = """SELECT * FROM month_data WHERE animal_date_time >= date('now', '-1 month')"""
    cur = conn.cursor()
    sql = """CREATE TABLE IF NOT EXISTS '%s' (
        id BLOB PRIMARY KEY,
        longitude REAL,
        latitude REAL,
        animal_date_time INTEGER,
        add_timestamp REAL
    )"""
    """SELECT * FROM month_data WHERE animal_date_time < date('now', '-1 month')"""
    cur.execute(sql % 'month_data')
    cur.execute(sql % 'archive')
    conn.commit()
    cur.close()
    return conn

def archivate_data(conn):
    cur = conn.cursor()
    cur.execute("""SELECT * FROM month_data WHERE animal_date_time > date('now', '-1 month')""")
    data_to_archive = cur.fetchall()
    id_list = [(data_row[0],) for i in data_to_archive]
    print(data_to_archive)
    cur.executemany("""DELETE FROM month_data WHERE id IN (?)""", id_list)
    conn.commit()
    cur.executemany("""INSERT INTO archive (id, longitude, latitude, date_time) VALUES(?,?,?,?)""", data_to_archive)
    conn.commit()
    cur.close()

def add_data(conn, data):
    cur = conn.cursor()
    add_req_sql = """INSERT INTO month_data (id, longitude, latitude, animal_date_time, add_timestamp)
        VALUES (?, ?, ?, ?, ?)"""
    req_data = json.JSONDecoder().decode(data.decode('utf-8'))
    #TODO перевод uuid в blob и обратно
    for row in req_data:
        record_data = (
            row["id"], 
            row["longitude"], 
            row["latitude"], 
            row["animal_date_time"], 
            time.time()
        )
        cur.execute(add_req_sql, record_data)
    
    conn.commit()
    cur.close()
    return True

def get_data(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT id, longitude, latitude, animal_date_time FROM month_data")
    res = [ {
        "id": data_row[0], 
        "longitude": data_row[1], 
        "latitude": data_row[2], 
        "animal_date_time": data_row[3]
     } for data_row in cur.fetchall() ]
    cur.close()
    return res

def get_some_data(db_connection):
    cur = db_connection.cursor()
    cur.execute("SELECT * FROM month_data WHERE animal_date_time > date('now', '-1 day')")
    return cur.fetchall()

def get_location_data(db_connection, data):
    cur = db_connection.cursor()
    req_data = json.JSONDecoder().decode(data.decode('utf-8'))
    get_data_sql = """SELECT id, longitude, latitude, animal_date_time FROM month_data
        WHERE longitude BETWEEN ?-5 AND ?+5 AND latitude BETWEEN ?-5 AND ?+5"""
    cur.execute(get_data_sql, (req_data["longitude"], req_data["longitude"], req_data["latitude"], req_data["latitude"]))
    res = [ {
        "id": data_row[0], 
        "longitude": data_row[1], 
        "latitude": data_row[2], 
        "animal_date_time": data_row[3]
     } for data_row in cur.fetchall() ]
    cur.close()
    return res