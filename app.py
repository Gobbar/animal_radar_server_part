import flask
from flask import Flask, jsonify, g
import time
import threading
from threading import Thread
from flask_apscheduler import APScheduler
import flask_apscheduler
import db_work
import sqlite3

class Config:
    SCHEDULER_API_ENABLED = True

app = Flask(__name__)
app.config.from_object(Config())

scheduler = APScheduler()

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = db_work.init_db()
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@scheduler.task('interval', id='week_interval', weeks=2, misfire_grace_time=900)
def cache():
    with scheduler.app.app_context():
        conn = get_db()
        db_work.archivate_data(conn)

@app.route('/', methods=['POST', 'GET'])
def index():
    conn = get_db()
    return jsonify(db_work.get_data(conn))

@app.route('/getLocationData', methods=['POST'])
def get_location_data():
    conn = get_db()
    #return jsonify(db_work.get_location_data(conn, flask.request.data))
    return jsonify(db_work.get_location_data(conn, flask.request.data))

@app.route('/cur_time')
def get_cur_time():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT strftime('%s', 'now') FROM  month_data")
    return jsonify(cur.fetchone())

@app.route('/addRecords', methods=['POST'])
def add_record():
    conn = get_db()
    res = db_work.add_data(conn, flask.request.data)
    return jsonify(res)


#def add_record():
#    print(flask.request.data)
#    conn = get_db()
#    res = db_work.add_data(conn, flask.request.data)
#    return jsonify(res)

@app.route('/get_day', methods=['GET'])
def get_day():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM month_data WHERE animal_date_time > strftime('%s', date('now', '-1 day'))")
    res = cur.fetchall()
    cur.close()
    return jsonify(res)

@app.route('/clearAllDB')
def clear_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM month_data")
    cur.close()
    return true

if __name__ == "__main__": 
    with app.app_context():
        g._database = db_work.init_db()
    app.run(debug=True)