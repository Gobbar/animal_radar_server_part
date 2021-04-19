import flask_apscheduler

@scheduler.task('interval', id='week_interval', weel=1, misfire_grace_time=None)
def cache(db):
    db.archivate_data()