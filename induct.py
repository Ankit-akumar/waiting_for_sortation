from database import DB_Connect
from datetime import datetime
from sqlalchemy import text

iengine_sms = DB_Connect().openconnection_influx_sms()
pengine_wms_process = DB_Connect().openconnection_postgres_wms_process()

# Case 2: When induct ID and timestamp is given

timestamp=input("Please enter the timestamp in UTC format(yyyy-mm-dd HH:MM): ")
front_pps_id = input("Please enter the Front PPS ID: ")

# Formating timestamp

dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
timestamp_start = dt.replace(second=0).isoformat() + "Z"
timestamp_end = dt.replace(second=59).isoformat() + "Z"
print(timestamp)
print(timestamp_start)
print(timestamp_end)

# Getting induct ID from front pps ID

with pengine_wms_process.connect() as connection:
    induct_id = connection.execute(text(f"SELECT induct_id FROM induct_pps_mapping WHERE front_pps_id = \'{front_pps_id}\';"))
    induct_id = induct_id.fetchone()[0]
    print(f"Induct ID is {induct_id}")

    if induct_id:
        # Getting bot ids that arrived on the induct during the time interval

        get_arrived_bots = f'SELECT bot_id FROM \"telegraf\".\"autogen\".\"induct_interaction_data\" WHERE time > \'{timestamp_start}\' AND time < \'{timestamp_end}\' AND station_id = \'{induct_id}\' AND status=\'"bot_arrival"\''
        arrived_bots = iengine_sms.query(get_arrived_bots)

        points_itr = arrived_bots.get_points()
        first_point = next(points_itr,None)

        if first_point:
            
            for point in arrived_bots.get_points():
                print(point['bot_id'])