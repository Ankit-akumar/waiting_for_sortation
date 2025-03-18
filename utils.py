from sqlalchemy import text
from datetime import datetime, timezone

# To get failed arrived messages of a bot from GTP influxDB
def get_failed_arrived_msg_by_botID(bot_id, iengine_greyorange):
    get_failed_arrived_msg_query = f'SELECT * FROM \"GreyOrange\".\"autogen\".\"zw_bot_events\" WHERE time > now() -3d AND time < now() AND bot_id = \'{bot_id}\' AND event = \'arrived\' AND processing_status = \'unprocessable\' order by time desc limit 1'
    failed_arrived_msg = iengine_greyorange.query(get_failed_arrived_msg_query)

    return failed_arrived_msg


# To get induct ID from induct_pps_mapping
def get_induct_id(front_pps_id, pengine_wms_process):
    with pengine_wms_process.connect() as connection:
        result = connection.execute(text(f"SELECT induct_id FROM induct_pps_mapping WHERE front_pps_id = \'{front_pps_id}\';"))
        induct_id = result.fetchone()[0]

    return induct_id


# To get the previous shipments sorted by the bot
def get_shipments_from_bot(time,pengine_cbort,bot_id):
    with pengine_cbort.connect() as connection:
        result = connection.execute(text(f"SELECT uid, inductid, botid, awb, dashboard_status, chuteid, destination, induct_time, sort_time, sort_success FROM data_shipment WHERE botid = '{bot_id}' and induct_time < '{time}' ORDER BY induct_time DESC LIMIT 2;"))
        shipments = []

        for row in result.fetchall():
            shipment_data = {
                "uid": row[0],
                "inductid": row[1],
                "botid": row[2],
                "awb": row[3],
                "dashboard_status": row[4],
                "chuteid": row[5],
                "destination": row[6],
                "induct_time": row[7],
                "sort_time": row[8],
                "sort_success": row[9]
            }
            shipments.append(shipment_data)

    return shipments


# To Print shipments
def print_shipments(shipments):
    print("Last two shipments sorted by the Bot -")
    for index, shipment in enumerate(shipments, start=1):
        print(f"\n Shipment {index}:")
        print(f"Shipment UID/Pick transaction ID: {shipment['uid']}")
        print(f"Induct ID/Front PPS ID: {shipment['inductid']}")
        print(f"RMS Bot ID: {shipment['botid']}")
        print(f"AWB/srms ID: {shipment['awb']}")
        print(f"Shipment Status in RMS: {shipment['dashboard_status']}")
        print(f"Chute ID: {shipment['chuteid']}")
        print(f"Destination: {shipment['destination']}")
        print(f"Induct Time: {shipment['induct_time']}")
        print(f"Sort Time: {shipment['sort_time']}")

        if shipment['sort_success'] == '':
            induct_time=datetime.strptime(shipment['induct_time'], "%Y-%m-%d %H:%M:%S.%f%z")
            current_time=datetime.now(timezone.utc)
            time_diff = current_time - induct_time
            print(f'Duration since induction time: {time_diff}')
        else:
            print(f"Sort Success: {shipment['sort_success']}")

        print("-" * 40)


# To get bot ids that arrived on the induct during the time interval
def get_failed_arrived_msg_by_ppsID(iengine_greyorange, timestamp_start, timestamp_end, front_pps_id):
    get_failed_arrived_msg = f'SELECT * FROM \"GreyOrange\".\"autogen\".\"zw_bot_events\" WHERE time > \'{timestamp_start}\' AND time < \'{timestamp_end}\' AND front_pps_id = {front_pps_id} AND event = \'arrived\' AND processing_status = \'unprocessable\' order by time limit 1'
    failed_arrived_msg = iengine_greyorange.query(get_failed_arrived_msg)

    return failed_arrived_msg
