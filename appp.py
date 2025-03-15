from database import DB_Connect
from sqlalchemy import text
from datetime import datetime

pengine_cbort = DB_Connect().openconnection_postgres_cbort()
iengine_autogen = DB_Connect().openconnection_influx_autogen()
pengine_wms_process = DB_Connect().openconnection_postgres_wms_process()

# CASE 1: When bot ID is given

bot_id = input("Please enter the bot id: ")

# Getting failed arrived message

get_failed_arrived_msg_query = f'SELECT * FROM \"GreyOrange\".\"autogen\".\"zw_bot_events\" WHERE time > now() - 3d AND time < now() AND bot_id = \'{bot_id}\' AND event = \'arrived\' AND processing_status = \'unprocessable\' order by time desc limit 1'
failed_arrived_msg = iengine_autogen.query(get_failed_arrived_msg_query)

time, back_pps_id, front_pps_id, processing_failure_reason = None, None, None, None

points_itr = failed_arrived_msg.get_points()
first_point = next(points_itr,None)

if first_point:
    # if a failed msg was found then retrieving the necessary details

    for point in failed_arrived_msg.get_points():
        time = point["received_at"]
        back_pps_id = point["back_pps_id"]
        front_pps_id = point["front_pps_id"]
        processing_failure_reason = point["processing_failure_reason"]

    print(f"Bot ID {bot_id} sent an unprocessable arrived event at {time} from back PPS {back_pps_id} and front PPS {front_pps_id} with failure reason - {processing_failure_reason}")

    # Getting induct ID from induct_pps_mapping

    with pengine_wms_process.connect() as connection:
        induct_id = connection.execute(text(f"SELECT induct_id FROM induct_pps_mapping WHERE back_pps_id = \'{back_pps_id}\' AND front_pps_id = \'{front_pps_id}\';"))
        induct_id = induct_id.fetchone()[0]
        print(f"Induct ID is {induct_id}")

        # Getting shipments the bot was carrying before arrived failed
        if induct_id:
            # Getting the previous shipment sorted by the bot

            # formatting time for postgres query
            dt = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
            formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")

            print(formatted_time)

            with pengine_cbort.connect() as connection:
                result = connection.execute(text(f"SELECT uid, inductid, botid, awb, dashboard_status, chuteid, destination, induct_time, sort_time FROM data_shipment WHERE botid = '{bot_id}' and induct_time < '{formatted_time}' ORDER BY induct_time DESC LIMIT 2;"))

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
                        "sort_time": row[8]
                    }
                    shipments.append(shipment_data) 

            if shipments:
                for index, shipment in enumerate(shipments, start=1):
                    print(f"\n Shipment {index}:")
                    print(f"Pick transaction ID: {shipment['uid']}")
                    print(f"Induct ID: {shipment['inductid']}")
                    print(f"Bot ID: {shipment['botid']}")
                    print(f"AWB: {shipment['awb']}")
                    print(f"Dashboard Status: {shipment['dashboard_status']}")
                    print(f"Chute ID: {shipment['chuteid']}")
                    print(f"Destination: {shipment['destination']}")
                    print(f"Induct Time: {shipment['induct_time']}")
                    print(f"Sort Time: {shipment['sort_time']}")
                    print("-" * 40) 
            else:
                print("No data found for the given bot_id.")
        
        else:
            print(f"Could not find induct id from induct_pps_mapping for back_pps_id = \'{back_pps_id}\' and front_pps_id = \'{front_pps_id}\'...")

else:
    print(f"No failed arrival messages found for bot {bot_id} in the past 3 days")