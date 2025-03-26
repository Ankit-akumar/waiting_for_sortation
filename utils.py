from sqlalchemy import text
from datetime import datetime, timezone
import logging
import os
import pandas as pd
import requests

def setup_logger(filename, console=False, level=logging.DEBUG):
    filename = os.path.dirname(os.path.realpath(__file__)) + '/logs/{}.log'.format(filename)
    logger = logging.getLogger(filename)
    logger.setLevel(level)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        #Creating Formatter
        formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")

        #Adding formatter to Console Handler
        console_handler.setFormatter(formatter)

        #Adding console handler to logging
        logger.addHandler(console_handler)

    else:
        file = logging.FileHandler(filename)
        formatter = logging.Formatter("%(asctime)s %(levelname)s - %(message)s")

        file.setFormatter(formatter)
        logger.addHandler(file)

    return logger

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
def get_shipments_from_bot(time,pengine_cbort,bot_id,file_logger):
    with pengine_cbort.connect() as connection:
        result = connection.execute(text(f"SELECT uid, inductid, botid, awb, dashboard_status, chuteid, destination, induct_time, sort_time, sort_success FROM data_shipment WHERE botid = '{bot_id}' and induct_time < '{time}' ORDER BY induct_time DESC LIMIT 2;"))
        file_logger.debug(f'Shipment data fetched - {result}')
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


# Get all failed arrived messages in the last 6 hours
def get_all_failed_arrived_msg(iengine_greyorange, file_logger):
    query = f'SELECT * FROM \"GreyOrange\".\"autogen\".\"zw_bot_events\" WHERE time > now()-6h AND time < now() AND event = \'arrived\' AND processing_status = \'unprocessable\' order by time'
    tables = iengine_greyorange.query(query)

    data = []
    for measurement, table in tables.items():
        for record in table:
            data.append({
                "bot_id": record.get("bot_id"),
                "time": record.get("received_at"),
                "front_pps_id": record.get("front_pps_id"),
                "back_pps_id": record.get("back_pps_id"),
                "processing_failure_reason": record.get("processing_failure_reason")
            })

    df = pd.DataFrame(data)

    if df.empty == True:
        return df

    # Ensure 'time' is in datetime format
    df["time"] = pd.to_datetime(df["time"])

    # Sort by time in desc order
    df = df.sort_values(by="time", ascending=False)

    # Get the lastest entry for each bot_id
    df = df.groupby("bot_id").first().reset_index()

    df_string = df.to_string()
    file_logger.debug(f'Failed arrived msg dataframe - {df_string}')
    # print(df_string)

    return df


# Get last drop message
def get_last_drop_msg_time_for_bot(iengine_greyorange, bot_id):
    query = f'SELECT * FROM \"GreyOrange\".\"autogen\".\"zw_bot_events\" WHERE time > now()-6h AND time < now() AND event = \'drop\' AND processing_status = \'success\' AND bot_id = \'{bot_id}\' order by time desc limit 1'
    last_drop_msg = iengine_greyorange.query(query)

    points_list = list(last_drop_msg.get_points())  # Convert iterator to list

    if points_list:
        for point in points_list:
            drop_time = point["received_at"]
            return drop_time
    return None


# Get all shipment data in the df
def get_failed_bots(pengine_cbort, df, file_logger, iengine_greyorange):
    failed_bots = []

    with pengine_cbort.connect() as connection:
        for index,row in df.iterrows():
            bot_id = row["bot_id"]
            time = row["time"]

            result = connection.execute(text(f"select count(*) from data_shipment where botid = '{bot_id}' and induct_time > '{time}';"))
            file_logger.debug(f'shiment count query result for bot_id={bot_id} {result}')
            result = result.fetchone()[0]

            if(result == 0):
                # Checking if the last drop msg was sent after last failed arrived msg

                # Get last drop message
                drop_time = get_last_drop_msg_time_for_bot(iengine_greyorange=iengine_greyorange,bot_id=bot_id)
                if drop_time:
                    drop_time = datetime.strptime(drop_time, "%Y-%m-%d %H:%M:%S.%f%z")
                    drop_time = drop_time.strftime("%Y-%m-%d %H:%M:%S")
                    drop_time = datetime.strptime(drop_time, "%Y-%m-%d %H:%M:%S")

                    if drop_time > time:
                        continue

                failed_bots.append(f'{bot_id}')
                file_logger.debug(f'Bot {bot_id} has not sorted any shipments since experiencing failed arrived event at {time}. Kindly check this bot')
                print(f"bot_id={bot_id} has not sorted any shipments since experiencing failed arrived event at {time}. Kindly check this bot")
            else:
                # file_logger.debug(f'Bot {bot_id} has sorted {result} shipments after experiencing failed arrived msg at {time}')
                # print(f"bot_id={bot_id} has sorted {result} shipments after experiencing failed arrived msg at {time}")
                pass

    return failed_bots


# Execution method by bot
def execute_by_bot(pengine_cbort, points_list, file_logger, iengine_greyorange, check_drop):
    file_logger.debug(f"failed arrived msg -> {points_list}")

    time, back_pps_id, front_pps_id, bin_id, processing_failure_reason = None, None, None, None, None

    # Iterate over all points
    for point in points_list:
        time = point["received_at"]
        back_pps_id = point["back_pps_id"]
        front_pps_id = point["front_pps_id"]
        bin_id = point["bin_id"]
        bot_id = point["bot_id"]
        processing_failure_reason = point["processing_failure_reason"]

    arrived_failure_output = f"Bot ID {bot_id} sent an unprocessable arrived event at {time} from back PPS {back_pps_id}, front PPS {front_pps_id} and bin {bin_id} with failure reason - {processing_failure_reason}"
    print(arrived_failure_output)
    file_logger.debug(arrived_failure_output)

    # formatting time for postgres query
    dt = datetime.strptime(time, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    file_logger.debug(f"formatted_time for postgres query - {formatted_time}")

    if check_drop:
        # Checking if the last drop msg was sent after last failed arrived msg
        drop_time = get_last_drop_msg_time_for_bot(iengine_greyorange=iengine_greyorange,bot_id=bot_id)
    
        if drop_time:
            drop_time = datetime.strptime(drop_time, "%Y-%m-%dT%H:%M:%SZ")
            drop_time = drop_time.strftime("%Y-%m-%d %H:%M:%S")
            drop_time = datetime.strptime(drop_time, "%Y-%m-%d %H:%M:%S")
            arrived_time = datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S")
            if drop_time > arrived_time:
                print("Drop event has already been sent for the bot")
                return

    # Getting previous shipments sorted by the bot
    shipments = get_shipments_from_bot(time=formatted_time,pengine_cbort=pengine_cbort,bot_id=bot_id,file_logger=file_logger)

    # printing shipments
    if shipments:
        print_shipments(shipments=shipments)
        file_logger.debug(f"shipments inducted on the bot - {shipments}")
        link_to_sop='https://work.greyorange.com/confluence/x/aWKxDQ'
        print(f"Kindly send the drop events manually. Here is the link to SOP - {link_to_sop}")
    else:
        file_logger.error(f"ERROR: No shipments found for the given bot {bot_id} before {time}")
        print(f"ERROR: No shipments found for the given bot {bot_id} before {time}")


# To check kakfa rabbitmq connectors
def check_kafka_rabbitmq_connector(file_logger):
    url = "http://172.28.62.190:8083/connectors?expand=status"

    try:
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            json_output = response.json()

            # Extracting required values
            connector_status = {
                key: value["status"]["connector"]["state"] for key, value in json_output.items()
            }

            file_logger.debug(f'connector_status: {connector_status}')
            print(connector_status)

            for connector, status in connector_status.items():
                if status!='RUNNING':
                    print('Kafka Rabbitmq connectors are not running. Please check\n')
                    return 0

            print('All connectors are running.\n')
            return 1
        else:
            print(f"Request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print("Error making request:", e)
