from xml.dom import NotFoundErr
from google.oauth2 import service_account # type: ignore
from googleapiclient.discovery import build # type: ignore
import os.path
from datetime import datetime
import http.client
import json
import base64
import ssl
import sys
import time
import schedule
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTMessage
from ha_mqtt_discoverable import Settings, DeviceInfo
from ha_mqtt_discoverable.sensors import Sensor, SensorInfo, BinarySensor, BinarySensorInfo, Button, ButtonInfo, Switch, SwitchInfo

debug = False
partial = False
mqtt_creds = None

if os.path.exists("creds.json"):
    with open('creds.json', 'r') as file:
        creds_data = json.load(file)
        mqtt_creds = creds_data.get('mqtt', {})

buttonMQTT = Settings.MQTT(
    host=mqtt_creds.get('broker_ip'),
    port=int(mqtt_creds.get('port')),
    username=mqtt_creds.get('username'),
    password=mqtt_creds.get('password'),
)

device_info = DeviceInfo(
    name="PCO Sheets Bridge",
    identifiers="pco_sheets_bridge",
)

def full_sync_callback(client, userdata, message: MQTTMessage):
    global partial, debug
    if debug: print("Full Sync button pressed.")
    partial = False
    binary_sensors["full_sync_running"].on()
    PCOSheetsRunSynchronization(2015)

def partial_sync_callback(client, userdata, message: MQTTMessage):
    global partial,debug
    if debug: print("Partial Sync button pressed.")
    partial = True
    binary_sensors["partial_sync_running"].on()
    PCOSheetsRunSynchronization(datetime.now().year)

def debug_switch_callback(client, user_data, message: MQTTMessage):
    global debug
    payload = message.payload.decode()
    if payload == "ON":
        debug = True
        print("Debug Mode Turned On!")
        debug_mode_switch.on()
    elif payload == "OFF":
        debug = False
        print("Debug Mode Turned Off!")
        debug_mode_switch.off()


full_sync_button_info = ButtonInfo(
    name="Full Sync",
    unique_id="full_sync_button",
    device=device_info,
)
full_sync_button = Button(
    Settings(
        mqtt=buttonMQTT,
        entity=full_sync_button_info,
    ),
    full_sync_callback,
)

partial_sync_button_info = ButtonInfo(
    name="Partial Sync",
    unique_id="partial_sync_button",
    device=device_info,
)
partial_sync_button = Button(
    Settings(
        mqtt=buttonMQTT,
        entity=partial_sync_button_info,
    ),
    partial_sync_callback,
)

debug_mode_switch = Switch(
    Settings(
        mqtt=buttonMQTT, 
        entity=SwitchInfo(
            name="Debug Mode",
            unique_id="debug_mode_switch",
            device=device_info,
        )
    ),
    debug_switch_callback,
)


full_sync_button.write_config()
partial_sync_button.write_config()


MQTTCLIENT = mqtt.Client(protocol=mqtt.MQTTv311)
MQTTCLIENT.username_pw_set(mqtt_creds.get('username'), mqtt_creds.get('password'))
MQTTCLIENT.connect(mqtt_creds.get('broker_ip'), int(mqtt_creds.get('port')), 60)
MQTTCLIENT.enable_logger()
mqtt_settings = Settings.MQTT(client=MQTTCLIENT)

device_info = {"name": "PCO Sheets Bridge", "identifiers": "pco_sheets_bridge"}


binary_sensors = {
    "full_sync_running": BinarySensor(
        Settings(
            mqtt=mqtt_settings,
            entity=BinarySensorInfo(
                name="Full Sync Running",
                device_class="running", 
                unique_id="full_sync_running",
                device=device_info,
            ),
        )
    ),
    "partial_sync_running": BinarySensor(
        Settings(
            mqtt=mqtt_settings,
            entity=BinarySensorInfo(
                name="Partial Sync Running",
                device_class="running",  
                unique_id="partial_sync_running",
                device=device_info,
            ),
        )
    ),
    "PCO-Sheets Online": BinarySensor(
        Settings(
            mqtt=mqtt_settings,
            entity=BinarySensorInfo(
                name="PCO-Sheets Online",
                device_class="connectivity",  
                unique_id="pco_sheets_online",
                device=device_info,
            ),
        )
    ),
}

text_sensors = {
    "currently_updating_service": Sensor(
        Settings(
            mqtt=mqtt_settings,
            entity=SensorInfo(
                name="Currently Updating Service",
                unique_id="currently_updating_service",
                device_class=None,
                unit_of_measurement=None,
                device=device_info,
            ),
        )
    ),
    "last_full_sync": Sensor(
        Settings(
            mqtt=mqtt_settings,
            entity=SensorInfo(
                name="Last Full Sync",
                unique_id="last_full_sync",
                device_class=None,
                unit_of_measurement=None,
                icon="mdi:calendar-clock",
                device=device_info,
            ),
        )
    ),
    "last_partial_sync": Sensor(
        Settings(
            mqtt=mqtt_settings,
            entity=SensorInfo(
                name="Last Partial Sync",
                unique_id="last_partial_sync",
                device_class=None,
                unit_of_measurement=None,
                icon="mdi:calendar-clock",
                device=device_info,
            ),
        )
    ),
}


# Finished Setting up MQTT, move to main

binary_sensors["PCO-Sheets Online"].on()
lastFound = 50
response = None
data = None

def queryPCO(service_type_id,offset,increment,application_id,secret):
    global response, data, debug
    HOST = 'api.planningcenteronline.com'
    URL = f'/services/v2/service_types/{service_type_id}/plans?offset={offset}&per_page={increment}&order=sort_date'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()
    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }
    if debug:
        print(f"#Debug: Querying PCO API")
    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()
    conn.close()

def get_plan_id_by_date(service_type_id, date, application_id, secret, findingService):
    
    global lastFound, response, data, debug
    if findingService:
        if data == "PewPewPew":
            mathNumber = 1000
            increment = 25
            offset = lastFound
            findingService = False
            queryPCO(service_type_id,offset,increment,application_id,secret)
            print(f"Initial service found")
        else:
            mathNumber = 1000
            offset = 0
            increment = 100
            print(f"Searching for starting position (patience)")
    else:
        mathNumber = 49
        offset = lastFound
        increment = 25

    while 3==1+2: #crazy math i know
        if response.status == 200:
            plans = json.loads(data)
            for plan in plans['data']:
                plan_date = plan['attributes']['dates']
                if plan_date == date:
                    if debug:
                        print(f"#Debug: Found Service on {date}")
                    lastFound = offset
                    if findingService:
                        if debug:
                            print(f"#Debug: Found ballpark, scanning narrowly")
                        data = "PewPewPew"
                        get_plan_id_by_date(service_type_id, date, application_id, secret, True)
                    return plan['id']
            offset+=increment
            if debug:
                print(f"#Debug: {date} not found, offset increasing to {offset}")
            queryPCO(service_type_id,offset,increment,application_id,secret)
        else:
            print(f"#Error: Failed to retrieve plans: {response.status}")
            return None

        if offset > lastFound + mathNumber:
            queryPCO(service_type_id,lastFound,increment,application_id,secret)
            raise NotFoundErr(f"No plan found for {date}")


def get_item_id_by_plan(service_type_id, plan_id, application_id, secret):
    global debug
    URL = f'/services/v2/service_types/{service_type_id}/plans/{plan_id}/items'
    HOST = 'api.planningcenteronline.com'
    conn = http.client.HTTPSConnection(HOST, context=ssl._create_unverified_context())
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()

    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }

    conn.request('GET', URL, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()

    if response.status == 200:
        items = json.loads(data)
        # Filter items by title and get the id
        for item in items['data']:
            if item['attributes']['title'] == 'Scripture Readings' or item['attributes']['title'] == 'Scripture Reading' or item['attributes']['title'] == 'Scripture':
                idToReturn = item['id']
                if debug:
                    print(f"#Debug: Found item Scripture Readings item in plan ID#{idToReturn}")
                break
        else:
            conn.close()
            raise NotFoundErr(f"Item 'Scripture Readings' not found")
    else:
        print(f"#Error: Failed to retrieve data: {response.status}")
        conn.close()
        sys.exit(1)
    conn.close()
    return idToReturn
    


def push_data_by_date(service_type_id, application_id, secret, datetime, SCRIPTURE_READING, findingService):
    global debug
    PLAN_ID = get_plan_id_by_date(service_type_id, datetime_to_string(datetime), application_id, secret, findingService)
    if not PLAN_ID:
        sys.exit(1)

    ITEM_ID = get_item_id_by_plan(service_type_id, PLAN_ID, application_id, secret)

    URL = f'/services/v2/service_types/{service_type_id}/plans/{PLAN_ID}/items/{ITEM_ID}'
    HOST = 'api.planningcenteronline.com'


    scriptureString = f"<p>{SCRIPTURE_READING}</p>\n<p><span style=\"font-size:8px;\">Imported by Noah's PCO-Sheets Bridge Utility at {datetime.now().strftime('%Y-%m-%d %I:%M %p')} </p>"

    payload = {
        "data": {
            "type": "Item",
            "id": ITEM_ID,
            "attributes": {
                "description": SCRIPTURE_READING,
                "html_details": scriptureString
            }
        }
    }
    payload_json = json.dumps(payload)
    auth = base64.b64encode(f'{application_id}:{secret}'.encode()).decode()
    headers = {
        'Authorization': f'Basic {auth}',
        'Content-Type': 'application/json'
    }
    context = ssl._create_unverified_context()
    conn = http.client.HTTPSConnection(HOST, context=context)
    conn.request('PATCH', URL, body=payload_json, headers=headers)
    response = conn.getresponse()
    data = response.read().decode()

    if response.status == 200:
        print(f"Updated service on {datetime_to_string(datetime)} with {SCRIPTURE_READING}")
        text_sensors["currently_updating_service"].set_state(datetime_to_string(datetime))
        if debug:
            print(f"#Debug: Successfully pushed \"{SCRIPTURE_READING}\" to Scripture Readings section of plan on {datetime_to_string(datetime)}")
    else:
        print(f"#Error: Failed to update 'html_details': {response.status}")
        print(data)

    conn.close()

def datetime_to_string(dt):
    return dt.strftime("%B %d, %Y").replace(" 0", " ").lstrip("0")

def PCOSheetsRunSynchronization(startYear):
    global partial, debug
    MQTTCLIENT.connect(mqtt_creds.get('broker_ip'), int(mqtt_creds.get('port')), 60)
    service_account_file = 'creds.json'
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    spreadsheet_id = '13PLI1UFcaAnyFp1MqTFLhC2Gy8nnvyFMQ5Kst6cTeY0'

    creds = None
    if os.path.exists(service_account_file):
        creds = service_account.Credentials.from_service_account_file(
            service_account_file, scopes=scopes)
        with open(service_account_file, 'r') as file:
            creds_data = json.load(file)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    values = []

    planning_center_creds = creds_data.get('planning_center', {})
    application_id = planning_center_creds.get('application_id')
    secret = planning_center_creds.get('secret')
    service_type_id = planning_center_creds.get('service_type_id')

    if not all([application_id, secret, service_type_id]):
        print("Planning Center credentials are missing in the JSON file.")
        return
    

    queryPCO(service_type_id,lastFound,25,application_id,secret)

    findingService = True

    for i in range(startYear, datetime.now().year + 1):
        range_string = f"{i}!A2:C60"
        try:
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_string).execute()
            values = result.get('values', [])
            if not values:
                print(f'#Error No data found for the year {i}.')
            else:
                for row_index, row in enumerate(values):
                    for col_index, cell in enumerate(row):
                        if col_index == 0:
                            values[row_index][col_index] = f"{cell}/{i}"
                    
                    currDate = datetime.strptime(values[row_index][0], '%m/%d/%Y') 
                    if values[row_index][1]:
                        values[row_index][1] = "NULL"
                    try:
                        if len(values[row_index]) == 3:
                            #values[row_index][1] = get_plan_id_by_date(service_type_id, datetime_to_string(currDate), application_id, secret)
                            if row_index == 0 or findingService:
                                push_data_by_date(service_type_id, application_id, secret, currDate, values[row_index][2], findingService)
                                findingService = False
                            else:
                                push_data_by_date(service_type_id, application_id, secret, currDate, values[row_index][2], findingService)
                        else:
                            print(f"No scripture provided for {datetime_to_string(currDate)}")
                    except Exception as e:   
                        print(f"#Error: Updating service on {datetime_to_string(currDate)}: {e}") 
        except Exception as e:
            print(f'#Error: An error occurred for the year {i}: {e}')
        binary_sensors["partial_sync_running"].off()
        binary_sensors["full_sync_running"].off()
        if partial:
            text_sensors["last_partial_sync"].set_state(datetime.now().strftime('%Y-%m-%d %I:%M %p'))
        else:
            text_sensors["last_full_sync"].set_state(datetime.now().strftime('%Y-%m-%d %I:%M %p'))

        print(f"Sync Complete at {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")


MQTTCLIENT.loop_start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Shutting Down")
    binary_sensors["partial_sync_running"].off()
    binary_sensors["full_sync_running"].off()
    binary_sensors["PCO-Sheets Online"].off()
    time.sleep(1)
    MQTTCLIENT.disconnect()
