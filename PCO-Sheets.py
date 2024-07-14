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

increment = 25
lastFound = 50
response = None
data = None

def queryPCO(service_type_id,offset,increment,application_id,secret, debug):
    global response, data
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

def get_plan_id_by_date(service_type_id, date, application_id, secret, debug):
    
    global lastFound, increment, response, data
    offset = lastFound

    while True:
        if response.status == 200:
            plans = json.loads(data)
            for plan in plans['data']:
                plan_date = plan['attributes']['dates']
                if plan_date == date:
                    if debug:
                        print(f"#Debug: Found Service on {date}")
                    lastFound = offset
                    return plan['id']
            offset+=increment
            if debug:
                print(f"#Debug: {date} not found, offset increasing to {offset}")
            queryPCO(service_type_id,offset,increment,application_id,secret, debug)
        else:
            print(f"#Error: Failed to retrieve plans: {response.status}")
            return None

        if offset > lastFound + 49:
            queryPCO(service_type_id,lastFound,increment,application_id,secret, debug)
            raise NotFoundErr(f"No plan found for {date}")


def get_item_id_by_plan(service_type_id, plan_id, application_id, secret, debug):
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
    



def push_data_by_date(service_type_id, application_id, secret, datetime, SCRIPTURE_READING, debug):
    print(f"Updating service on {datetime_to_string(datetime)} with {SCRIPTURE_READING}")
    PLAN_ID = get_plan_id_by_date(service_type_id, datetime_to_string(datetime), application_id, secret, debug)
    if not PLAN_ID:
        sys.exit(1)

    ITEM_ID = get_item_id_by_plan(service_type_id, PLAN_ID, application_id, secret, debug)

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
        if debug:
            print(f"#Debug: Successfully pushed \"{SCRIPTURE_READING}\" to Scripture Readings section of plan on {datetime_to_string(datetime)}")
    else:
        print(f"#Error: Failed to update 'html_details': {response.status}")
        print(data)

    conn.close()


def datetime_to_string(dt):
    return dt.strftime("%B %d, %Y").replace(" 0", " ").lstrip("0")

def PCOSheetsRunSynchronization(debug):

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

    queryPCO(service_type_id,lastFound,increment,application_id,secret, debug)


    for i in range(2015, datetime.now().year + 1):
        range_string = f"{i}!A2:C60"
        try:
            result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_string).execute()
            values = result.get('values', [])
            if not values:
                print(f'#Error No data found for the year {i}.')
            else:
                for row_index, row in enumerate(values):
                    values[row_index] = [f"{cell}/{i}" for cell in row]
                    currDate = datetime.strptime(values[row_index][0], '%m/%d/%Y')
                    if values[row_index][1]:
                        values[row_index][1] = "NULL"
                    try:
                        if len(values[row_index]) == 3:
                            values[row_index][1] = get_plan_id_by_date(service_type_id, datetime_to_string(currDate), application_id, secret, debug)
                            values[row_index][2] = values[row_index][2][:-5]
                            push_data_by_date(service_type_id, application_id, secret, currDate, values[row_index][2], debug)
                        else:
                            print(f"No scripture provided for {datetime_to_string(currDate)}")
                    except Exception as e:   
                        print(f'#Error: {e}') 
        except Exception as e:
            print(f'#Error: An error occurred for the year {i}: {e}')

        print(f"Sync Complete at {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")

def parse_arguments():
    runFirst = False
    debug = False

    if len(sys.argv) > 1 and sys.argv[1].lower() in ('true', '1', 't', 'y', 'yes'):
        runFirst = True
    if len(sys.argv) > 2 and sys.argv[2].lower() in ('true', '1', 't', 'y', 'debug'):
        debug = True
    
    return runFirst, debug

runFirst, debug = parse_arguments()

if(runFirst):
    PCOSheetsRunSynchronization(debug)

print(f"Scheduled to Run at 1:00")
schedule.every().day.at("01:00:00").do(PCOSheetsRunSynchronization, debug)

while True:
    schedule.run_pending()
    time.sleep(1)
