# PCO-Sheets
A Basic Utility that allows the syncing of information between Google Sheets and Planning Center

Syncs to Home Assistant through MQTT current state and is controlled via Home Assistant.

Scans each week in Sermon Planning Google Sheet, if the scripture exists for the week in that spreadsheet, then if the service doesn't exist create it, then import the scripture into planning center.