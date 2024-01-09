import requests
import json
import logging
from tqdm import tqdm

logger = logging.getLogger('my_app')

def upload_to_airtable(api_key, base_id, table_name, data):
    # Fetch all records from Airtable
    logger.info("Fetching all records from Airtable.")
    
    try:    
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        airtableRecords = []
        offset = None
        while True:
            url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
            if offset:
                url += f"?offset={offset}"
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error("Failed to fetch records from Airtable.")
                return

            responsePage = response.json()
            airtableRecords.extend(responsePage['records']);

            offset = responsePage.get('offset')
            if not offset:
                break

        if response.status_code != 200:
            logger.error("Failed to fetch records from Airtable.")
            return
        
        airtableRecordsDict = {record['fields'].get('LiQ - Payroll Number'): record for record in airtableRecords}


        # Convert excel data to dictionary
        logger.info('Converting excel data to dictionary.')
        records = data.to_dict('records')
        records_to_insert = []
        records_to_update = []

        # Compare excel data with Airtable data
        logger.info("Processing records.")
        for record in records:
            payroll_number = record.get('LiQ - Payroll Number')
            if payroll_number:
                match = airtableRecordsDict.get(payroll_number, None)
                if match:
                    data_record = {k: record[k] for k in record.keys()}
                    match_record = {k: match['fields'].get(k, 0 if isinstance(record[k], int) else '') for k in record.keys()}
                    needs_update = False
                    for key in data_record:
                        if data_record[key] != match_record.get(key, None):
                            needs_update = True
                            # logger.debug(f"Field {key} differs. Data: {data_record[key]}, Match: {match_record.get(key)}")
                            break
                    if needs_update:
                        record_to_update = {"id": match['id'], "fields": data_record}
                        records_to_update.append(record_to_update)
                else:
                    if payroll_number not in airtableRecordsDict:
                        records_to_insert.append({"fields": record})
            else:
                logger.warning(f"Record missing 'LiQ - Payroll Number': {record}")

        # Batch update records
        if (len(records_to_update) > 0):
            logger.info("Batch updating records.")
            for i in tqdm(range(0, len(records_to_update), 10), desc="Updating records", unit="batch"):
                batch = records_to_update[i:i+10]
                response = requests.patch(f"https://api.airtable.com/v0/{base_id}/{table_name}", headers=headers, data=json.dumps({"records": batch}))

        # Batch insert records
        if (len(records_to_insert) > 0):
            logger.info("Batch inserting records.")
            for i in tqdm(range(0, len(records_to_insert), 10), desc="Inserting records", unit="batch"):
                batch = records_to_insert[i:i+10]
                response = requests.post(f"https://api.airtable.com/v0/{base_id}/{table_name}", headers=headers, data=json.dumps({"records": batch}))

    except Exception as e:
        logger.error(f"Failed to upload data to Airtable: {e}")
        raise