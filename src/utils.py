import pathlib
import csv
import json
from src import auth, domain
import http.client, urllib.parse

def currentDir(_file_, path):
    return str(pathlib.PurePath(_file_).parent.joinpath(path))

def getCoordinates(region, query) -> domain.Path:
    conn = http.client.HTTPConnection('api.positionstack.com')

    params = urllib.parse.urlencode({
        'access_key': auth.positionstack,
        'query': query,
        'region': region,
        'limit': 1,
    })

    conn.request('GET', '/v1/forward?{}'.format(params))

    res = conn.getresponse()
    res: dict = json.loads(res.read())
    data = res.get('data', [{}])
    return domain.Path(data[0] if len(data) == 1 else {})

def saveCoordinates(rows, path):
    # field names
    fields = ['Address', 'latitude', 'longitude', 'confidence', 'type', 'name', 'number', 'street']

    # data rows of csv file
    rows_csv = [ ]
    s = len(rows)
    for i, row in enumerate(rows):
        if i%10 == 0:
            print(f"Complete: {round(i/s * 100)}%")
        row: domain.CSVRow = row
        c = getCoordinates(row.firm.Incorporation_state, row.firm.Firm_address)
        rows_csv.append([
            row.firm.Firm_address,
            c.latitude,
            c.longitude,
            c.confidence,
            c.type,
            c.name,
            c.number,
            c.street
        ])


    # writing to csv file
    with open(path, 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        csvwriter.writerows(rows_csv)

