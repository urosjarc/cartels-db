import pathlib
import csv
import os
import json
from src import auth, domain
import http.client, urllib.parse


def reformat_value(val: str, hasDates=False):
    if val.count('/') == 2:
        return val

    return val \
        .replace('(', '') \
        .replace(')', '') \
        .replace('/', '_') \
        .replace('.', '_') \
        .replace(' ', '_') \
        .replace('-', '_') \
        .replace('&', '') \
        .replace('%', '') \
        .replace("'", '')


def reformat_dict(row: dict, hasDates=False):
    delKeys = []
    pairs = []
    for k, v in row.items():
        new_key = reformat_value(k, hasDates)
        pairs.append([new_key, v])
        delKeys.append(k)
    for k in delKeys:
        row.pop(k)
    for k, v in pairs:
        row[k] = v

    return row


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


def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def saveCoordinates(rows, path):
    fields = ['Address', 'latitude', 'longitude', 'confidence', 'type', 'name', 'number', 'street']
    s = len(rows)

    with open(path, 'w') as csvfile:

        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)

        for i, row in enumerate(rows):
            if i % 10 == 0:
                print(f"Complete: {round(i / s * 100)}%")

            row: domain.CSV_Core = row
            print(i, row.firm.Incorporation_state, row.firm.Firm_address)
            c = getCoordinates(row.firm.Incorporation_state, row.firm.Firm_address)

            csvwriter.writerow([
                row.firm.Firm_address,
                c.latitude,
                c.longitude,
                c.confidence,
                c.type,
                c.name,
                c.number,
                c.street
            ])
