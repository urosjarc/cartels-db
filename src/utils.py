import http
from src.countryinfo import countries
import json
import pathlib
import os
import urllib

from src import domain, auth


def reformat_value(val: str):
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


def reformat_dict(row: dict):
    pairs = []
    nrow = {}

    for k, v in row.items():
        new_key = reformat_value(k)
        pairs.append([new_key, v])
    for k, v in pairs:
        nrow[k] = v

    return nrow


def currentDir(_file_, path):
    return str(pathlib.PurePath(_file_).parent.joinpath(path))


def getCoordinates(query):
    conn = http.client.HTTPConnection('api.positionstack.com')

    params = urllib.parse.urlencode({
        'access_key': auth.positionstack,
        'query': query,
        # 'region': region,
        'limit': 1,
    })

    conn.request('GET', '/v1/forward?{}'.format(params))

    res = conn.getresponse()
    res: dict = json.loads(res.read())
    data = res.get('data', [{}])
    return domain.Path(data[0] if len(data) == 1 else {})

def getCountryInfo(country):
    for c in countries:
        if c['name'] == country:
            return c



def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))

