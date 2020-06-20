import pathlib
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

