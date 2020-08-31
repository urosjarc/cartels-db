from src import utils
from datetime import datetime

def InfringeDurationOverall(row):
    begin = {}
    end = {}
    for i in range(1, 13):
        dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
        dateEnd = utils.parseDate(row[f'InfrEnd{i}'])
        if dateBegin is not None:
            begin[i] = dateBegin
        if dateEnd is not None:
            end[i] = dateEnd

    beginMin: datetime = None
    endMax: datetime = None

    if len(begin.values()) > 0:
        beginMin = min(begin.values())
    if len(end.values()) > 0:
        endMax = max(end.values())

    if len(end.values()) == 0 and len(begin.values()) > 0:
        endMax = utils.parseDate(row['EC_Date_of_decision'])

    diff = None
    if endMax is not None and beginMin is not None:
        diff = (endMax - beginMin).days

    return (diff, beginMin, endMax)
