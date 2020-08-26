from src import utils
from datetime import datetime

def InfringeDurationOverall(firm):
    begin = {}
    end = {}
    for i in range(1, 13):
        dateBegin = utils.parseDate(firm[f'InfrBegin{i}'])
        dateEnd = utils.parseDate(firm[f'InfrEnd{i}'])
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
        endMax = utils.parseDate(firm['EC_Date_of_decision'])

    diff = None
    if endMax is not None and beginMin is not None:
        diff = (endMax - beginMin).days

    return diff
