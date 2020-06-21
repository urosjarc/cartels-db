from py2neo import Node as NeoNode


class Path:
    def __init__(self, json: dict):
        self.confidence = -1
        self.street = None
        self.number = None
        self.name = None
        self.type = None
        self.latitude = None
        self.longitude = None

        for attr, val in self.__dict__.items():
            if val is None:
                setattr(self, attr, json.get(attr, None))


class CSVRow:
    def __init__(self, row):
        self._row = row

        self.firm = Firm()
        self.case = Case()
        self.undertaking = Undertaking()
        self.holding = Holding()

        self._createNodes()

    def _createNodes(self):
        for attr, val in self.__dict__.items():
            if isinstance(val, Node):
                node = getattr(self, attr)
                for nodeAttr in node.__dict__:
                    if not nodeAttr.startswith('_'):
                        node._setVar(nodeAttr, self._row[nodeAttr])

                node._init(self._row)


class Node:
    def __init__(self, name):
        self._data = None
        self._exists = None
        self._name = name
        self._instance = None

    def _setVar(self, key, value):
        setattr(self, key, value)

    def __str__(self):
        return str(self._instance)

    def _getAttr(self):
        varg = {}
        for name, val in self.__dict__.items():
            if not name.startswith('_'):
                varg[name] = val
        return varg

    def _init(self, data):
        self._data = data
        name = getattr(self, self._name, False)
        self._exists = name and not name.isspace()

        if self._exists:
            self._instance = NeoNode(self._name, **self._getAttr())

    def post_init(self):
        pass

class Case(Node):
    def __init__(self):
        super().__init__('Case')
        self.Case = None
        self.Press_Release = None
        self.EC_Date_of_decision = None
        self.GC_Case_number = None
        self.ECJ_Case_number = None
        self.Case_File = None
        self.Case_File_summary = None
        self.Case_File_French = None
        self.Case_File_Italian = None
        self.Case_File_German = None
        self.Case_File_Dutch = None

        self.GC_File = None
        self.ECJ_File = None
        self.DR_Event_File = None
        self.EC_Event_dec_file = None


class Firm(Node):
    def __init__(self):
        super().__init__('Firm')
        self.Firm = None
        self.Incorporation_state = None
        self.Firm_address = None
        self.Firm_type = None
        self.Ticker_firm = None
        self.Stock_exchange_firm = None

    def post_init(self):

        type = None
        if 'association' not in self.Firm_type and self.Ticker_firm is None:
            type = 'association'
        elif self.Ticker_firm is not None:
            type = 'public'
        else:
            type = 'association'
        self.Ticker_firm = type


class Undertaking(Node):
    def __init__(self):
        super().__init__('Undertaking')
        self.Undertaking = None
        self.IncorpStateUnder = None
        self.Under_address = None
        self.Ticker_undertaking = None
        self.Stock_exchange_undertaking = None


class Holding(Node):
    def __init__(self):
        super().__init__('Holding')
        self.Holding = None
        self.Holding_Ticker_parent = None
        self.Stock_exchange_holding = None
        self.Incorporation_state_for_holding = None


