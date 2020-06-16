from py2neo import Node as NeoNode


class CSVRow:
    def __init__(self, row):
        self._row = row

        self.firm = Firm()
        self.case = Case()
        self.undertaking = Undertaking()
        self.holding = Holding()

        self._createNodes()
        self._createConnections()

    def _createNodes(self):
        for attr, val in self.__dict__.items():
            if isinstance(val, Node):
                node = getattr(self, attr)
                for nodeAttr in node.__dict__:
                    if not nodeAttr.startswith('_'):
                        node._setVar(nodeAttr, self._row[nodeAttr])

                node._init()

    def _createConnections(self):
        pass


class Node:
    def __init__(self, name):
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

    def _init(self):
        self._instance = NeoNode(self._name, **self._getAttr())

class Case(Node):
    def __init__(self):
        super().__init__('Case')
        self.Case = None
        self.EC_Date_of_decision = None


class Firm(Node):
    def __init__(self):
        super().__init__('Firm')
        self.Firm = None
        self.Incorporation_state = None
        self.Firm_address = None
        self.Firm_type = None


class Undertaking(Node):
    def __init__(self):
        super().__init__('Undertaking')
        self.Undertaking = None
        self.IncorpStateUnder = None
        self.Under_address = None


class Holding(Node):
    def __init__(self):
        super().__init__('Holding')
        self.Holding = None
        self.Holding_Ticker_parent = None
