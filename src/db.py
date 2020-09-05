from py2neo import Graph, NodeMatcher, RelationshipMatch
import sys
from src import auth as aut
from src.domain import *
from typing import List, Dict

matcher: NodeMatcher = None
graph: Graph = None

this = sys.modules[__name__]
this.graph: Graph = None
this.matcher: NodeMatcher = None

this.core_rows: List[CSV_Core] = []
this.stock_meta_rows: List[StockMeta] = []

this.A1012M_rows: List[StockData] = []
this.DSLOC_rows: List[StockDataOther] = []
this.LEV2IN_REL = {}
this.LEV2IN_rows: List[StockDataOther] = []
this.LEV4SE_REL = {}
this.LEV4SE_rows: List[StockDataOther] = []
this.MLOC_rows: List[StockDataOther] = []
this.TOTMKWD_rows: List[StockDataOther] = []

this.csvCorePathIn = utils.currentDir(__file__, '../data/csv/core.csv')
this.csv_EC_annual_data = utils.currentDir(__file__, '../data/csv/core_EC_annual_data.csv')
this.csv_ECJ_annual_data = utils.currentDir(__file__, '../data/csv/core_ECJ_annual_data.csv')
this.csvCorePathOut = utils.currentDir(__file__, '../data/csv/core_out.csv')
this.csvStockMetaPath = utils.currentDir(__file__, '../data/csv/stock-meta.csv')
this.csvStockDataPaths = [i for i in utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/A1012M/data'))]
this.csvStockDataAnnualPath = utils.currentDir(__file__, '../data/csv/A1012M/annual_figures.csv')
this.csvStockData2RelIndustryPaths= utils.currentDir(__file__, '../data/csv/LEV/REL_STOCK_LEV2IN.csv')
this.csvStockData2IndustryPaths = [i for i in
                                   utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/LEV/2IN'))]

this.csvStockData4RelIndustryPaths= utils.currentDir(__file__, '../data/csv/LEV/REL_STOCK_LEV4SE.csv')
this.csvStockData4IndustryPaths = [i for i in
                                   utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/LEV/4SE'))]
this.csvStockDataMLOCPaths = [i for i in utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/MLOC'))]
this.csvStockDataDSLOCPaths = [i for i in utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/DSLOC'))]
this.csvStockDataTOTMKWDPaths = [i for i in utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/TOTMKWD'))]

# DATABASE
core = None
core_fields = None
this.core: List[Dict] = []
this.core_EC_annual_data: List[Dict] = []
this.core_ECJ_annual_data: List[Dict] = []
this.core_fields: List[str] = []

def init_core():
    with open(this.csv_EC_annual_data, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core_EC_annual_data.append(row)

    with open(this.csv_ECJ_annual_data, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core_ECJ_annual_data.append(row)

    with open(this.csvCorePathIn, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core.append(row)

    this.core_fields = list(this.core[0].keys())

def save_core():
    with open(this.csvCorePathOut, 'w') as csvfile:
        writer= csv.DictWriter(csvfile,fieldnames=this.core_fields)
        writer.writeheader()
        writer.writerows(this.core)

def getFirmsRows(firmName):
    with open(this.csvCorePath) as csvfile:
        reader = csv.DictReader(csvfile)
        firms = {}
        for row in reader:
            if row['Firm'] == firmName:
                firms[row['Case']] = row

        return firms

def init():
    init_db()

    init_LEV_REL()

    init_nodes_core()
    init_nodes_stock_meta()
    init_nodes_stock_data_A1012M()
    init_nodes_stock_data_A1012M_annual()

    def industry_name(name):
        return name.replace("WORLD-DS ", "").split(' - ')[0]

    def mloc_name(name):
        return name.split(' - ')[0]

    def dsloc_name(name):
        return name.split(' - ')[0].replace("-DS", "")

    this.LEV2IN_rows = init_nodes_stock_data_other(this.csvStockData2IndustryPaths, industry_name, 'LEV2IN')
    this.LEV4SE_rows = init_nodes_stock_data_other(this.csvStockData4IndustryPaths, industry_name, 'LEV4SE')
    this.MLOC_rows = init_nodes_stock_data_other(this.csvStockDataMLOCPaths, mloc_name, 'MLOC')
    this.DSLOC_rows = init_nodes_stock_data_other(this.csvStockDataDSLOCPaths, dsloc_name, 'DSLOC')
    this.TOTMKWD_rows = init_nodes_stock_data_other(this.csvStockDataTOTMKWDPaths, dsloc_name, 'TOTMKWD')



def init_db():
    this.graph = Graph(uri=aut.dbUrl, auth=aut.neo4j, max_connection=3600 * 24 * 30, keep_alive=True)
    this.matcher = NodeMatcher(this.graph)

def init_nodes_core():
    with open(this.csvCorePath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core_rows.append(CSV_Core(row))


def init_nodes_stock_meta():
    with open(this.csvStockMetaPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.stock_meta_rows.append(StockMeta(row))


def init_nodes_stock_data_A1012M():
    # GET ALL FILES
    stockDatas = {}  # code -> path -> StockData
    names = []
    dates = {}
    for path in this.csvStockDataPaths:
        name = path.split("/")[-1].split(',')[1][1:].replace(' ', '_')
        print(f'Parsing: [{name + "]":<30} {path.split("/")[-1]}')

        names.append(name)
        dates[name] = None

        sds = StockData.parse_A1012M(path)

        # MERGE DATA
        for sd in sds:
            if dates[name] is None:
                dates[name] = sd.dates

            if sd.StockData in [None, '']:
                continue

            if sd.StockData not in stockDatas:
                setattr(sd, name, sd.values)
                setattr(sd, name + "_dates", sd.dates)
                delattr(sd, 'values')
                delattr(sd, 'dates')
                stockDatas[sd.StockData] = sd
            else:
                nameVal = getattr(stockDatas[sd.StockData], name, None)
                if nameVal is not None:
                    raise Exception(f'{name} is occuring multiple times for {sd.StockData}')
                setattr(stockDatas[sd.StockData], name, sd.values)
                setattr(stockDatas[sd.StockData], name + '_dates', sd.dates)

    # Adding missing properties
    missingVal = 0
    vals = 0
    for code, stockData in stockDatas.items():
        for n in names:
            data = getattr(stockData, n, None)

            if data is None:
                zerros = [-1 for i in dates[n]]
                setattr(stockData, n, zerros)
                setattr(stockData, n + "_dates", dates[n])
                missingVal += 1
            else:
                vals += 1
    # Initing

    for sd in stockDatas.values():
        sd.type = 'A1012M'
        sd._init()
        this.A1012M_rows.append(sd)


def init_nodes_stock_data_A1012M_annual():
    stockDatas = {}  # code -> name -> StockData
    uniqueNames = set()

    sds = StockData.parse_A1012M_annual(this.csvStockDataAnnualPath)
    sd_date = sds[0].dates
    sd_default = [-1 for d in sd_date]

    for sd in sds:
        name = utils.reformat_value(sd._data['Name'].split(' - ')[-1])
        if name == '#ERROR':
            continue

        name = 'annual_' + name

        uniqueNames.add(name)
        if sd.StockData not in stockDatas:
            stockDatas[sd.StockData] = {name: sd}
        else:
            stockDatas[sd.StockData][name] = sd

    for sd in this.A1012M_rows:
        if sd.StockData in stockDatas:

            setattr(sd, 'annual_dates', sd_date)

            for k, v in stockDatas[sd.StockData].items():
                setattr(sd, k, v.values)

            for un in uniqueNames.difference(list(stockDatas[sd.StockData].keys())):
                setattr(sd, un, sd_default)

            sd._init()


def init_nodes_stock_data_other(paths, nameReformat, type):
    stockDatas = {}  # code -> path -> StockData
    uniqueNames = set()
    def_val = None
    def_date = None

    for path in paths:

        name = path.split("/")[-1].split(',')[1][1:].replace(' ', '_')
        uniqueNames.add(name)

        print(f'Parsing: [{name + "]":<30} {path.split("/")[-1]}')

        sds = StockDataOther.parse(path)

        def_val = [-1 for _ in sds[0].values]
        def_date = sds[0].dates

        # MERGE DATA
        for sd in sds:
            sd.name = nameReformat(sd._data['Name'])

            if sd.StockDataOther not in stockDatas:
                setattr(sd, name, sd.values)
                setattr(sd, name + "_dates", sd.dates)
                delattr(sd, 'values')
                delattr(sd, 'dates')
                stockDatas[sd.StockDataOther] = sd
            else:
                nameVal = getattr(stockDatas[sd.StockDataOther], name, None)
                if nameVal is not None:
                    raise Exception(f'{name} is occuring multiple times for {sd.StockDataOther}')
                setattr(stockDatas[sd.StockDataOther], name, sd.values)
                setattr(stockDatas[sd.StockDataOther], name + '_dates', sd.dates)

    arr = []
    for sd in stockDatas.values():
        diff = uniqueNames.difference(sd.__dict__.keys())
        for att in diff:
            setattr(sd, att, def_val)
            setattr(sd, att + '_dates', def_date)

        sd.type = type
        sd._init()
        arr.append(sd)

    return arr

def init_LEV_REL():
    with open(this.csvStockData2RelIndustryPaths) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.LEV2IN_REL[row['code']] = row['name']

    with open(this.csvStockData4RelIndustryPaths) as csvfile:
        reader = csv.DictReader(csvfile, quotechar="'")
        for row in reader:
            this.LEV4SE_REL[row['code']] = row['name']

    pass

# DELETE ALL IN DATABASE
def delete_all():
    this.graph.delete_all()


# CREATE NODES
def create_nodes_core():
    size = len(this.core_rows)
    notExists = {
        'firm': 0,
        'case': 0,
        'holding': 0,
        'undertaking': 0,
    }

    for i, row in enumerate(this.core_rows):
        if i % 100 == 0:
            print(f'CREATING CORE NODES: {round(i / size * 100)}%')

        if row.firm._exists:
            this.graph.merge(row.firm._instance, 'Firm', 'Firm')
        if row.case._exists:
            this.graph.merge(row.case._instance, 'Case', 'Case')
        if row.holding._exists:
            this.graph.merge(row.holding._instance, 'Holding', 'Holding')
        else:
            notExists['holding'] += 1
        if row.undertaking._exists:
            this.graph.merge(row.undertaking._instance, 'Undertaking', 'Undertaking')
        else:
            notExists['undertaking'] += 1


def create_nodes_stock_meta():
    size_stock = len(this.stock_meta_rows)
    for i, row in enumerate(this.stock_meta_rows):
        if i % 100 == 0:
            print(f'CREATING STOCK META NODES: {round(i / size_stock * 100)}%')

        if row._exists:
            this.graph.merge(row._instance, 'StockMeta', 'StockMeta')


def create_nodes_stock_data():
    for name in ["A1012M", "DSLOC", "LEV2IN", "LEV4SE", "MLOC", "TOTMKWD"]:
        arr = getattr(this, name + "_rows")
        size = len(arr)
        parr = 'StockData' if name == "A1012M" else 'StockDataOther'
        log_iter = ((size // 10) if size > 10 else size)
        for i, row in enumerate(arr):
            if i % log_iter == 0:
                print(f'CREATING {name} NODES: {round(i / size * 100)}%')

            if row._exists:
                this.graph.merge(row._instance, parr, parr)


# CREATE CONNECTIONS
def create_relationships_core():
    size = len(this.core_rows)

    for i, row in enumerate(this.core_rows):
        if i % 100 == 0:
            print(f'CONNECTING CORE: {round(i / size * 100)}%')

        # Firm => Case
        if row.firm._exists and row.case._exists:
            this.graph.run(
                'MATCH (c:Case), (f:Firm) WHERE c.Case=$Case AND f.Firm=$Firm MERGE (f)-[r:REL_CORE]->(c) RETURN type(r)',
                Case=row.case.Case, Firm=row.firm.Firm)
        else:
            raise Exception(f'Not exists: {row.firm} {row.case}')

        # Firm => Undertaking
        if row.firm._exists and row.undertaking._exists:
            this.graph.run(
                'MATCH (f:Firm), (u:Undertaking) WHERE f.Firm=$Firm AND u.Undertaking=$Undertaking MERGE (f)-[r:REL_CORE]->(u) RETURN type(r)',
                Firm=row.firm.Firm, Undertaking=row.undertaking.Undertaking)
        else:
            raise Exception(f'Not exists: {row.firm} {row.undertaking}')

        # Undertaking => Holding
        if row.undertaking._exists and row.holding._exists:
            this.graph.run(
                'MATCH (u:Undertaking), (h:Holding) WHERE u.Undertaking=$Undertaking AND h.Holding=$Holding MERGE (u)-[r:REL_CORE]->(h) RETURN type(r)',
                Undertaking=row.undertaking.Undertaking, Holding=row.holding.Holding)


def create_relationships_stock_meta():
    size = len(this.stock_meta_rows)
    for i, row in enumerate(this.stock_meta_rows):
        if i % 100 == 0:
            print(f'CONNECTING STOCK META: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {row.StockMeta}')

        this.graph.run(
            'MATCH (sm:StockMeta), (f:Firm) WHERE sm.StockMeta=$StockMeta AND f.Stock_exchange_firm=$StockMeta MERGE (sm)-[r:REL_STOCK_META]->(f) RETURN type(r)',
            StockMeta=row.StockMeta)

        this.graph.run(
            'MATCH (sm:StockMeta), (u:Undertaking) WHERE sm.StockMeta=$StockMeta AND u.Stock_exchange_undertaking=$StockMeta MERGE (sm)-[r:REL_STOCK_META]->(u) RETURN type(r)',
            StockMeta=row.StockMeta)

        this.graph.run(
            'MATCH (sm:StockMeta), (h:Holding) WHERE sm.StockMeta=$StockMeta AND h.Stock_exchange_holding=$StockMeta MERGE (sm)-[r:REL_STOCK_META]->(h) RETURN type(r)',
            StockMeta=row.StockMeta)


def create_relationships_stock_A1012M():
    size = len(this.A1012M_rows)
    for i, row in enumerate(this.A1012M_rows):
        if i % 5 == 0:
            print(f'CONNECTING STOCK DATA A1012M: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {i} {row.__dict__}')

        this.graph.run(
            'MATCH (sm:StockMeta), (sd:StockData) WHERE sm.StockMeta=$StockData AND sd.StockData=$StockData MERGE (sm)-[r:REL_STOCK_A1012M]->(sd) RETURN type(r)',
            StockData=row.StockData)


def create_relationships_stock_LEV2IN():
    size = len(this.LEV2IN_rows)
    for i, row in enumerate(this.LEV2IN_rows):
        if i % 5 == 0:
            print(f'CONNECTING STOCK DATA LEV2IN: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {i} {row.__dict__}')

        this.graph.run(
            'MATCH (sm:StockMeta), (sd:StockDataOther) WHERE sm.LEVEL2_SECTOR_NAME=$name AND sd.StockDataOther=$code MERGE (sm)-[r:REL_STOCK_LEV2IN]->(sd) RETURN type(r)',
            name=this.LEV2IN_REL[row.StockDataOther], code=row.StockDataOther)


def create_relationships_stock_LEV4SE():
    size = len(this.LEV4SE_rows)
    for i, row in enumerate(this.LEV4SE_rows):
        if i % 5 == 0:
            print(f'CONNECTING STOCK DATA LEV4SE: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {i} {row.__dict__}')

        this.graph.run(
            'MATCH (sm:StockMeta), (sd:StockDataOther) WHERE sm.LEVEL4_SECTOR_NAME=$name AND sd.StockDataOther=$code MERGE (sm)-[r:REL_STOCK_LEV4SE]->(sd) RETURN type(r)',
            name=this.LEV4SE_REL[row.StockDataOther], code=row.StockDataOther)


def create_relationships_stock_DSLOC():
    size = len(this.DSLOC_rows)
    for i, row in enumerate(this.DSLOC_rows):
        if i % 5 == 0:
            print(f'CONNECTING STOCK DATA DSLOC: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {i} {row.__dict__}')

        this.graph.run(
            'MATCH (sm:StockMeta), (sd:StockDataOther) WHERE sm.DATASTREAM_INDEX=$code AND sd.StockDataOther=$code MERGE (sm)-[r:REL_STOCK_DSLOC]->(sd) RETURN type(r)',
            code=row.StockDataOther)


def create_relationships_stock_MLOC():
    size = len(this.MLOC_rows)
    for i, row in enumerate(this.MLOC_rows):
        if i % 5 == 0:
            print(f'CONNECTING STOCK DATA MLOC: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {i} {row.__dict__}')

        this.graph.run(
            'MATCH (sm:StockMeta), (sd:StockDataOther) WHERE sm.LOCAL_INDEX=$code AND sd.StockDataOther=$code MERGE (sm)-[r:REL_STOCK_MLOC]->(sd) RETURN type(r)',
            code=row.StockDataOther)


