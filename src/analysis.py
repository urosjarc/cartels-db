from src import db
from datetime import datetime
import sys

this = sys.modules[__name__]
this.matcher = None

db.init_db()

def EC_duration():
    '''
    EC_duration (datum izdaje odločbe minus (najstarejši datum enega od stolpcev (Readoption_amendment Ex offo  Notification Complaint Leniency Statement of objections Dawn raid))
    TODO: Readoption_amendment je nedokoncan stolpec, kaj mi manjka???
    '''
    for case in list(db.matcher.match('Case')):
        print(case['Readoption_amendment'], case['EC_Date_of_decision'])

def EC_decision_year():
    '''
    EC_decision_year (vstavi se samo leto izdaje odločbe)
    '''
    for case in list(db.matcher.match('Case')):
        print(case['EC_Date_of_decision'].split('/')[-1])

def EC_dec_may_2004():
    '''
    EC_dec_may_2004 (dummy 01, če je bil datum izdaje pred 1. majem 2004 - 0, če je bil datum po 1)
    '''
    date_format = '%m/%d/%Y'
    weightDate = datetime.strptime('5/01/2004', date_format)
    for case in list(db.matcher.match('Case')):
        date = datetime.strptime(case['EC_Date_of_decision'], date_format)
        dummy = (weightDate - date).days > 0
        print(dummy)

def EC_dec_EN():
    #(dummy, 01, če je 1, če ni 0)
    for case in list(db.matcher.match('Case')):
        print(len(case['Case_File']) > 0, str(case['Case_File']).isspace(),'\t',case['Case_File'])

def N_firms_within_EC_case():
    '''
    N_firms_within_EC_case (število vseh firm znotraj Case)
    '''
    for case in list(db.matcher.match('Case')):
        num = db.graph.run('MATCH (Firm)-[r]->(c:Case) where c.Case=$case RETURN count(r)',
                         case=case['Case']).data()[0]['count(r)']
        print(num,'\t' ,case['Case'])







if __name__ == '__main__':
    N_firms_within_EC_case()
