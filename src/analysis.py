from src import db, utils
from datetime import datetime
import pycountry
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
        print(
            case['EC_Date_of_decision'],
            case['Readoption_amendment'],
            case['Ex_offo'],
            case['Notification'],
            case['Complaint'],
            case['Leniency'],
            case['Statement_of_objections'],
            case['Dawn_raid']
        )

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

def N_firms_within_under():
    '''
    N_firms_within_under (število firm notraj Undertaking-a znotraj Case-a)
    '''
    for undertaking in list(db.matcher.match('Undertaking')):
        num = db.graph.run('MATCH (f:Firm)-[r]->(u:Undertaking) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']
        print(num, '\t' ,undertaking['Undertaking'])

def Multiple_firm_under():
    '''
        Multiple_firm_under (dummy 01, če je samo ena firma znotraj undertakinga, potem 0, če jih je več 1)
    '''
    for undertaking in list(db.matcher.match('Undertaking')):
        num = db.graph.run('MATCH (f:Firm)-[r]->(u:Undertaking) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']
        print(1 if num>1 else 0, '\t' ,undertaking['Undertaking'])

def Repeat_firm_N_EC_cases():
    '''
    Repeat_Firm_N_EC_cases (število Case-ov, v katerih se pojavi to eno in isto podjetje)
    '''
    for firm in list(db.matcher.match('Firm')):
        num = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where f.Firm=$firm RETURN count(r)',
                           firm=firm['Firm']).data()[0]['count(r)']
        print(num, '\t' ,firm['Firm'])

def Recidivist_firm_D():
    '''Recidivist_firm_D (dummy 01, če se pojavi samo enkrat v Case-ih, potem, če se pojavi vsaj 2-krat, potem 1)'''
    for firm in list(db.matcher.match('Firm')):
        num = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where f.Firm=$firm RETURN count(r)',
                           firm=firm['Firm']).data()[0]['count(r)']
        print(1 if num >= 2 else 0, '\t' ,firm['Firm'])

def Recidivist_firm_2nd_time_D():
    '''
    Todo: Recidivist_firm_2nd_time_D (dummy 01, ko se firma, ki je recidivist datumsko glede na EC_Date_of_decision prvič pojavi v bazi je tudi 0 (in ne 1 kot pri prejšnjem dummy-ju), ko se pa pojavi časovno gledano drugič, tretjič itd. pa je 1)
    '''

def N_Firm_Inc_states_within_EC_case():
    '''
        N_Firm_Inc_states_within_EC_case (število vseh držav znotraj Case)
    '''
    for case in list(db.matcher.match('Case')):
        firms_r = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                           case=case['Case']).data()

        states = set()
        for firm_r in firms_r:
            states.add(firm_r['f']['Incorporation_state'])
        print(len(states),case['Case'], sep='\t->\t')

def European_firm():
    '''Todo: make this happend...'''
    for firm in list(db.matcher.match('Firm')):
        try:
            country = pycountry.countries.search_fuzzy(firm['Incorporation_state'])
        except:
            country = []
        c_r = None
        for c in country:
            if c.name == firm['Incorporation_state']:
                c_r = c
                break

        if c_r is not None:
            cinfo = utils.getCountryInfo(c_r.name)
            if cinfo is not None:
                print(cinfo.get('continent'), firm['Incorporation_state'])
            else:
                print('cinfo', firm['Incorporation_state'])
        else:
            print('c_r', firm['Incorporation_state'])

if __name__ == '__main__':
    European_firm()
