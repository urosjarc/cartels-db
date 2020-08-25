from src import db, utils
from datetime import datetime
import sys

this = sys.modules[__name__]
this.matcher = None

db.init_db()


def EC_duration():
    '''
    EC_duration (datum izdaje odločbe minus (najstarejši datum enega od stolpcev (Readoption_amendment Ex offo  Notification Complaint Leniency Statement of objections Dawn raid))
    '''
    dates = [
        'Readoption_amendment',
        'Ex_offo',
        'Notification',
        'Complaint',
        'Leniency',
        'Statement_of_objections',
        'Dawn_raid',
    ]

    for case in db.matcher.match('Case'):
        oldest = datetime.now()
        for date in dates:
            timestamp = utils.parseDate(case[date])
            if timestamp is not None and oldest > timestamp:
                oldest = timestamp

        if oldest is None:
            raise Exception('Not found oldest!')

        dateOfDecision = utils.parseDate(case['EC_Date_of_decision'])
        case['EC_duration'] = (dateOfDecision - oldest).days
        db.graph.push(case)


def EC_decision_year():
    '''
    EC_decision_year (vstavi se samo leto izdaje odločbe)
    '''
    for case in db.matcher.match('Case'):
        case['EC_decision_year'] = int(case['EC_Date_of_decision'].split('/')[-1])
        db.graph.push(case)


def EC_dec_may_2004():
    '''
    EC_dec_may_2004 (dummy 01, če je bil datum izdaje pred 1. majem 2004 - 0, če je bil datum po 1)
    '''
    weightDate = utils.parseDate('5/01/2004')
    for case in db.matcher.match('Case'):
        date = utils.parseDate(case['EC_Date_of_decision'])
        case['EC_dec_may_2004'] = (weightDate - date).days > 0
        db.graph.push(case)


def EC_dec_EN():
    # (dummy, 01, če je 1, če ni 0)
    for case in db.matcher.match('Case'):
        case['EC_dec_EN'] = utils.exists(case['Case_File'])
        db.graph.push(case)


def N_firms_within_EC_case():
    '''
    N_firms_within_EC_case (število vseh firm znotraj Case)
    '''
    for case in db.matcher.match('Case'):
        num = db.graph.run('MATCH (Firm)-[r]->(c:Case) where c.Case=$case RETURN count(r)',
                           case=case['Case']).data()[0]['count(r)']
        case['N_firms_within_EC_case'] = int(num)
        db.graph.push(case)


def N_firms_within_under():
    '''
    N_firms_within_under (število firm notraj Undertaking-a znotraj Case-a)
    '''
    for undertaking in db.matcher.match('Undertaking'):
        num = db.graph.run('MATCH (f:Firm)-[r]->(u:Undertaking) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']
        undertaking['N_firms_within_under'] = int(num)
        db.graph.push(undertaking)


def Multiple_firm_under():
    '''
        Multiple_firm_under (dummy 01, če je samo ena firma znotraj undertakinga, potem 0, če jih je več 1)
    '''
    for undertaking in db.matcher.match('Undertaking'):
        num = db.graph.run('MATCH (f:Firm)-[r]->(u:Undertaking) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']
        undertaking['Multiple_firm_under'] = 1 if int(num) > 1 else 0
        db.graph.push(undertaking)


def Repeat_firm_N_EC_cases():
    '''
    Repeat_Firm_N_EC_cases (število Case-ov, v katerih se pojavi to eno in isto podjetje)
    '''
    for firm in db.matcher.match('Firm'):
        num = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where f.Firm=$firm RETURN count(r)',
                           firm=firm['Firm']).data()[0]['count(r)']

        firm['Repeat_firm_N_EC_cases'] = int(num)
        db.graph.push(firm)


def Recidivist_firm_D():
    '''
        Recidivist_firm_D (dummy 01, če se pojavi samo enkrat v Case-ih, potem, če se pojavi vsaj 2-krat, potem 1)
        Todo: Za ispis (preko tabele)
    '''

    for firm in db.matcher.match('Firm'):
        num = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where f.Firm=$firm RETURN count(r)',
                           firm=firm['Firm']).data()[0]['count(r)']
        firm['Recidivist_firm_D'] = 1 if num >= 2 else 0
        db.graph.push(firm)


def Recidivist_firm_2nd_time_D():
    '''
    Todo: Recidivist_firm_2nd_time_D (dummy 01, ko se firma, ki je recidivist datumsko glede na EC_Date_of_decision prvič pojavi v bazi je tudi 0 (in ne 1 kot pri prejšnjem dummy-ju), ko se pa pojavi časovno gledano drugič, tretjič itd. pa je 1)
    Todo: Za izspis
    '''


def N_Firm_Inc_states_within_EC_case():
    '''
        N_Firm_Inc_states_within_EC_case (število vseh držav znotraj Case)
    '''
    for case in db.matcher.match('Case'):
        firms_r = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                               case=case['Case']).data()

        states = set()
        for firm_r in firms_r:
            states.add(firm_r['f']['Incorporation_state'])

        case['N_Firm_Inc_states_within_EC_case'] = len(states)
        db.graph.push(case)


def European_firm():
    firms = list(db.matcher.match('Firm'))
    for firm in firms:
        country = firm['Incorporation_state']
        cinfo = utils.getCountryInfo(country)
        firm['European_firm'] = 1 if cinfo.get('continent') == 'Europe' else 0
        db.graph.push(firm)


def Extra_Europe_firm():
    extra_eu = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Hong Kong', 'India', 'Israel', 'Malaysia',
        'New Zealand', 'Republic of South Africa', 'Singapore', 'USA', 'Brasil',
        'Chile', 'Kuwait', 'Mexico', 'Tunisia', 'China', 'Japan', 'South Korea', 'Taiwan',
    ]
    firms = list(db.matcher.match('Firm'))
    for firm in firms:
        country = firm['Incorporation_state']

        firm['Extra_Europe_firm'] = 1 if country in extra_eu else 0
        db.graph.push(firm)


def EU_all_time_firm():
    slovarDrzav = {
        'Ireland': '1/1/1973',  # %m/%d/%Y
        'UK': '1/1/1973',
        'Belgium': '7/23/1952',
        'France': '7/23/1952',
        'Greece': '1/1/1981',
        'Italy': '7/23/1952',
        'Lithuania': '5/1/2004',
        'Luxembourg': '7/23/1952',
        'Netherlands': '7/23/1952',
        'Portugal': '1/1/1986',
        'Romania': '1/1/2007',
        'Spain': '1/1/1986',
        'Austria': '1/1/1995',
        'Croatia': '7/1/2013',
        'Czech Republic': '5/1/2004',
        'Estonia': '5/1/2004',
        'Germany': '7/23/1952',
        'Hungary': '5/1/2004',
        'Latvia': '5/1/2004',
        'Poland': '5/1/2004',
        'Slovakia': '5/1/2004',
        'Slovenia': '5/1/2004',
        'Denmark': '1/1/1973',
        'Finland': '1/1/1995',
        'Sweden': '1/1/1995',
    }
    '''
    EU_all_time_firm(dummy 01, če je bila kadarkoli v EU, 1, če ne 0) '''
    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        datumPriklucitve = slovarDrzav.get(country, None)
        firm['EU_all_time_firm'] = 1 if datumPriklucitve is not None else 0
        db.graph.push(firm)


def EU_EC_decision_firm():
    '''Todo:  Izspis'''
    slovarDrzav = {
        'Ireland': '1/1/1973',  # %m/%d/%Y
        'UK': '1/1/1973',
        'Belgium': '7/23/1952',
        'France': '7/23/1952',
        'Greece': '1/1/1981',
        'Italy': '7/23/1952',
        'Lithuania': '5/1/2004',
        'Luxembourg': '7/23/1952',
        'Netherlands': '7/23/1952',
        'Portugal': '1/1/1986',
        'Romania': '1/1/2007',
        'Spain': '1/1/1986',
        'Austria': '1/1/1995',
        'Croatia': '7/1/2013',
        'Czech Republic': '5/1/2004',
        'Estonia': '5/1/2004',
        'Germany': '7/23/1952',
        'Hungary': '5/1/2004',
        'Latvia': '5/1/2004',
        'Poland': '5/1/2004',
        'Slovakia': '5/1/2004',
        'Slovenia': '5/1/2004',
        'Denmark': '1/1/1973',
        'Finland': '1/1/1995',
        'Sweden': '1/1/1995',
    }
    '''
    Todo: ????
    EU_all_time_firm(dummy 01, če je bila kadarkoli v EU, 1, če ne 0) '''
    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        cinfo = utils.getCountryInfo(country)
        if cinfo.get('continent') == 'Europe':
            datumPriklucitve = slovarDrzav.get(country, None)
            if datumPriklucitve is not None:
                cases_r = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where f.Firm=$firm RETURN c',
                                       firm=firm['Firm']).data()

                print(firm['Firm'])
                for case in cases_r:
                    case = case['c']
                    EC_date = utils.parseDate(case['EC_Date_of_decision'])
                    prikl_date = utils.parseDate(datumPriklucitve)

                    if prikl_date < EC_date:
                        print('\t', case['Case'], 1)
                    else:
                        print('\t', case['Case'], 0)
                print('----------------------')


def Old_EU_firm():
    '''
    Old_EU_firm(če gre za ustavno članico EU iz l. 1952, potem 1, drugače 0) '''
    EU_founders = ['Belgium', 'France', 'Germany', 'Italy', 'Luxembourg', 'Netherland']
    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['Old_EU_firm'] = 1 if country in EU_founders else 0
        db.graph.push(firm)


def USA_firm():
    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['USA_firm'] = 1 if country == 'USA' else 0
        db.graph.push(firm)


def Japan_firm():
    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['Japan_firm'] = 1 if country == 'Japan' else 0
        db.graph.push(firm)


def Common_Law_Firm():
    countries_common_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Channel Islands', 'Hong Kong', 'India',
        'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa',
        'Singapore', 'UK', 'USA']

    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['Common_Law_Firm'] = 1 if country in countries_common_law else 0


def English_Law_Firm():
    '''
    English_Law_Firm (dummy 01, če je English 1, če ni 0)
    '''

    english_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada', 'Cayman Islands', 'Channel Islands',
        'Hong Kong', 'India', 'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa', 'Singapore',
        'UK', 'USA']

    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['English_Law_Firm'] = 1 if country in english_law else 0
        db.graph.push(firm)


def French_Law_Firm():
    french_law = [
        'Belgium', 'Brasil', 'Chile', 'France',
        'Greece', 'Italy', 'Kuwait', 'Lithuania',
        'Luxembourg', 'Mexico', 'Netherlands', 'Portugal',
        'Romania', 'Spain', 'Tunisia'
    ]

    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['French_Law_Firm'] = 1 if country in french_law else 0
        db.graph.push(firm)


def German_Law_Firm():
    german_law = [
        'Austria', 'China', 'Croatia', 'Czech Republic', 'Estonia', 'Germany',
        'Hungary', 'Japan', 'Latvia', 'Liechtenstein', 'Poland', 'Slovakia',
        'Slovenia', 'South Korea', 'Switzerland', 'Taiwan'
    ]

    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['German_Law'] = 1 if country in german_law else 0
        db.graph.push(firm)


def Scandinavian_Law_Firm():
    scandinavian_law = [
        'Denmark',
        'Finland',
        'Iceland',
        'Norway',
        'Sweden'
    ]

    for firm in db.matcher.match('Firm'):
        country = firm['Incorporation_state']
        firm['Scandinavian_Law_Firm'] = 1 if country in scandinavian_law else 0
        db.graph.push(firm)


def Transcontinental_case():
    '''
    Transcontinental_case (dummy, če je vsaj ena firma v Case-u izven Europe, potem 1, če je v Europe 0)
    '''
    for case in db.matcher.match('Case'):
        firms_r = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                               case=case['Case']).data()

        isTranscontinental = False
        for firm_r in firms_r:
            country = firm_r['f']['Incorporation_state']
            continent = utils.getCountryInfo(country)['continent']
            if continent != 'Europe':
                isTranscontinental = True
                break

        case['Transcontinental_case'] = 1 if isTranscontinental else 0
        db.graph.push(case)


def Europe_only_case():
    '''
    Europe_only_case (dummy 01, obrnjeno od prenšnjega stolpca, če je v Case-u samo Europe 1, če je Extra_Europe potem 0)
    '''
    for case in db.matcher.match('Case'):
        firms_r = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                               case=case['Case']).data()

        isEuropeOnly = True
        for firm_r in firms_r:
            country = firm_r['f']['Incorporation_state']
            continent = utils.getCountryInfo(country)['continent']
            if continent != 'Europe':
                isEuropeOnly = False
                break

        case['Europe_only_case'] = 1 if isEuropeOnly else 0
        db.graph.push(case)


def National_only_case():
    '''National_only_case (dummy 01, če so vse Firme znotraj Case iz iste države, potem 1, drugače 0)'''
    for case in db.matcher.match('Case'):
        firms_r = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                               case=case['Case']).data()

        theSameCountry = True
        country = None
        for firm_r in firms_r:
            firm_country = firm_r['f']['Incorporation_state']
            if country is None:
                country = firm_country
            elif country != firm_country:
                theSameCountry = False
                break

        case['National_only_case'] = 1 if theSameCountry else 0
        db.graph.push(case)


def N_under_within_EC_case():
    '''
    Todo: Ali dam vsoto firm ki so znotraj katerega koli undertakinga???
    N_firms_within_under (število firm notraj Undertaking-a znotraj Case-a)
    '''

    # for undertaking in list(db.matcher.match('Undertaking')):
    #     num = db.graph.run('MATCH (f:Firm)-[r]->(u:Undertaking) where u.Undertaking=$undertaking RETURN count(r)',
    #                        undertaking=undertaking['Undertaking']).data()[0]['count(r)']
    #     print(num, '\t', undertaking['Undertaking'])


def N_undertaking_within_EC_case():
    '''
    N_firms_within_EC_case (število vseh firm znotraj Case)
    '''
    for case in db.matcher.match('Case'):
        num = db.graph.run('MATCH (Undertaking)-[r]->(c:Case) where c.Case=$case RETURN count(r)',
                           case=case['Case']).data()[0]['count(r)']
        case['N_undertaking_within_EC_case'] = int(num)
        db.graph.push(case)


def Repeat_undertaking_N_EC_cases():
    '''
    Repeat_Firm_N_EC_cases (število Case-ov, v katerih se pojavi to eno in isto podjetje)
    '''
    for undertaking in db.matcher.match('Undertaking'):
        num = db.graph.run('MATCH (u:Undertaking)-[r]->(c:Case) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']

        undertaking['Repeat_undertaking_N_EC_cases'] = int(num)
        db.graph.push(undertaking)


def Recidivist_undertaking_D():
    for undertaking in db.matcher.match('Undertaking'):
        num = db.graph.run('MATCH (u:Undertaking)-[r]->(c:Case) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']
        undertaking['Recidivist_undertaking_D'] = 1 if num >= 2 else 0
        db.graph.push(undertaking)


def Recidivist_undertaking_2nd_time_D():
    '''
    Todo: Recidivist_firm_2nd_time_D (dummy 01, ko se firma, ki je recidivist datumsko glede na EC_Date_of_decision prvič pojavi v bazi je tudi 0 (in ne 1 kot pri prejšnjem dummy-ju), ko se pa pojavi časovno gledano drugič, tretjič itd. pa je 1)
    Todo: Za izspis
    '''


def N_Undertaking_Inc_states_within_EC_case():
    '''
        N_Undertaking_Inc_states_within_EC_case (število vseh držav znotraj Case)
    '''
    for case in db.matcher.match('Case'):
        undertakings_r = db.graph.run('MATCH (f:Undertaking)-[r]->(c:Case) where c.Case=$case RETURN f',
                                      case=case['Case']).data()

        states = set()
        for undertaking_r in undertakings_r:
            states.add(undertaking_r['f']['Incorporation_state'])

        case['N_Undertaking_Inc_states_within_EC_case'] = len(states)
        db.graph.push(case)


def European_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        cinfo = utils.getCountryInfo(country)
        undertaking['European_undertaking'] = 1 if cinfo.get('continent') == 'Europe' else 0
        db.graph.push(undertaking)


def Extra_Europe_undertaking():
    extra_eu = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Hong Kong', 'India', 'Israel', 'Malaysia',
        'New Zealand', 'Republic of South Africa', 'Singapore', 'USA', 'Brasil',
        'Chile', 'Kuwait', 'Mexico', 'Tunisia', 'China', 'Japan', 'South Korea', 'Taiwan',
    ]
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']

        undertaking['Extra_Europe_undertaking'] = 1 if country in extra_eu else 0
        db.graph.push(undertaking)


def EU_all_time_undertaking():
    slovarDrzav = {
        'Ireland': '1/1/1973',  # %m/%d/%Y
        'UK': '1/1/1973',
        'Belgium': '7/23/1952',
        'France': '7/23/1952',
        'Greece': '1/1/1981',
        'Italy': '7/23/1952',
        'Lithuania': '5/1/2004',
        'Luxembourg': '7/23/1952',
        'Netherlands': '7/23/1952',
        'Portugal': '1/1/1986',
        'Romania': '1/1/2007',
        'Spain': '1/1/1986',
        'Austria': '1/1/1995',
        'Croatia': '7/1/2013',
        'Czech Republic': '5/1/2004',
        'Estonia': '5/1/2004',
        'Germany': '7/23/1952',
        'Hungary': '5/1/2004',
        'Latvia': '5/1/2004',
        'Poland': '5/1/2004',
        'Slovakia': '5/1/2004',
        'Slovenia': '5/1/2004',
        'Denmark': '1/1/1973',
        'Finland': '1/1/1995',
        'Sweden': '1/1/1995',
    }
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        datumPriklucitve = slovarDrzav.get(country, None)
        undertaking['EU_all_time_undertaking'] = 1 if datumPriklucitve is not None else 0
        db.graph.push(undertaking)


def EU_EC_decision_undertaking():
    '''Todo:  Izspis'''
    slovarDrzav = {
        'Ireland': '1/1/1973',  # %m/%d/%Y
        'UK': '1/1/1973',
        'Belgium': '7/23/1952',
        'France': '7/23/1952',
        'Greece': '1/1/1981',
        'Italy': '7/23/1952',
        'Lithuania': '5/1/2004',
        'Luxembourg': '7/23/1952',
        'Netherlands': '7/23/1952',
        'Portugal': '1/1/1986',
        'Romania': '1/1/2007',
        'Spain': '1/1/1986',
        'Austria': '1/1/1995',
        'Croatia': '7/1/2013',
        'Czech Republic': '5/1/2004',
        'Estonia': '5/1/2004',
        'Germany': '7/23/1952',
        'Hungary': '5/1/2004',
        'Latvia': '5/1/2004',
        'Poland': '5/1/2004',
        'Slovakia': '5/1/2004',
        'Slovenia': '5/1/2004',
        'Denmark': '1/1/1973',
        'Finland': '1/1/1995',
        'Sweden': '1/1/1995',
    }
    '''
    Todo: ????
    EU_all_time_undertaking(dummy 01, če je bila kadarkoli v EU, 1, če ne 0) '''
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        cinfo = utils.getCountryInfo(country)
        if cinfo.get('continent') == 'Europe':
            datumPriklucitve = slovarDrzav.get(country, None)
            if datumPriklucitve is not None:
                cases_r = db.graph.run('MATCH (f:Undertaking)-[r]->(c:Case) where f.Undertaking=$undertaking RETURN c',
                                       undertaking=undertaking['Undertaking']).data()

                print(undertaking['Undertaking'])
                for case in cases_r:
                    case = case['c']
                    EC_date = utils.parseDate(case['EC_Date_of_decision'])
                    prikl_date = utils.parseDate(datumPriklucitve)

                    if prikl_date < EC_date:
                        print('\t', case['Case'], 1)
                    else:
                        print('\t', case['Case'], 0)
                print('----------------------')


def Old_EU_undertaking():
    '''
    Old_EU_undertaking(če gre za ustavno članico EU iz l. 1952, potem 1, drugače 0) '''
    EU_founders = ['Belgium', 'France', 'Germany', 'Italy', 'Luxembourg', 'Netherland']
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['Old_EU_undertaking'] = 1 if country in EU_founders else 0
        db.graph.push(undertaking)


def USA_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['USA_undertaking'] = 1 if country == 'USA' else 0
        db.graph.push(undertaking)


def Japan_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['Japan_undertaking'] = 1 if country == 'Japan' else 0
        db.graph.push(undertaking)


def Common_Law_undertaking():
    countries_common_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Channel Islands', 'Hong Kong', 'India',
        'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa',
        'Singapore', 'UK', 'USA']

    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['Common_Law_Undertaking'] = 1 if country in countries_common_law else 0


def English_Law_undertaking():
    '''
    English_Law_Undertaking (dummy 01, če je English 1, če ni 0)
    '''

    english_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada', 'Cayman Islands', 'Channel Islands',
        'Hong Kong', 'India', 'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa', 'Singapore',
        'UK', 'USA']

    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['English_Law_Undertaking'] = 1 if country in english_law else 0
        db.graph.push(undertaking)


def French_Law_Undertaking():
    french_law = [
        'Belgium', 'Brasil', 'Chile', 'France',
        'Greece', 'Italy', 'Kuwait', 'Lithuania',
        'Luxembourg', 'Mexico', 'Netherlands', 'Portugal',
        'Romania', 'Spain', 'Tunisia'
    ]

    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['French_Law_Undertaking'] = 1 if country in french_law else 0
        db.graph.push(undertaking)


def German_Law_Undertaking():
    german_law = [
        'Austria', 'China', 'Croatia', 'Czech Republic', 'Estonia', 'Germany',
        'Hungary', 'Japan', 'Latvia', 'Liechtenstein', 'Poland', 'Slovakia',
        'Slovenia', 'South Korea', 'Switzerland', 'Taiwan'
    ]

    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['German_Law'] = 1 if country in german_law else 0
        db.graph.push(undertaking)


def Scandinavian_Law_Undertaking():
    scandinavian_law = [
        'Denmark',
        'Finland',
        'Iceland',
        'Norway',
        'Sweden'
    ]

    for undertaking in db.matcher.match('Undertaking'):
        country = undertaking['Incorporation_state']
        undertaking['Scandinavian_Law_Undertaking'] = 1 if country in scandinavian_law else 0
        db.graph.push(undertaking)


def Holding_Ticker_D_Firm():
    for firm in db.matcher.match('Firm'):
        num = db.graph.run('MATCH (f:Firm)-[r]->(Holding) where f.Firm=$firm RETURN count(r)',
                           firm=firm['Firm']).data()[0]['count(r)']
        firm['Holding_Ticker_D_Firm'] = 1 if int(num) > 0 else 0
        db.graph.push(firm)


def Holding_Ticker_D_Undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        num = db.graph.run('MATCH (u:Undertaking)-[r]->(Holding) where u.Undertaking=$undertaking RETURN count(r)',
                           undertaking=undertaking['Undertaking']).data()[0]['count(r)']
        undertaking['Holding_Ticker_D_Undertaking'] = 1 if int(num) > 0 else 0
        db.graph.push(undertaking)

def Private_firm():
    for firm in db.matcher.match('Firm'):
        Association_firm:bool = utils.exists(firm['Firm_type'])
        Public_firm:bool = utils.exists(firm['Ticker_firm'])
        firm['Private_firm'] = 1 if (not Association_firm and not Public_firm) else 0
        db.graph.push(firm)

def Public_firm():
    for firm in db.matcher.match('Firm'):
        firm['Public_firm'] = 1 if utils.exists(firm['Ticker_firm']) else 0
        db.graph.push(firm)

def Association_firm():
    for firm in db.matcher.match('Firm'):
        firm['Association_firm'] = 1 if utils.exists(firm['Firm_type']) else 0
        db.graph.push(firm)

def Firm_governance():
    for firm in db.matcher.match('Firm'):
        Association_firm:bool = utils.exists(firm['Firm_type'])
        Public_firm:bool = utils.exists(firm['Ticker_firm'])
        Private_firm:bool = not Association_firm and not Public_firm

        fg = 'Private'
        if Association_firm:
            fg = 'Association'
        elif Public_firm:
            fg = 'Public'

        firm['Firm_governance'] = fg
        db.graph.push(firm)
        
def Private_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        Association_undertaking:bool = utils.exists(undertaking['Undertaking_type'])
        Public_undertaking:bool = utils.exists(undertaking['Ticker_undertaking'])
        undertaking['Private_undertaking'] = 1 if (not Association_undertaking and not Public_undertaking) else 0
        db.graph.push(undertaking)

def Public_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        undertaking['Public_undertaking'] = 1 if utils.exists(undertaking['Ticker_undertaking']) else 0
        db.graph.push(undertaking)

def Association_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        rezult = db.graph.run('MATCH (f:Firm) where f.Firm=$firm RETURN f',
                           firm=undertaking['Undertaking']).data()

        if len(rezult) > 0:
            firm = rezult[0]['f']
            # print(firm)
        elif len(rezult) == 0:
            print(undertaking['Undertaking'], rezult)

        # if num > 1:
        #     print(undertaking['Undertaking'], num)
        # undertaking['Association_undertaking'] = 1 if utils.exists(undertaking['Undertaking_type']) else 0
        # db.graph.push(undertaking)
Association_undertaking()
def Undertaking_governance():
    for undertaking in db.matcher.match('Undertaking'):
        Association_undertaking:bool = utils.exists(undertaking['Undertaking_type'])
        Public_undertaking:bool = utils.exists(undertaking['Ticker_undertaking'])
        Private_undertaking:bool = not Association_undertaking and not Public_undertaking

        fg = 'Private'
        if Association_undertaking:
            fg = 'Association'
        elif Public_undertaking:
            fg = 'Public'

        undertaking['Undertaking_governance'] = fg
        db.graph.push(undertaking)

def DUMMY_VARIABLES():
    case_dumies = ['Ticker_Case']
    firm_dumies = ['Ticker_Firm']
    undertaking_dumies = ['Ticker_Undertaking']
    holding_dumies = []

    for case in db.matcher.match('Case'):
        for d in case_dumies:
            case[f'{d}_D'] = 1 if case[d] is not None else 0
        db.graph.push(case)

    for firm in db.matcher.match('Firm'):
        for d in firm_dumies:
            firm[f'{d}_D'] = 1 if firm[d] is not None else 0
        db.graph.push(firm)

    for undertaking in db.matcher.match('Undertaking'):
        for d in undertaking_dumies:
            undertaking[f'{d}_D'] = 1 if undertaking[d] is not None else 0
        db.graph.push(undertaking)

    for holding in db.matcher.match('Holding'):
        for d in holding_dumies:
            print(d, holding[d])
            holding[f'{d}_D'] = 1 if holding[d] is not None else 0
        db.graph.push(holding)
