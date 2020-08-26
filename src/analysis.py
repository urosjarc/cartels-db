from src import db, utils, analysis_utils
from datetime import datetime
import sys

this = sys.modules[__name__]
this.matcher = None


def EC_duration():
    db.core_fields.append('EC_duration')
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

    for row in db.core:
        oldest = datetime.now()
        for date in dates:
            timestamp = utils.parseDate(row[date])
            if timestamp is not None and oldest > timestamp:
                oldest = timestamp

        if oldest is None:
            raise Exception('Not found oldest!')

        dateOfDecision = utils.parseDate(row['EC_Date_of_decision'])

        row['EC_duration'] = (dateOfDecision - oldest).days


def EC_decision_year():
    db.core_fields.append('EC_decision_year')
    '''
    EC_decision_year (vstavi se samo leto izdaje odločbe)
    '''
    for row in db.core:
        row['EC_decision_year'] = int(row['EC_Date_of_decision'].split('/')[-1])


def EC_dec_may_2004():
    db.core_fields.append('EC_dec_may_2004')
    '''
    EC_dec_may_2004 (dummy 01, če je bil datum izdaje pred 1. majem 2004 - 0, če je bil datum po 1)
    '''
    weightDate = utils.parseDate('5/01/2004')
    for row in db.core:
        date = utils.parseDate(row['EC_Date_of_decision'])
        row['EC_dec_may_2004'] = (weightDate - date).days > 0


def EC_dec_EN():
    db.core_fields.append('EC_dec_EN')
    # (dummy, 01, če je 1, če ni 0)
    for row in db.core:
        row['EC_dec_EN'] = utils.exists(row['Case_File'])


def N_firms_within_EC_case():
    db.core_fields.append('N_firms_within_EC_case')
    '''
    N_firms_within_EC_case (število vseh firm znotraj Case)
    '''
    for row in db.core:
        firms = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                firms.add(row2['Firm'])

        row['N_firms_within_EC_case'] = len(firms)


def N_firms_within_under():
    db.core_fields.append('N_firms_within_under')
    '''
    N_firms_within_under (število firm notraj Undertaking-a znotraj Case-a)
    '''
    for row in db.core:
        firms = set()
        for row2 in db.core:
            if row['Case'] == row2['Under']:
                firms.add(row2['Firm'])

        row['N_firms_within_under'] = len(firms)


def Multiple_firm_under():
    db.core_fields.append('Multiple_firm_under')
    '''
        Multiple_firm_under (dummy 01, če je samo ena firma znotraj undertakinga, potem 0, če jih je več 1)
    '''
    for row in db.core:
        row['Multiple_firm_under'] = 1 if row['N_firms_within_under'] > 1 else 0


def Repeat_firm_N_EC_cases():
    db.core_fields.append('Repeat_firm_N_EC_cases')
    '''
    Repeat_Firm_N_EC_cases (število Case-ov, v katerih se pojavi to eno in isto podjetje)
    '''
    for row in db.core:
        cases = set()
        for row2 in db.core:
            if row['Firm'] == row2['Firm']:
                cases.add(row2['Case'])

        row['Repeat_firm_N_EC_cases'] = len(cases)


def Recidivist_firm_D():
    db.core_fields.append('Recidivist_firm_D')
    '''
        Recidivist_firm_D (dummy 01, če se pojavi samo enkrat v Case-ih, potem, če se pojavi vsaj 2-krat, potem 1)
        Todo: Za ispis (preko tabele)
    '''

    for row in db.core:
        row['Recidivist_firm_D'] = 1 if row['Repeat_Firm_N_EC_cases'] >= 2 else 0


def Recidivist_firm_2nd_time_D():
    db.core_fields.append('Recidivist_firm_2nd_time_D')
    '''
    Todo: Recidivist_firm_2nd_time_D (dummy 01, ko se firma, ki je recidivist datumsko glede na EC_Date_of_decision prvič pojavi v bazi je tudi 0 (in ne 1 kot pri prejšnjem dummy-ju), ko se pa pojavi časovno gledano drugič, tretjič itd. pa je 1)
    Todo: Za izspis
    '''
    for row in db.core:
        firms = []
        dates = []
        for row2 in db.core:
            if row['Firm'] == row2['Firm']:
                firms.append(row2)
                dates.append(utils.parseDate(row2['EC_Date_of_decision']))

        najstarejsa = min(dates)
        index_najs = dates.index(najstarejsa)
        for i, firm in enumerate(firms):
            if i == index_najs:
                firm['Recidivist_firm_2nd_time_D'] = 0
            else:
                firm['Recidivist_firm_2nd_time_D'] = 1


def N_Firm_Inc_states_within_EC_case():
    db.core_fields.append('N_Firm_Inc_states_within_EC_case')
    '''
        N_Firm_Inc_states_within_EC_case (število vseh držav znotraj Case)
    '''
    for row in db.core:

        states = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                states.add(row2['Incorporation_state'])

        row['N_Firm_Inc_states_within_EC_case'] = len(states)


def European_firm():
    db.core_fields.append('European_firm')
    for row in db.core:
        country = row['Incorporation_state']
        cinfo = utils.getCountryInfo(country)
        row['European_firm'] = 1 if cinfo.get('continent') == 'Europe' else 0


def Extra_Europe_firm():
    db.core_fields.append('Extra_Europe_firm')
    extra_eu = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Hong Kong', 'India', 'Israel', 'Malaysia',
        'New Zealand', 'Republic of South Africa', 'Singapore', 'USA', 'Brasil',
        'Chile', 'Kuwait', 'Mexico', 'Tunisia', 'China', 'Japan', 'South Korea', 'Taiwan',
    ]
    for row in db.core:
        country = row['Incorporation_state']

        row['Extra_Europe_firm'] = 1 if country in extra_eu else 0


def EU_all_time_firm():
    db.core_fields.append('EU_all_time_firm')
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
    for row in db.core:
        country = row['Incorporation_state']
        datumPriklucitve = slovarDrzav.get(country, None)
        row['EU_all_time_firm'] = 1 if datumPriklucitve is not None else 0


def EU_EC_decision_firm():
    db.core_fields.append('EU_EC_decision_firm')

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
    for row in db.core:
        country = row['Incorporation_state']
        wasInEU = 0

        datumPriklucitve = utils.parseDate(slovarDrzav.get(country, None))
        EC_dod = utils.parseDate(row['EC_Date_of_decision'])
        if datumPriklucitve is not None:
            if datumPriklucitve < EC_dod:
                wasInEU = 1

        row['EU_EC_decision_firm'] = wasInEU


def Old_EU_firm():
    db.core_fields.append('Old_EU_firm')
    '''
    Old_EU_firm(če gre za ustavno članico EU iz l. 1952, potem 1, drugače 0) '''
    EU_founders = ['Belgium', 'France', 'Germany', 'Italy', 'Luxembourg', 'Netherland']
    for row in db.core:
        country = row['Incorporation_state']
        row['Old_EU_firm'] = 1 if country in EU_founders else 0


def USA_firm():
    db.core_fields.append('USA_firm')
    for row in db.core:
        country = row['Incorporation_state']
        row['USA_firm'] = 1 if country == 'USA' else 0


def Japan_firm():
    db.core_fields.append('Japan_firm')
    for core in db.core:
        country = core['Incorporation_state']
        core['Japan_firm'] = 1 if country == 'Japan' else 0


def Common_Law_Firm():
    db.core_fields.append('Common_Law_Firm')
    countries_common_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Channel Islands', 'Hong Kong', 'India',
        'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa',
        'Singapore', 'UK', 'USA']

    for row in db.core:
        country = row['Incorporation_state']
        row['Common_Law_Firm'] = 1 if country in countries_common_law else 0


def English_Law_Firm():
    db.core_fields.append('English_Law_Firm')
    '''
    English_Law_Firm (dummy 01, če je English 1, če ni 0)
    '''

    english_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada', 'Cayman Islands', 'Channel Islands',
        'Hong Kong', 'India', 'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa', 'Singapore',
        'UK', 'USA']

    for row in db.core:
        country = row['Incorporation_state']
        row['English_Law_Firm'] = 1 if country in english_law else 0


def French_Law_Firm():
    db.core_fields.append('French_Law_Firm')
    french_law = [
        'Belgium', 'Brasil', 'Chile', 'France',
        'Greece', 'Italy', 'Kuwait', 'Lithuania',
        'Luxembourg', 'Mexico', 'Netherlands', 'Portugal',
        'Romania', 'Spain', 'Tunisia'
    ]

    for row in db.core:
        country = row['Incorporation_state']
        row['French_Law_Firm'] = 1 if country in french_law else 0


def German_Law_Firm():
    db.core_fields.append('German_Law_Firm')
    german_law = [
        'Austria', 'China', 'Croatia', 'Czech Republic', 'Estonia', 'Germany',
        'Hungary', 'Japan', 'Latvia', 'Liechtenstein', 'Poland', 'Slovakia',
        'Slovenia', 'South Korea', 'Switzerland', 'Taiwan'
    ]

    for row in db.core:
        country = row['Incorporation_state']
        row['German_Law_Firm'] = 1 if country in german_law else 0


def Scandinavian_Law_Firm():
    db.core_fields.append('Scandinavian_Law_Firm')
    scandinavian_law = [
        'Denmark',
        'Finland',
        'Iceland',
        'Norway',
        'Sweden'
    ]

    for row in db.core:
        country = row['Incorporation_state']
        row['Scandinavian_Law_Firm'] = 1 if country in scandinavian_law else 0


def Transcontinental_case():
    db.core_fields.append('Transcontinental_case')
    '''
    Transcontinental_case (dummy, če je vsaj ena firma v Case-u izven Europe, potem 1, če je v Europe 0)
    '''
    for row in db.core:
        isTranscontinental = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                country = row2['Incorporation_state']
                continent = utils.getCountryInfo(country)['continent']
                if continent != 'Europe':
                    isTranscontinental = False

        row['Transcontinental_case'] = 1 if isTranscontinental else 0


def National_only_case():
    db.core_fields.append('National_only_case')
    '''National_only_case (dummy 01, če so vse Firme znotraj Case iz iste države, potem 1, drugače 0)'''
    for row in db.core:
        countries = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                countries.add(row2['Incorporation_state'])

        row['National_only_case'] = 1 if len(countries) == 1 else 0


def N_undertaking_within_EC_case():
    db.core_fields.append('N_undertaking_within_EC_case')
    for row in db.core:
        undertakings = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                undertakings.add(row2['Undertaking'])

        row['N_undertaking_within_EC_case'] = len(undertakings)


def Repeat_undertaking_N_EC_cases():
    db.core_fields.append('Repeat_undertaking_N_EC_cases')
    '''
    Repeat_Firm_N_EC_cases (število Case-ov, v katerih se pojavi to eno in isto podjetje)
    '''
    for row in db.core:
        cases = set()
        for row2 in db.core:
            if row['Undertaking'] == row2['Undertaking']:
                cases.add(row2['Case'])

        row['Repeat_undertaking_N_EC_cases'] = len(cases)


def Recidivist_undertaking_D():
    db.core_fields.append('Recidivist_undertaking_D')
    for row in db.core:
        row['Recidivist_undertaking_D'] = 1 if row['Repeat_undertaking_N_EC_cases'] >= 2 else 0


def Recidivist_undertaking_2nd_time_D():
    db.core_fields.append('Recidivist_undertaking_2nd_time_D')
    for row in db.core:
        undertakings = []
        dates = []
        for row2 in db.core:
            if row['Firm'] == row2['Firm']:
                undertakings.append(row2)
                dates.append(utils.parseDate(row2['EC_Date_of_decision']))

        najstarejsa = min(dates)
        index_najs = dates.index(najstarejsa)
        for i, undertaking in enumerate(undertakings):
            if i == index_najs:
                undertaking['Recidivist_undertaking_2nd_time_D'] = 0
            else:
                undertaking['Recidivist_undertaking_2nd_time_D'] = 1


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
        Association_firm: bool = utils.exists(firm['Firm_type'])
        Public_firm: bool = utils.exists(firm['Ticker_firm'])
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
        Association_firm: bool = utils.exists(firm['Firm_type'])
        Public_firm: bool = utils.exists(firm['Ticker_firm'])
        Private_firm: bool = not Association_firm and not Public_firm

        fg = 'Private'
        if Association_firm:
            fg = 'Association'
        elif Public_firm:
            fg = 'Public'

        firm['Firm_governance'] = fg
        db.graph.push(firm)


def Private_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        firm = db.graph.run('MATCH (f:Firm) where f.Firm=$firm RETURN f',
                            firm=undertaking['Undertaking']).data()[0]['f']

        Association_Undertaking: bool = utils.exists(firm['Firm_type'])
        Public_Undertaking: bool = utils.exists(firm['Ticker_firm'])
        undertaking['Private_undertaking'] = 1 if (not Association_Undertaking and not Public_Undertaking) else 0
        db.graph.push(undertaking)


def Public_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        firm = db.graph.run('MATCH (f:Firm) where f.Firm=$firm RETURN f',
                            firm=undertaking['Undertaking']).data()[0]['f']

        undertaking['Public_undertaking'] = 1 if utils.exists(firm['Ticker_firm']) else 0
        db.graph.push(undertaking)


def Association_undertaking():
    for undertaking in db.matcher.match('Undertaking'):
        firm = db.graph.run('MATCH (f:Firm) where f.Firm=$firm RETURN f',
                            firm=undertaking['Undertaking']).data()[0]['f']

        undertaking['Association_undertaking'] = 1 if utils.exists(firm['Firm_type']) else 0
        db.graph.push(undertaking)


def Undertaking_governance():
    for undertaking in db.matcher.match('Undertaking'):
        firm = db.graph.run('MATCH (f:Firm) where f.Firm=$firm RETURN f',
                            firm=undertaking['Undertaking']).data()[0]['f']

        Association_Undertaking: bool = utils.exists(firm['Firm_type'])
        Public_Undertaking: bool = utils.exists(firm['Ticker_firm'])

        ug = 'Private'
        if Association_Undertaking:
            ug = 'Association'
        elif Public_Undertaking:
            ug = 'Public'

        undertaking['Undertaking_governance'] = ug
        db.graph.push(undertaking)


def Case_A101_only():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        A101_only = True
        for firm in firms:
            if not utils.exists(firm['A_101']):
                A101_only = False

        case['Case_A101_only'] = 1 if A101_only else 0
        db.graph.push(case)


def Case_A102_only():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        A102_only = True
        for firm in firms:
            if not utils.exists(firm['A_102']):
                A102_only = False

        case['Case_A102_only'] = 1 if A102_only else 0
        db.graph.push(case)


def Case_A101_A102_only():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        A101_102_only = True
        for firm in firms:
            if not utils.exists(firm['A101_102']):
                A101_102_only = False

        case['Case_A101_102_only'] = 1 if A101_102_only else 0
        db.graph.push(case)


def Case_a101():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        a101 = False
        for firm in firms:
            if utils.exists(firm['a_101']):
                a101 = True
                break

        case['Case_a101'] = 1 if a101 else 0
        db.graph.push(case)


def Case_a102():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        a102 = False
        for firm in firms:
            if utils.exists(firm['a_102']):
                a102 = True
                break

        case['Case_a102'] = 1 if a102 else 0
        db.graph.push(case)


def Case_cartel_VerR():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        Cartel_VerR = False
        for firm in firms:
            if utils.exists(firm['Cartel_VerR']):
                Cartel_VerR = True
                break

        case['Case_cartel_VerR'] = 1 if Cartel_VerR else 0
        db.graph.push(case)


def Case_Ringleader():
    for case in db.matcher.match('Case'):
        firms = db.graph.run('MATCH (f:Firm)-[r]->(c:Case) where c.Case=$case RETURN f',
                             case=case['Case']).data()
        rl = False
        for firm in firms:
            if utils.exists(firm['Ringleader']):
                rl = True
                break

        case['Case_Ringleader'] = 1 if rl else 0
        db.graph.push(case)


def Investigation_begin():
    for case in db.matcher.match('Case'):
        dates_prop = [
            'Readoption_amendment',
            'Notification',
            'Complaint',
            'Ex_offo',
            'Leniency',
            'Dawn_raid',
            'Statement_of_objections',
        ]

        dates = []
        for dp in dates_prop:
            d = utils.parseDate(case[dp])
            if d is not None:
                dates.append(d)

        if len(dates) > 0:
            case['Investigation_begin'] = min(dates)
            db.graph.push(case)
        else:
            raise Exception('Somthing is wrong!')


def InfringeDurationOverallFirm():
    for firm in db.matcher.match('Firm'):
        if firm['Firm'] != 'Bayer AG':
            continue

        print(firm['Firm'])
        for caseName, firm in db.getFirmsRows(firm['Firm']).items():
            diff = analysis_utils.InfringeDurationOverall(firm)
            print(diff / 365, caseName)


def InfringeDurationOverallUndertaking():
    for undertaking in db.matcher.match('Undertaking'):
        firm = db.graph.run('MATCH (f:Firm) where f.Firm=$name RETURN f',
                            name=undertaking['Undertaking']).data()[0]['f']

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

        undertaking['InfringeDurationOverallUndertaking'] = diff
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


db.save_core()
