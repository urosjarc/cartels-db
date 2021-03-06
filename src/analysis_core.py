from src import db, utils, analysis_utils
from datetime import datetime
import sys


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


def EC_decision_May_2004():
    db.core_fields.append('EC_decision_May_2004')
    may = utils.parseDate('05/01/2004')
    '''
    EC_decision_year (vstavi se samo leto izdaje odločbe)
    '''
    for row in db.core:
        row['EC_decision_May_2004'] = 1 if utils.parseDate(row['EC_Date_of_decision']) > may else 0


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
            if row['Undertaking'] == row2['Undertaking'] and row['Case'] == row2['Case']:
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
        row['Recidivist_firm_D'] = 1 if row['Repeat_firm_N_EC_cases'] >= 2 else 0


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
        isTranscontinental = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                country = row2['Incorporation_state']
                continent = utils.getCountryInfo(country)['continent']
                if continent != 'Europe':
                    isTranscontinental = True

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
            if row['Undertaking'] == row2['Undertaking']:
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
    db.core_fields.append('N_Undertaking_Inc_states_within_EC_case')
    '''
        N_Undertaking_Inc_states_within_EC_case (število vseh držav znotraj Case)
    '''
    for row in db.core:

        states = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                states.add(row2['IncorpStateUnder'])

        row['N_Undertaking_Inc_states_within_EC_case'] = len(states)


def European_undertaking():
    db.core_fields.append('European_undertaking')
    for row in db.core:
        country = row['IncorpStateUnder']
        cinfo = utils.getCountryInfo(country)
        row['European_undertaking'] = 1 if cinfo.get('continent') == 'Europe' else 0


def Extra_Europe_undertaking():
    db.core_fields.append('Extra_Europe_undertaking')
    extra_eu = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Hong Kong', 'India', 'Israel', 'Malaysia',
        'New Zealand', 'Republic of South Africa', 'Singapore', 'USA', 'Brasil',
        'Chile', 'Kuwait', 'Mexico', 'Tunisia', 'China', 'Japan', 'South Korea', 'Taiwan',
    ]
    for row in db.core:
        country = row['IncorpStateUnder']
        row['Extra_Europe_undertaking'] = 1 if country in extra_eu else 0


def EU_all_time_undertaking():
    db.core_fields.append('EU_all_time_undertaking')
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
    for row in db.core:
        country = row['IncorpStateUnder']
        datumPriklucitve = slovarDrzav.get(country, None)
        row['EU_all_time_undertaking'] = 1 if datumPriklucitve is not None else 0


def EU_EC_decision_undertaking():
    db.core_fields.append('EU_EC_decision_undertaking')
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
    for row in db.core:
        country = row['IncorpStateUnder']
        wasInEU = 0

        datumPriklucitve = utils.parseDate(slovarDrzav.get(country, None))
        EC_dod = utils.parseDate(row['EC_Date_of_decision'])
        if datumPriklucitve is not None:
            if datumPriklucitve < EC_dod:
                wasInEU = 1

        row['EU_EC_decision_undertaking'] = wasInEU


def Old_EU_undertaking():
    db.core_fields.append('Old_EU_undertaking')
    '''
    Old_EU_undertaking(če gre za ustavno članico EU iz l. 1952, potem 1, drugače 0) '''
    EU_founders = ['Belgium', 'France', 'Germany', 'Italy', 'Luxembourg', 'Netherland']
    for row in db.core:
        country = row['IncorpStateUnder']
        row['Old_EU_undertaking'] = 1 if country in EU_founders else 0


def USA_undertaking():
    db.core_fields.append('USA_undertaking')
    for row in db.core:
        country = row['IncorpStateUnder']
        row['USA_undertaking'] = 1 if country == 'USA' else 0


def Japan_undertaking():
    db.core_fields.append('Japan_undertaking')
    for row in db.core:
        country = row['IncorpStateUnder']
        row['Japan_undertaking'] = 1 if country == 'Japan' else 0


def Common_Law_Undertaking():
    db.core_fields.append('Common_Law_Undertaking')
    countries_common_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada',
        'Cayman Islands', 'Channel Islands', 'Hong Kong', 'India',
        'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa',
        'Singapore', 'UK', 'USA']

    for row in db.core:
        country = row['IncorpStateUnder']
        row['Common_Law_Undertaking'] = 1 if country in countries_common_law else 0


def English_Law_Undertaking():
    db.core_fields.append('English_Law_Undertaking')
    english_law = [
        'Australia', 'Bermuda', 'British Virgin Islands', 'Canada', 'Cayman Islands', 'Channel Islands',
        'Hong Kong', 'India', 'Ireland', 'Israel', 'Malaysia', 'New Zealand', 'Republic of South Africa', 'Singapore',
        'UK', 'USA']

    for row in db.core:
        country = row['IncorpStateUnder']
        row['English_Law_Undertaking'] = 1 if country in english_law else 0


def French_Law_Undertaking():
    db.core_fields.append('French_Law_Undertaking')
    french_law = [
        'Belgium', 'Brasil', 'Chile', 'France',
        'Greece', 'Italy', 'Kuwait', 'Lithuania',
        'Luxembourg', 'Mexico', 'Netherlands', 'Portugal',
        'Romania', 'Spain', 'Tunisia'
    ]

    for row in db.core:
        country = row['IncorpStateUnder']
        row['French_Law_Undertaking'] = 1 if country in french_law else 0


def German_Law_Undertaking():
    db.core_fields.append('German_Law_Undertaking')
    german_law = [
        'Austria', 'China', 'Croatia', 'Czech Republic', 'Estonia', 'Germany',
        'Hungary', 'Japan', 'Latvia', 'Liechtenstein', 'Poland', 'Slovakia',
        'Slovenia', 'South Korea', 'Switzerland', 'Taiwan'
    ]

    for row in db.core:
        country = row['IncorpStateUnder']
        row['German_Law_Undertaking'] = 1 if country in german_law else 0


def Scandinavian_Law_Undertaking():
    db.core_fields.append('Scandinavian_Law_Undertaking')

    scandinavian_law = [
        'Denmark',
        'Finland',
        'Iceland',
        'Norway',
        'Sweden'
    ]

    for row in db.core:
        country = row['IncorpStateUnder']
        row['Scandinavian_Law_Undertaking'] = 1 if country in scandinavian_law else 0


def Holding_Ticker_D_Firm():
    db.core_fields.append('Holding_Ticker_D_Firm')

    for row in db.core:
        row['Holding_Ticker_D_Firm'] = 1 if utils.exists(row['Holding_Ticker_parent']) else 0


def Holding_Ticker_D_Undertaking():
    db.core_fields.append('Holding_Ticker_D_Undertaking')

    for row in db.core:
        rows = []
        htp = None
        for row2 in db.core:
            if row['Undertaking'] == row2['Undertaking'] and row['Case'] == row2['Case']:
                rows.append(row2)
                if utils.exists(row2['Holding_Ticker_parent']):
                    htp = utils.exists(row2['Holding_Ticker_parent'])

        for r in rows:
            r['Holding_Ticker_D_Undertaking'] = 1 if htp else 0


def Private_firm():
    db.core_fields.append('Private_firm')
    for row in db.core:
        Association_firm: bool = utils.exists(row['Firm_type'])
        Public_firm: bool = utils.exists(row['Ticker_firm'])
        row['Private_firm'] = 1 if (not Association_firm and not Public_firm) else 0


def Public_firm():
    db.core_fields.append('Public_firm')
    for row in db.core:
        row['Public_firm'] = 1 if utils.exists(row['Ticker_firm']) else 0


def Association_firm():
    db.core_fields.append('Association_firm')
    for row in db.core:
        row['Association_firm'] = 1 if utils.exists(row['Firm_type']) else 0


def Firm_governance():
    db.core_fields.append('Firm_governance')
    for row in db.core:
        Association_firm: bool = utils.exists(row['Firm_type'])
        Public_firm: bool = utils.exists(row['Ticker_firm'])

        fg = 'Private'
        if Association_firm:
            fg = 'Association'
        elif Public_firm:
            fg = 'Public'

        row['Firm_governance'] = fg


def Private_undertaking():
    db.core_fields.append('Private_undertaking')
    for row in db.core:
        undertaking = row['Undertaking']
        result1 = None
        result2 = None
        for row2 in db.core:
            if undertaking == row2['Firm']:
                result1 = utils.exists(row2['Ticker_undertaking'])
                result2 = utils.exists(row2['Firm_type'])

        for row2 in db.core:
            if undertaking == row2['Undertaking']:
                row2['Private_undertaking'] = 1 if not result1 and not result2 else 0


def Public_undertaking():
    db.core_fields.append('Public_undertaking')
    for row in db.core:
        undertaking = row['Undertaking']
        result = None
        for row2 in db.core:
            if undertaking == row2['Firm']:
                result = 1 if utils.exists(row2['Ticker_undertaking']) else 0

        for row2 in db.core:
            if undertaking == row2['Undertaking']:
                row2['Public_undertaking'] = result


def Association_undertaking():
    db.core_fields.append('Association_undertaking')
    for row in db.core:
        undertaking = row['Undertaking']
        result = None
        for row2 in db.core:
            if undertaking == row2['Firm']:
                result = 1 if utils.exists(row2['Firm_type']) else 0

        for row2 in db.core:
            if undertaking == row2['Undertaking']:
                row2['Association_undertaking'] = result


def Undertaking_governance():
    db.core_fields.append('Undertaking_governance')
    for row in db.core:
        Association_Undertaking: bool = row['Association_undertaking']
        Public_Undertaking: bool = row['Public_undertaking']

        ug = 'Private'
        if Association_Undertaking:
            ug = 'Association'
        elif Public_Undertaking:
            ug = 'Public'

        row['Undertaking_governance'] = ug


def Case_A101_only():
    db.core_fields.append('Case_A101_only')

    for row in db.core:
        A101_only = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if str(row2['A101']) != '1':
                    A101_only = False
                    break

        row['Case_A101_only'] = 1 if A101_only else 0


def Case_A102_only():
    db.core_fields.append('Case_A102_only')

    for row in db.core:
        A102_only = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if str(row2['A102']) != '1':
                    A102_only = False
                    break

        row['Case_A102_only'] = 1 if A102_only else 0


def Case_A101_102_only():
    db.core_fields.append('Case_A101_102_only')

    for row in db.core:
        A101_102 = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if str(row2['A101_102']) != '1':
                    A101_102 = False
                    break

        row['Case_A101_102_only'] = 1 if A101_102 else 0


def Case_a101():
    db.core_fields.append('Case_a101')
    for row in db.core:
        a101 = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if str(row2['a_101']) == '1':
                    a101 = True
                    break

        row['Case_a101'] = 1 if a101 else 0


def Case_a102():
    db.core_fields.append('Case_a102')
    for row in db.core:
        a102 = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if str(row2['a_102']) == '1':
                    a102 = True
                    break

        row['Case_a102'] = 1 if a102 else 0


def Case_cartel_VerR():
    db.core_fields.append('Case_cartel_VerR')
    for row in db.core:
        Cartel_VerR = False
        Cartel_VerR_empty = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['Cartel_VerR']):
                    Cartel_VerR_empty = False
                    if str(row2['Cartel_VerR']) == '1':
                        Cartel_VerR = True
                        break

        if not Cartel_VerR_empty:
            row['Case_cartel_VerR'] = 1 if Cartel_VerR else 0
        else:
            row['Case_cartel_VerR'] = None


def Case_Ringleader():
    db.core_fields.append('Case_Ringleader')
    for row in db.core:
        rl = False
        rl_empty = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['Ringleader']):
                    rl_empty = False
                    if str(row2['Ringleader']) == '1':
                        rl = True
                        break

        if not rl_empty:
            row['Case_Ringleader'] = 1 if rl else 0
        else:
            row['Case_Ringleader'] = None


def Case_A101_only_undertaking():
    db.core_fields.append('Case_A101_only_undertaking')

    for row in db.core:
        A101_only = True
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if str(row2['A101']) != '1':
                    A101_only = False
                    break

        row['Case_A101_only_undertaking'] = 1 if A101_only else 0


def Case_A102_only_undertaking():
    db.core_fields.append('Case_A102_only_undertaking')

    for row in db.core:
        A102_only = True
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if str(row2['A102']) != '1':
                    A102_only = False
                    break

        row['Case_A102_only_undertaking'] = 1 if A102_only else 0


def Case_A101_102_only_undertaking():
    db.core_fields.append('Case_A101_102_only_undertaking')

    for row in db.core:
        A101_102 = True
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if str(row2['A101_102']) != '1':
                    A101_102 = False
                    break

        row['Case_A101_102_only_undertaking'] = 1 if A101_102 else 0


def Case_a101_undertaking():
    db.core_fields.append('Case_a101_undertaking')
    for row in db.core:
        a101 = False
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if str(row2['a_101']) == '1':
                    a101 = True
                    break

        row['Case_a101_undertaking'] = 1 if a101 else 0


def Case_a102_undertaking():
    db.core_fields.append('Case_a102_undertaking')
    for row in db.core:
        a102 = False
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if str(row2['a_102']) == '1':
                    a102 = True
                    break

        row['Case_a102_undertaking'] = 1 if a102 else 0


def Case_cartel_VerR_undertaking():
    db.core_fields.append('Case_cartel_VerR_undertaking')
    for row in db.core:
        Cartel_VerR = False
        Cartel_VerR_empty = True
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['Cartel_VerR']):
                    Cartel_VerR_empty = False
                    if str(row2['Cartel_VerR']) == '1':
                        Cartel_VerR = True
                        break

        if not Cartel_VerR_empty:
            row['Case_cartel_VerR_undertaking'] = 1 if Cartel_VerR else 0
        else:
            row['Case_cartel_VerR_undertaking'] = None


def Case_Ringleader_undertaking():
    db.core_fields.append('Case_Ringleader_undertaking')
    for row in db.core:
        rl = False
        rl_empty = True
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['Ringleader']):
                    rl_empty = False
                    if str(row2['Ringleader']) == '1':
                        rl = True
                        break

        if not rl_empty:
            row['Case_Ringleader_undertaking'] = 1 if rl else 0
        else:
            row['Case_Ringleader_undertaking'] = None


def Investigation_begin():
    db.core_fields.append('Investigation_begin')
    for row in db.core:

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
            d = utils.parseDate(row[dp])
            if d is not None:
                dates.append(d)

        row['Investigation_begin'] = min(dates)

def Investigation_begin_without_dawn_raid():
    db.core_fields.append('Investigation_begin_without_dawn_raid')
    for row in db.core:

        inv_begin = row['Investigation_begin']
        dawn_raid = utils.parseDate(row['Dawn_raid'])

        if dawn_raid not in [1, None]:
            row['Investigation_begin_without_dawn_raid'] = inv_begin if inv_begin < dawn_raid else None
        else:
            row['Investigation_begin_without_dawn_raid'] = inv_begin


def InfringeDurationOverallFirm():
    db.core_fields.append('InfringeDurationOverallFirm')
    for row in db.core:
        diff, beginMin, endMax = analysis_utils.InfringeDurationOverall(row)
        row['InfringeDurationOverallFirm'] = diff


def InfringeDurationOverallUndertaking():
    db.core_fields.append('InfringeDurationOverallUndertaking')
    for row in db.core:
        beginMins = []
        endMaxs = []
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                diff, beginMin, endMax = analysis_utils.InfringeDurationOverall(row2)

                if beginMin is not None:
                    beginMins.append(beginMin)
                if endMax is not None:
                    endMaxs.append(endMax)

        if len(beginMins) > 0:
            minBegin = min(beginMins)
            maxEnd = max(endMaxs)
            row['InfringeDurationOverallUndertaking'] = (maxEnd - minBegin).days
        else:
            row['InfringeDurationOverallUndertaking'] = None


def Ticker_firm_D():
    db.core_fields.append('Ticker_firm_D')
    for row in db.core:
        row['Ticker_firm_D'] = 1 if utils.exists(row['Ticker_firm']) else 0


def Ticker_case_D():
    db.core_fields.append('Ticker_case_D')
    for row in db.core:
        exists = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                tf = utils.exists(row2['Ticker_firm'])
                tu = utils.exists(row2['Ticker_undertaking'])
                th = utils.exists(row2['Holding_Ticker_parent'])
                if tf or tu or th:
                    exists = True
                    break

        row['Ticker_case_D'] = 1 if exists else 0


def Ticker_undertaking_D():
    db.core_fields.append('Ticker_undertaking_D')
    for row in db.core:
        row['Ticker_undertaking_D'] = 1 if utils.exists(row['Ticker_undertaking']) else 0


def Infringement_begin():
    db.core_fields.append('Infringement_begin')
    for row in db.core:
        dates = []
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                for i in range(1, 13):
                    ib = utils.parseDate(row2[f'InfrBegin{i}'])
                    if ib is not None:
                        dates.append(ib)

        row['Infringement_begin'] = min(dates) if len(dates) > 0 else None


def InfringeDurationOverallCase():
    db.core_fields.append('InfringeDurationOverallCase')
    for row in db.core:
        beginMins = []
        endMaxs = []
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                diff, beginMin, endMax = analysis_utils.InfringeDurationOverall(row2)
                if beginMin is not None:
                    beginMins.append(beginMin)
                if endMax is not None:
                    endMaxs.append(endMax)

        if len(beginMins) > 0:
            minBegin = min(beginMins)
            maxEnd = max(endMaxs)
            row['InfringeDurationOverallCase'] = (maxEnd - minBegin).days
        else:
            row['InfringeDurationOverallCase'] = None


def In_flagrante_investigation_firm():
    db.core_fields.append('In_flagrante_investigation_firm')
    for row in db.core:

        infr = []
        ECdod = utils.parseDate(row['EC_Date_of_decision'])

        for i in range(1, 13):
            dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
            dateEnd = utils.parseDate(row[f'InfrEnd{i}'])
            if dateBegin is not None:
                if dateEnd is None:
                    dateEnd = ECdod
                infr.append((dateBegin, dateEnd))

        if len(infr) == 0:
            row['In_flagrante_investigation_firm'] = None
        else:
            maxDateEnd = max([infr[i][1] for i in range(len(infr))])
            inves_begin = row['Investigation_begin']
            row['In_flagrante_investigation_firm'] = 1 if maxDateEnd >= inves_begin else 0


def In_flagrante_investigation_undertaking():
    db.core_fields.append('In_flagrante_investigation_undertaking')
    for row in db.core:

        infr = []

        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                ECdod = utils.parseDate(row2['EC_Date_of_decision'])
                for i in range(1, 13):
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateBegin is not None:
                        if dateEnd is None:
                            dateEnd = ECdod
                        infr.append((dateBegin, dateEnd))

        if len(infr) == 0:
            row['In_flagrante_investigation_undertaking'] = None
        else:
            maxDateEnd = max([infr[i][1] for i in range(len(infr))])
            inves_begin = row['Investigation_begin']
            row['In_flagrante_investigation_undertaking'] = 1 if maxDateEnd >= inves_begin else 0


def In_flagrante_investigation_case():
    db.core_fields.append('In_flagrante_investigation_case')
    for row in db.core:

        infr = []

        for row2 in db.core:
            if row['Case'] == row2['Case']:
                ECdod = utils.parseDate(row2['EC_Date_of_decision'])
                for i in range(1, 13):
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateBegin is not None:
                        if dateEnd is None:
                            dateEnd = ECdod
                        infr.append((dateBegin, dateEnd))

        if len(infr) == 0:
            row['In_flagrante_investigation_case'] = None
        else:
            maxDateEnd = max([infr[i][1] for i in range(len(infr))])
            inves_begin = row['Investigation_begin']
            row['In_flagrante_investigation_case'] = 1 if maxDateEnd >= inves_begin else 0


def InfringeEndByDecision_firm():
    db.core_fields.append('InfringeEndByDecision_firm')
    for row in db.core:
        isOneBeginAlone = False
        begins = []
        for i in range(1, 13):
            dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
            dateEnd = utils.parseDate(row[f'InfrEnd{i}'])
            if dateBegin is not None:
                begins.append(dateBegin)
                if dateEnd is None:
                    isOneBeginAlone = True
                    break

        if len(begins) > 0:
            row['InfringeEndByDecision_firm'] = 1 if isOneBeginAlone else 0
        else:
            row['InfringeEndByDecision_firm'] = None


def InfringeEndByDecision_undertaking():
    db.core_fields.append('InfringeEndByDecision_undertaking')
    for row in db.core:
        isOneBeginAlone = False
        begins = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                for i in range(1, 13):
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateBegin is not None:
                        begins.append(dateBegin)
                        if dateEnd is None:
                            isOneBeginAlone = True
                            break
                if isOneBeginAlone:
                    break

        if len(begins) > 0:
            row['InfringeEndByDecision_undertaking'] = 1 if isOneBeginAlone else 0
        else:
            row['InfringeEndByDecision_undertaking'] = None


def InfringeEndByDecision_case():
    db.core_fields.append('InfringeEndByDecision_case')
    for row in db.core:
        isOneBeginAlone = False
        begins = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                for i in range(1, 13):
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateBegin is not None:
                        begins.append(dateBegin)
                        if dateEnd is None:
                            isOneBeginAlone = True
                            break
                if isOneBeginAlone:
                    break

        if len(begins) > 0:
            row['InfringeEndByDecision_case'] = 1 if isOneBeginAlone else 0
        else:
            row['InfringeEndByDecision_case'] = None


def InfringeDurationTillInvestigationFirm():
    db.core_fields.append('InfringeDurationTillInvestigationFirm')
    for row in db.core:
        infr = []
        InvBegin = row['Investigation_begin']

        for i in range(1, 13):
            dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
            dateEnd = utils.parseDate(row[f'InfrEnd{i}'])
            if dateBegin is not None:

                if dateEnd is None:
                    dateEnd = InvBegin
                elif InvBegin < dateEnd:
                    dateEnd = InvBegin

                infr.append((dateBegin, dateEnd))

        ends = [infr[i][1] for i in range(len(infr))]
        begins = [infr[i][0] for i in range(len(infr))]

        if len(infr) > 0:
            row['InfringeDurationTillInvestigationFirm'] = (max(ends) - min(begins)).days
        else:
            row['InfringeDurationTillInvestigationFirm'] = None


def InfringeDurationTillInvestigationUndertaking():
    db.core_fields.append('InfringeDurationTillInvestigationUndertaking')
    for row in db.core:

        infr = []

        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                InvBegin = row2['Investigation_begin']

                for i in range(1, 13):
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateBegin is not None:
                        if dateEnd is None:
                            dateEnd = InvBegin
                        elif InvBegin < dateEnd:
                            dateEnd = InvBegin
                        infr.append((dateBegin, dateEnd))

        ends = [infr[i][1] for i in range(len(infr))]
        begins = [infr[i][0] for i in range(len(infr))]

        if len(infr) > 0:
            row['InfringeDurationTillInvestigationUndertaking'] = (max(ends) - min(begins)).days
        else:
            row['InfringeDurationTillInvestigationUndertaking'] = None


def InfringeDurationTillInvestigationCase():
    db.core_fields.append('InfringeDurationTillInvestigationCase')
    for row in db.core:

        infr = []

        for row2 in db.core:
            if row['Case'] == row2['Case']:
                InvBegin = row2['Investigation_begin']

                for i in range(1, 13):
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateBegin is not None:
                        if dateEnd is None:
                            dateEnd = InvBegin
                        elif InvBegin < dateEnd:
                            dateEnd = InvBegin
                        infr.append((dateBegin, dateEnd))

        ends = [infr[i][1] for i in range(len(infr))]
        begins = [infr[i][0] for i in range(len(infr))]

        if len(infr) > 0:
            row['InfringeDurationTillInvestigationCase'] = (max(ends) - min(begins)).days
        else:
            row['InfringeDurationTillInvestigationCase'] = None


def Infringements_per_firm():
    db.core_fields.append('Infringements_per_firm')
    for row in db.core:
        begins = []
        for i in range(1, 13):
            dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
            if dateBegin is not None:
                begins.append(dateBegin)

        if len(begins) > 0:
            row['Infringements_per_firm'] = len(begins)
        else:
            row['Infringements_per_firm'] = 1


def Infringements_per_undertaking():
    db.core_fields.append('Infringements_per_undertaking')
    for row in db.core:
        begins = []
        for i in range(1, 13):
            begins.append(False)
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}']) is not None
                    if dateBegin:
                        begins[-1] = True
                        break

        row['Infringements_per_undertaking'] = begins.count(True) if begins.count(True) > 0 else 1


def Infringements_per_case():
    db.core_fields.append('Infringements_per_case')
    for row in db.core:
        begins = []
        for i in range(1, 13):
            begins.append(False)
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}']) is not None
                    if dateBegin:
                        begins[-1] = True
                        break

        row['Infringements_per_case'] = begins.count(True) if begins.count(True) > 0 else 1


def TwoOrMoreInfringements_per_firm():
    db.core_fields.append('TwoOrMoreInfringements_per_firm')
    for row in db.core:
        row['TwoOrMoreInfringements_per_firm'] = 1 if row['Infringements_per_firm'] >= 2 else 0


def TwoOrMoreInfringements_per_undertaking():
    db.core_fields.append('TwoOrMoreInfringements_per_undertaking')
    for row in db.core:
        row['TwoOrMoreInfringements_per_undertaking'] = 1 if row['Infringements_per_undertaking'] >= 2 else 0


def TwoOrMoreInfringements_per_case():
    db.core_fields.append('TwoOrMoreInfringements_per_case')
    for row in db.core:
        row['TwoOrMoreInfringements_per_case'] = 1 if row['Infringements_per_case'] >= 2 else 0


def MaxIndividualInfringeDurationFirm():
    db.core_fields.append('MaxIndividualInfringeDurationFirm')
    for row in db.core:
        diffs = []
        for i in range(1, 13):
            dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
            dateEnd = utils.parseDate(row[f'InfrEnd{i}'])
            diff = None
            if dateEnd is None:
                dateEnd = utils.parseDate(row['EC_Date_of_decision'])
            if dateBegin is not None:
                diff = (dateEnd - dateBegin).days

            if diff is not None:
                diffs.append(diff)

        row['MaxIndividualInfringeDurationFirm'] = max(diffs) if len(diffs) > 0 else None


def MaxIndividualInfringeDurationUndertaking():
    db.core_fields.append('MaxIndividualInfringeDurationUndertaking')
    for row in db.core:
        diffs = []
        for i in range(1, 13):
            begins = []
            ends = []

            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateEnd is None:
                        dateEnd = utils.parseDate(row2['EC_Date_of_decision'])
                    if dateBegin is not None:
                        begins.append(dateBegin)
                        ends.append(dateEnd)

            if len(begins) > 0:
                diffs.append(max(ends) - min(begins))

        row['MaxIndividualInfringeDurationUndertaking'] = max(diffs).days if len(diffs) > 0 else None


def MaxIndividualInfringeDurationCase():
    db.core_fields.append('MaxIndividualInfringeDurationCase')
    for row in db.core:
        diffs = []
        for i in range(1, 13):
            begins = []
            ends = []

            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    dateEnd = utils.parseDate(row2[f'InfrEnd{i}'])
                    if dateEnd is None:
                        dateEnd = utils.parseDate(row2['EC_Date_of_decision'])
                    if dateBegin is not None:
                        begins.append(dateBegin)
                        ends.append(dateEnd)

            if len(begins) > 0:
                diffs.append(max(ends) - min(begins))

        row['MaxIndividualInfringeDurationCase'] = max(diffs).days if len(diffs) > 0 else None


def InfringeBeginYearFirm():
    db.core_fields.append('InfringeBeginYearFirm')
    for row in db.core:
        begins = []
        for i in range(1, 13):
            dateBegin = utils.parseDate(row[f'InfrBegin{i}'])
            if dateBegin is not None:
                begins.append(dateBegin)

        row['InfringeBeginYearFirm'] = min(begins).year if len(begins) > 0 else None


def InfringeBeginYearUndertaking():
    db.core_fields.append('InfringeBeginYearUndertaking')
    for row in db.core:
        beginsTotal = []
        for i in range(1, 13):
            begins = []

            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    if dateBegin is not None:
                        begins.append(dateBegin)

            if len(begins) > 0:
                beginsTotal.append(min(begins))

        row['InfringeBeginYearUndertaking'] = min(beginsTotal).year if len(beginsTotal) > 0 else None


def InfringeBeginYearCase():
    db.core_fields.append('InfringeBeginYearCase')
    for row in db.core:
        beginsTotal = []
        for i in range(1, 13):
            begins = []

            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    dateBegin = utils.parseDate(row2[f'InfrBegin{i}'])
                    if dateBegin is not None:
                        begins.append(dateBegin)

            if len(begins) > 0:
                beginsTotal.append(min(begins))

        row['InfringeBeginYearCase'] = min(beginsTotal).year if len(beginsTotal) > 0 else None


def Settlement_fine_firm():
    db.core_fields.append('Settlement_fine_firm')
    for row in db.core:
        row['Settlement_fine_firm'] = 1 if utils.exists(row['Settlement_fine_reduction_in_percentage']) else 0


def Settlement_fine_undertaking():
    db.core_fields.append('Settlement_fine_undertaking')
    for row in db.core:
        sfr = False
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row['Settlement_fine_reduction_in_percentage']):
                    sfr = True
                    break

        row['Settlement_fine_undertaking'] = 1 if sfr else 0


def Settlement_fine_case():
    db.core_fields.append('Settlement_fine_case')
    for row in db.core:
        sfr = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row['Settlement_fine_reduction_in_percentage']):
                    sfr = True
                    break

        row['Settlement_fine_case'] = 1 if sfr else 0


def Full_immunity_firm():
    db.core_fields.append('Full_immunity_firm')
    for row in db.core:
        row['Full_immunity_firm'] = 1 if utils.exists(row['Full_immunity']) else 0


def Full_immunity_undertaking():
    db.core_fields.append('Full_immunity_undertaking')
    for row in db.core:
        fi = False
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row['Full_immunity']):
                    fi = True
                    break

        row['Full_immunity_undertaking'] = 1 if fi else 0


def Full_immunity_case():
    db.core_fields.append('Full_immunity_case')
    for row in db.core:
        fi = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['Full_immunity']):
                    fi = True
                    break

        row['Full_immunity_case'] = 1 if fi else 0


def Fine_imposed_D_firm():
    db.core_fields.append('Fine_imposed_D_firm')
    for row in db.core:
        fines = [utils.exists(row['Fine_final_single_firm'])]
        for i in range(1, 8):
            fines.append(utils.exists(row[f'Fine_jointly_severally_{i}']))

        fines = list(filter(lambda x: x != False, fines))

        row['Fine_imposed_D_firm'] = 1 if len(fines) > 0 else 0


def Fine_imposed_D_undertaking():
    db.core_fields.append('Fine_imposed_D_undertaking')
    for row in db.core:
        fines = []
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                fines.append(utils.exists(row2['Fine_final_single_firm']))
                for i in range(1, 8):
                    fines.append(utils.exists(row2[f'Fine_jointly_severally_{i}']))

        fines = list(filter(lambda x: x != False, fines))

        row['Fine_imposed_D_undertaking'] = 1 if len(fines) > 0 else 0


def Fine_imposed_D_case():
    db.core_fields.append('Fine_imposed_D_case')
    for row in db.core:
        fines = []
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                fines.append(utils.exists(row2['Fine_final_single_firm']))
                for i in range(1, 8):
                    fines.append(utils.exists(row2[f'Fine_jointly_severally_{i}']))

        fines = list(filter(lambda x: x != False, fines))

        row['Fine_imposed_D_case'] = 1 if len(fines) > 0 else 0


def Fine_max_firm():
    db.core_fields.append('Fine_max_firm')
    for row in db.core:
        fines = []
        if utils.exists(row['Fine_final_single_firm']):
            fines = [float(row['Fine_final_single_firm'])]
        for i in range(1, 8):
            if utils.exists(row[f'Fine_jointly_severally_{i}']):
                fines.append(float(row[f'Fine_jointly_severally_{i}']))

        row['Fine_max_firm'] = sum(fines)


def Fine_firm():
    db.core_fields.append('Fine_firm')
    for row in db.core:

        ffsf = row['Fine_final_single_firm']
        ffsf = float(ffsf) if utils.exists(ffsf) else 0
        fines = [[ffsf, 1]]
        for i in range(1, 8):
            fjs = row[f'Fine_jointly_severally_{i}']
            fjs = float(fjs) if utils.exists(fjs) else 0
            fines.append([fjs, 0])

        for row2 in db.core:
            if row['Case'] == row2['Case']:
                for i in range(1, 8):
                    fjs = row2[f'Fine_jointly_severally_{i}']
                    fjs = float(fjs) if utils.exists(fjs) else 0
                    if fines[i][0] == fjs:
                        fines[i][1] += 1

        for i in range(0, 8):
            fines[i] = fines[i][0] / fines[i][1]

        row['Fine_firm'] = sum(fines)


def Fine_undertaking():
    db.core_fields.append('Fine_undertaking')
    for row in db.core:
        fines = [[] for _ in range(8)]

        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                ffsf = row2['Fine_final_single_firm']
                ffsf = float(ffsf) if utils.exists(ffsf) else 0
                fines[0].append(ffsf)

                for i in range(1, 8):
                    fjs = row2[f'Fine_jointly_severally_{i}']
                    fjs = float(fjs) if utils.exists(fjs) else 0
                    fines[i].append(fjs)

        for i in range(1, 8):
            fines[i] = list(set(fines[i]))

        row['Fine_undertaking'] = sum([sum(l) for l in fines])


def Fine_case():
    db.core_fields.append('Fine_case')
    for row in db.core:
        fines = [[] for _ in range(8)]

        for row2 in db.core:
            if row['Case'] == row2['Case']:
                ffsf = row2['Fine_final_single_firm']
                ffsf = float(ffsf) if utils.exists(ffsf) else 0
                fines[0].append(ffsf)

                for i in range(1, 8):
                    fjs = row2[f'Fine_jointly_severally_{i}']
                    fjs = float(fjs) if utils.exists(fjs) else 0
                    fines[i].append(fjs)

        for i in range(1, 8):
            fines[i] = list(set(fines[i]))

        row['Fine_case'] = sum([sum(l) for l in fines])


def GC_fine_change_D_firm():
    db.core_fields.append('GC_fine_change_D_firm')
    for row in db.core:

        GCs = [utils.exists(row['GC_case_SF'])]
        fines = [utils.exists(row['Fine_final_single_firm'])]
        GCdd = utils.exists(row['GC_Decision_date'])

        for i in range(1, 8):
            GCs.append(utils.exists(row[f'GC_case_JSF{i}']))
            fines.append(utils.exists(row[f'Fine_jointly_severally_{i}']))

        if GCdd and fines.count(True) > 0:
            row['GC_fine_change_D_firm'] = 1 if GCs.count(True) > 0 else 0
        else:
            row['GC_fine_change_D_firm'] = None


def GC_fine_change_D_undertaking():
    db.core_fields.append('GC_fine_change_D_undertaking')
    for row in db.core:

        GCs = []
        fines = []
        GCdd = []

        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                GCs.append(utils.exists(row2['GC_case_SF']))
                fines.append(utils.exists(row2['Fine_final_single_firm']))
                GCdd.append(utils.exists(row2['GC_Decision_date']))

                for i in range(1, 8):
                    GCs.append(utils.exists(row2[f'GC_case_JSF{i}']))
                    fines.append(utils.exists(row2[f'Fine_jointly_severally_{i}']))

        if GCdd.count(True) > 0 and fines.count(True) > 0:
            row['GC_fine_change_D_undertaking'] = 1 if GCs.count(True) > 0 else 0
        else:
            row['GC_fine_change_D_undertaking'] = None


def GC_fine_change_D_case():
    db.core_fields.append('GC_fine_change_D_case')
    for row in db.core:

        GCs = []
        fines = []
        GCdd = []

        for row2 in db.core:
            if row['Case'] == row2['Case']:
                GCs.append(utils.exists(row2['GC_case_SF']))
                fines.append(utils.exists(row2['Fine_final_single_firm']))
                GCdd.append(utils.exists(row2['GC_Decision_date']))

                for i in range(1, 8):
                    GCs.append(utils.exists(row2[f'GC_case_JSF{i}']))
                    fines.append(utils.exists(row2[f'Fine_jointly_severally_{i}']))

        if GCdd.count(True) > 0 and fines.count(True) > 0:
            row['GC_fine_change_D_case'] = 1 if GCs.count(True) > 0 else 0
        else:
            row['GC_fine_change_D_case'] = None


def ECJ_fine_change_D_firm():
    db.core_fields.append('ECJ_fine_change_D_firm')
    for row in db.core:

        GCs = [utils.exists(row['ECJ_SF_fine'])]
        fines = [utils.exists(row['Fine_final_single_firm'])]
        GCdd = utils.exists(row['ECJ_Decision_date'])

        for i in range(1, 4):
            GCs.append(utils.exists(row[f'ECJ_JSF{i}']))
            fines.append(utils.exists(row[f'Fine_jointly_severally_{i}']))

        if GCdd and fines.count(True) > 0:
            row['ECJ_fine_change_D_firm'] = 1 if GCs.count(True) > 0 else 0
        else:
            row['ECJ_fine_change_D_firm'] = None


def ECJ_fine_change_D_undertaking():
    db.core_fields.append('ECJ_fine_change_D_undertaking')
    for row in db.core:

        GCs = []
        fines = []
        GCdd = []

        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                GCs.append(utils.exists(row2['ECJ_SF_fine']))
                fines.append(utils.exists(row2['Fine_final_single_firm']))
                GCdd.append(utils.exists(row2['ECJ_Decision_date']))

                for i in range(1, 4):
                    GCs.append(utils.exists(row2[f'ECJ_JSF{i}']))

                for i in range(1, 8):
                    fines.append(utils.exists(row2[f'Fine_jointly_severally_{i}']))

        if GCdd.count(True) > 0 and fines.count(True) > 0:
            row['ECJ_fine_change_D_undertaking'] = 1 if GCs.count(True) > 0 else 0
        else:
            row['ECJ_fine_change_D_undertaking'] = None


def ECJ_fine_change_D_case():
    db.core_fields.append('ECJ_fine_change_D_case')
    for row in db.core:

        GCs = []
        fines = []
        GCdd = []

        for row2 in db.core:
            if row['Case'] == row2['Case']:
                GCs.append(utils.exists(row2['ECJ_SF_fine']))
                fines.append(utils.exists(row2['Fine_final_single_firm']))
                GCdd.append(utils.exists(row2['ECJ_Decision_date']))

                for i in range(1, 4):
                    GCs.append(utils.exists(row2[f'ECJ_JSF{i}']))
                for i in range(1, 8):
                    fines.append(utils.exists(row2[f'Fine_jointly_severally_{i}']))

        if GCdd.count(True) > 0 and fines.count(True) > 0:
            row['ECJ_fine_change_D_case'] = 1 if GCs.count(True) > 0 else 0
        else:
            row['ECJ_fine_change_D_case'] = None


def GC_fine_change_firm():
    db.core_fields.append('GC_fine_change_firm')
    for row in db.core:

        Fine_final_single_firm = row['Fine_final_single_firm']
        GC_case_SF = row['GC_case_SF']
        GC_Decision_date = row['GC_Decision_date']
        missing_fines = []

        if not utils.exists(GC_Decision_date):
            row['GC_fine_change_firm'] = None
            continue

        if not utils.exists(GC_case_SF):
            GC_case_SF = Fine_final_single_firm
        elif not utils.exists(Fine_final_single_firm):
            missing_fines.append(float(GC_case_SF))

        fines = []
        if utils.exists(Fine_final_single_firm):
            fines.append([float(Fine_final_single_firm), float(GC_case_SF)])

        for i in range(1, 8):
            Fine_jointly_severally = row[f'Fine_jointly_severally_{i}']
            GC_case_JSF = row[f'GC_case_JSF{i}']

            if not utils.exists(GC_case_JSF) and utils.exists(Fine_jointly_severally):
                GC_case_JSF = Fine_jointly_severally

            if utils.exists(GC_case_JSF) and not utils.exists(Fine_jointly_severally):
                missing_fines.append(float(GC_case_JSF))

            if utils.exists(Fine_jointly_severally):
                fines.append([float(Fine_jointly_severally), float(GC_case_JSF)])

        if len(fines) > 0:
            row['GC_fine_change_firm'] = sum([fine[0] - fine[1] for fine in fines])
        else:
            row['GC_fine_change_firm'] = None

        if len(missing_fines) > 0:
            row['GC_fine_change_firm'] -= sum(missing_fines)


def GC_fine_percent_reduction_firm():
    db.core_fields.append('GC_fine_percent_reduction_firm')
    for row in db.core:
        Fine_max_firm = row['Fine_max_firm']
        GC_fine_change_firm = row['GC_fine_change_firm']

        rezult = None
        if Fine_max_firm is not None and GC_fine_change_firm is not None:
            if Fine_max_firm > 0:
                rezult = GC_fine_change_firm / Fine_max_firm * 100

        row['GC_fine_percent_reduction_firm'] = rezult if utils.exists(row['GC_Decision_date']) else None


def GC_fine_percent_reduction_undertaking():
    db.core_fields.append('GC_fine_percent_reduction_undertaking')
    for row in db.core:
        Fine_undertaking = row['Fine_undertaking']
        GC_fine_change_undertaking = row['GC_fine_change_undertaking']

        rezult = None
        if Fine_undertaking is not None and GC_fine_change_undertaking is not None:
            if Fine_undertaking > 0:
                rezult = GC_fine_change_undertaking / Fine_undertaking * 100

        row['GC_fine_percent_reduction_undertaking'] = rezult if utils.exists(row['GC_Decision_date']) else None


def GC_fine_percent_reduction_case():
    db.core_fields.append('GC_fine_percent_reduction_case')
    for row in db.core:
        Fine_case = row['Fine_case']
        GC_fine_change_case = row['GC_fine_change_case']

        rezult = None
        if Fine_case is not None and GC_fine_change_case is not None:
            if Fine_case > 0:
                rezult = GC_fine_change_case / Fine_case * 100

        row['GC_fine_percent_reduction_case'] = rezult if utils.exists(row['GC_Decision_date']) else None


def GC_fine_relative_percent_reduction_case():
    db.core_fields.append('GC_fine_relative_percent_reduction_case')
    for row in db.core:
        Fine_undertaking = float(row['Fine_undertaking'])

        gcfcc = []
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['GC_Decision_date']) and utils.exists(row2['GC_fine_change_case']):
                    gcfcc.append(float(row2['GC_fine_change_case']))

        rezult = None
        if Fine_undertaking > 0:
            rezult = sum(gcfcc) / Fine_undertaking * 100

        row['GC_fine_relative_percent_reduction_case'] = rezult


def ECJ_fine_change_firm():
    db.core_fields.append('ECJ_fine_change_firm')
    for row in db.core:

        Fine_final_single_firm = row['Fine_final_single_firm']
        ECJ_SF_fine = row['ECJ_SF_fine']
        ECJ_Decision_date = row['ECJ_Decision_date']
        missing_fines = []

        if not utils.exists(ECJ_Decision_date):
            row['ECJ_fine_change_firm'] = None
            continue

        if not utils.exists(ECJ_SF_fine):
            ECJ_SF_fine = Fine_final_single_firm
        elif not utils.exists(Fine_final_single_firm):
            missing_fines.append(float(ECJ_SF_fine))

        fines = []
        if utils.exists(Fine_final_single_firm):
            fines.append([float(Fine_final_single_firm), float(ECJ_SF_fine)])

        for i in range(1, 4):
            Fine_jointly_severally = row[f'Fine_jointly_severally_{i}']
            ECJ_JSF = row[f'ECJ_JSF{i}']

            if not utils.exists(ECJ_JSF) and utils.exists(Fine_jointly_severally):
                ECJ_JSF = Fine_jointly_severally

            if utils.exists(ECJ_JSF) and not utils.exists(Fine_jointly_severally):
                missing_fines.append(float(ECJ_JSF))

            if utils.exists(Fine_jointly_severally):
                fines.append([float(Fine_jointly_severally), float(ECJ_JSF)])

        if len(fines) > 0:
            row['ECJ_fine_change_firm'] = sum([fine[0] - fine[1] for fine in fines])
        else:
            row['ECJ_fine_change_firm'] = None

        if len(missing_fines) > 0:
            row['ECJ_fine_change_firm'] -= sum(missing_fines)


def ECJ_fine_percent_reduction_firm():
    db.core_fields.append('ECJ_fine_percent_reduction_firm')
    for row in db.core:
        Fine_max_firm = row['Fine_max_firm']
        ECJ_fine_change_firm = row['ECJ_fine_change_firm']

        rezult = None
        if Fine_max_firm is not None and ECJ_fine_change_firm is not None:
            Fine_max_firm = float(Fine_max_firm)
            ECJ_fine_change_firm = float(ECJ_fine_change_firm)
            if Fine_max_firm > 0:
                rezult = ECJ_fine_change_firm / Fine_max_firm * 100

        row['ECJ_fine_percent_reduction_firm'] = rezult if utils.exists(row['ECJ_Decision_date']) else None


def ECJ_fine_percent_reduction_undertaking():
    db.core_fields.append('ECJ_fine_percent_reduction_undertaking')
    for row in db.core:
        Fine_undertaking = row['Fine_undertaking']
        ECJ_fine_change_undertaking = row['ECJ_fine_change_undertaking']

        rezult = None
        if Fine_undertaking is not None and ECJ_fine_change_undertaking is not None:
            Fine_undertaking = float(Fine_undertaking)
            ECJ_fine_change_undertaking = float(ECJ_fine_change_undertaking)
            if Fine_undertaking > 0:
                rezult = ECJ_fine_change_undertaking / Fine_undertaking * 100

        row['ECJ_fine_percent_reduction_undertaking'] = rezult if utils.exists(row['ECJ_Decision_date']) else None


def ECJ_fine_percent_reduction_case():
    db.core_fields.append('ECJ_fine_percent_reduction_case')
    for row in db.core:
        Fine_case = row['Fine_case']
        ECJ_fine_change_case = row['ECJ_fine_change_case']

        rezult = None
        if Fine_case is not None and ECJ_fine_change_case is not None:
            Fine_case = float(Fine_case)
            ECJ_fine_change_case = float(ECJ_fine_change_case)
            if Fine_case > 0:
                rezult = ECJ_fine_change_case / Fine_case * 100

        row['ECJ_fine_percent_reduction_case'] = rezult if utils.exists(row['ECJ_Decision_date']) else None


def ECJ_fine_relative_percent_reduction_case():
    db.core_fields.append('ECJ_fine_relative_percent_reduction_case')
    for row in db.core:
        Fine_undertaking = float(row['Fine_undertaking'])

        ecjfcc = []
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Decision_date']) and utils.exists(row2['ECJ_fine_change_case']):
                    ecjfcc.append(float(row2['ECJ_fine_change_case']))

        rezult = None
        if Fine_undertaking > 0:
            rezult = sum(ecjfcc) / Fine_undertaking * 100

        row['ECJ_fine_relative_percent_reduction_case'] = rezult


def GC_fine_change_undertaking():
    db.core_fields.append("GC_fine_change_undertaking")
    for row in db.core:
        diffs = [[] for _ in range(8)]
        missing_fines = [set() for _ in range(8)]

        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:

                Fine_final_single_firm = row2['Fine_final_single_firm']
                GC_case_SF = row2['GC_case_SF']
                GC_Decision_date = row2['GC_Decision_date']

                if not utils.exists(GC_Decision_date):
                    continue

                if not utils.exists(GC_case_SF):
                    GC_case_SF = Fine_final_single_firm
                elif not utils.exists(Fine_final_single_firm):
                    missing_fines[0].add(float(GC_case_SF))

                if utils.exists(Fine_final_single_firm):
                    diffs[0].append(float(Fine_final_single_firm) - float(GC_case_SF))

                for i in range(1, 8):
                    Fine_jointly_severally = row2[f'Fine_jointly_severally_{i}']
                    GC_case_JSF = row2[f'GC_case_JSF{i}']

                    if not utils.exists(GC_case_JSF) and utils.exists(Fine_jointly_severally):
                        GC_case_JSF = Fine_jointly_severally

                    if utils.exists(GC_case_JSF) and not utils.exists(Fine_jointly_severally):
                        missing_fines[i].add(float(GC_case_JSF))

                    if utils.exists(Fine_jointly_severally):
                        diffs[i].append(float(Fine_jointly_severally) - float(GC_case_JSF))

        row['GC_fine_change_undertaking'] = sum(diffs[0]) + sum([max(l) if len(l) > 0 else 0 for l in diffs[1:]])

        if len(missing_fines) > 0:
            row['GC_fine_change_undertaking'] -= sum([sum(l) for l in missing_fines])


def GC_fine_change_case():
    db.core_fields.append("GC_fine_change_case")
    for row in db.core:
        GC_fcfs_u = []
        under = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] not in under:
                under.append(row2['Undertaking'])
                GC_fcu = row2['GC_fine_change_undertaking']
                if utils.exists(GC_fcu):
                    GC_fcfs_u.append(GC_fcu)

        if len(GC_fcfs_u) > 0:
            row['GC_fine_change_case'] = sum(GC_fcfs_u)
        else:
            row['GC_fine_change_case'] = None


def ECJ_fine_change_undertaking():
    db.core_fields.append("ECJ_fine_change_undertaking")
    for row in db.core:
        diffs = [[] for _ in range(4)]
        missing_fines = [set() for _ in range(4)]

        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:

                Fine_final_single_firm = row2['Fine_final_single_firm']
                ecjsffine = row2['ECJ_SF_fine']
                ecjdd = row2['ECJ_Decision_date']

                if not utils.exists(ecjdd):
                    continue

                if not utils.exists(ecjsffine):
                    ecjsffine = Fine_final_single_firm
                elif not utils.exists(Fine_final_single_firm):
                    missing_fines[0].add(float(ecjsffine))

                if utils.exists(Fine_final_single_firm):
                    diffs[0].append(float(Fine_final_single_firm) - float(ecjsffine))

                for i in range(1, 4):
                    Fine_jointly_severally = row2[f'Fine_jointly_severally_{i}']
                    ecjjsf = row2[f'ECJ_JSF{i}']

                    if not utils.exists(ecjjsf) and utils.exists(Fine_jointly_severally):
                        ecjjsf = Fine_jointly_severally

                    if utils.exists(ecjjsf) and not utils.exists(Fine_jointly_severally):
                        missing_fines[i].add(float(ecjjsf))

                    if utils.exists(Fine_jointly_severally):
                        diffs[i].append(float(Fine_jointly_severally) - float(ecjjsf))

        row['ECJ_fine_change_undertaking'] = sum(diffs[0]) + sum([max(l) if len(l) > 0 else 0 for l in diffs[1:]])

        if len(missing_fines) > 0:
            row['ECJ_fine_change_undertaking'] -= sum([sum(l) for l in missing_fines])


def ECJ_fine_change_case():
    db.core_fields.append("ECJ_fine_change_case")
    for row in db.core:
        GC_fcfs_u = []
        under = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] not in under:
                under.append(row2['Undertaking'])
                ECJ_fcf = row2['ECJ_fine_change_undertaking']
                if utils.exists(ECJ_fcf):
                    GC_fcfs_u.append(ECJ_fcf)

        if len(GC_fcfs_u) > 0:
            row['ECJ_fine_change_case'] = sum(GC_fcfs_u)
        else:
            row['ECJ_fine_change_case'] = None


def LeniencyFineReduction_D_firm():
    db.core_fields.append("LeniencyFineReduction_D_firm")
    for row in db.core:
        ls = [utils.exists(row['Leniency__Single_Fine_red_in_percent'])]
        for i in range(1, 8):
            ls.append(utils.exists(row[f'Reduction_{i}']))

        row['LeniencyFineReduction_D_firm'] = 1 if ls.count(True) > 0 else 0


def LeniencyFineReduction_D_undertaking():
    db.core_fields.append("LeniencyFineReduction_D_undertaking")
    for row in db.core:
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                ls.append(utils.exists(row2['Leniency__Single_Fine_red_in_percent']))
                for i in range(1, 8):
                    ls.append(utils.exists(row2[f'Reduction_{i}']))

        row['LeniencyFineReduction_D_undertaking'] = 1 if ls.count(True) > 0 else 0


def LeniencyFineReduction_D_case():
    db.core_fields.append("LeniencyFineReduction_D_case")
    for row in db.core:
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                ls.append(utils.exists(row2['Leniency__Single_Fine_red_in_percent']))
                for i in range(1, 8):
                    ls.append(utils.exists(row2[f'Reduction_{i}']))

        row['LeniencyFineReduction_D_case'] = 1 if ls.count(True) > 0 else 0


def LeniencyPercentMaxRed_firm():
    db.core_fields.append("LeniencyPercentMaxRed_firm")
    for row in db.core:
        ls = []
        ls.append(row['Leniency__Single_Fine_red_in_percent'])
        for i in range(1, 8):
            ls.append(row[f'Reduction_{i}'])

        lsNew = []
        for l in ls:
            if utils.exists(l):
                lsNew.append(float(l.replace('%', '')))

        if len(lsNew) > 0:
            row['LeniencyPercentMaxRed_firm'] = max(lsNew) / 100
        else:
            row['LeniencyPercentMaxRed_firm'] = None


def LeniencyPercentMaxRed_undertaking():
    db.core_fields.append("LeniencyPercentMaxRed_undertaking")
    for row in db.core:
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                ls.append(row2['Leniency__Single_Fine_red_in_percent'])
                for i in range(1, 8):
                    ls.append(row2[f'Reduction_{i}'])

        lsNew = []
        for l in ls:
            if utils.exists(l):
                lsNew.append(float(l.replace('%', '')))

        if len(lsNew) > 0:
            row['LeniencyPercentMaxRed_undertaking'] = max(lsNew) / 100
        else:
            row['LeniencyPercentMaxRed_undertaking'] = None


def LeniencyPercentMaxRed_case():
    db.core_fields.append("LeniencyPercentMaxRed_case")
    for row in db.core:
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                ls.append(row2['Leniency__Single_Fine_red_in_percent'])
                for i in range(1, 8):
                    ls.append(row2[f'Reduction_{i}'])

        lsNew = []
        for l in ls:
            if utils.exists(l):
                lsNew.append(float(l.replace('%', '')))

        if len(lsNew) > 0:
            row['LeniencyPercentMaxRed_case'] = max(lsNew) / 100
        else:
            row['LeniencyPercentMaxRed_case'] = None


def LeniencyPercentMinRed_firm():
    db.core_fields.append("LeniencyPercentMinRed_firm")
    for row in db.core:
        ls = []
        ls.append(row['Leniency__Single_Fine_red_in_percent'])
        for i in range(1, 8):
            ls.append(row[f'Reduction_{i}'])

        lsNew = []
        for l in ls:
            if utils.exists(l):
                lsNew.append(float(l.replace('%', '')))

        if len(lsNew) > 0:
            row['LeniencyPercentMinRed_firm'] = min(lsNew) / 100
        else:
            row['LeniencyPercentMinRed_firm'] = None


def LeniencyPercentMinRed_undertaking():
    db.core_fields.append("LeniencyPercentMinRed_undertaking")
    for row in db.core:
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                ls.append(row2['Leniency__Single_Fine_red_in_percent'])
                for i in range(1, 8):
                    ls.append(row2[f'Reduction_{i}'])

        lsNew = []
        for l in ls:
            if utils.exists(l):
                lsNew.append(float(l.replace('%', '')))

        if len(lsNew) > 0:
            row['LeniencyPercentMinRed_undertaking'] = min(lsNew) / 100
        else:
            row['LeniencyPercentMinRed_undertaking'] = None


def LeniencyPercentMinRed_case():
    db.core_fields.append("LeniencyPercentMinRed_case")
    for row in db.core:
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                ls.append(row2['Leniency__Single_Fine_red_in_percent'])
                for i in range(1, 8):
                    ls.append(row2[f'Reduction_{i}'])

        lsNew = []
        for l in ls:
            if utils.exists(l):
                lsNew.append(float(l.replace('%', '')))

        if len(lsNew) > 0:
            row['LeniencyPercentMinRed_case'] = min(lsNew) / 100
        else:
            row['LeniencyPercentMinRed_case'] = None


def LeniencyPercentAvgRed_firm():
    db.core_fields.append("LeniencyPercentAvgRed_firm")
    for row in db.core:
        fine_max_firm = row['Fine_max_firm']
        ls = []
        ls.append([row['Leniency__Single_Fine_red_in_percent'], row['Fine_final_single_firm']])
        for i in range(1, 8):
            ls.append([row[f'Reduction_{i}'], row[f'Fine_jointly_severally_{i}']])

        lsNewUp = 0
        lsNewDown = 0
        lsCalculate = False
        for l in ls:
            if utils.exists(l[0]) and utils.exists(l[1]) and fine_max_firm > 0:
                lsNewUp += float(l[0].replace('%', '')) * float(l[1])
                lsNewDown += float(l[1])
                lsCalculate = True

        if lsCalculate:
            row['LeniencyPercentAvgRed_firm'] = lsNewUp / lsNewDown
        else:
            row['LeniencyPercentAvgRed_firm'] = None


def LeniencyPercentAvgRed_undertaking():
    db.core_fields.append("LeniencyPercentAvgRed_undertaking")
    for row in db.core:
        fine_max_firm = row['Fine_max_firm']
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                ls.append([row2['Leniency__Single_Fine_red_in_percent'], row2['Fine_final_single_firm']])
                for i in range(1, 8):
                    ls.append([row2[f'Reduction_{i}'], row2[f'Fine_jointly_severally_{i}']])

        lsNewUp = 0
        lsNewDown = 0
        lsCalculate = False
        for l in ls:
            if utils.exists(l[0]) and utils.exists(l[1]) and fine_max_firm > 0:
                lsNewUp += float(l[0].replace('%', '')) * float(l[1])
                lsNewDown += float(l[1])
                lsCalculate = True

        if lsCalculate:
            row['LeniencyPercentAvgRed_undertaking'] = lsNewUp / lsNewDown
        else:
            row['LeniencyPercentAvgRed_undertaking'] = None


def LeniencyPercentAvgRed_case():
    db.core_fields.append("LeniencyPercentAvgRed_case")
    for row in db.core:
        fine_max_firm = row['Fine_max_firm']
        ls = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                ls.append([row2['Leniency__Single_Fine_red_in_percent'], row2['Fine_final_single_firm']])
                for i in range(1, 8):
                    ls.append([row2[f'Reduction_{i}'], row2[f'Fine_jointly_severally_{i}']])

        lsNewUp = 0
        lsNewDown = 0
        lsCalculate = False
        for l in ls:
            if utils.exists(l[0]) and utils.exists(l[1]) and fine_max_firm > 0:
                lsNewUp += float(l[0].replace('%', '')) * float(l[1])
                lsNewDown += float(l[1])
                lsCalculate = True

        if lsCalculate and lsNewDown > 0:
            row['LeniencyPercentAvgRed_case'] = lsNewUp / lsNewDown
        else:
            row['LeniencyPercentAvgRed_case'] = None


def DUMIES_10():
    dumies = [
        "Structural_remedy",
        "Behavioral_remedy",
        "Concrete_Behavioral_Remedy",
        "Readoption_amendment",
        "Ex_offo",
        "Notification",
        "Notification_additional_to_complaint",
        "Complaint",
        "Complaint_post_initiation",
        "Leniency",
        "Dawn_raid",
        "Statement_of_objections",
    ]

    for i, d in enumerate(dumies):
        var = None
        if i <= 2:
            var = f'{d}_D_firm'
        else:
            var = f'{d}_D'

        db.core_fields.append(var)
        for row in db.core:
            row[var] = 1 if utils.exists(row[d]) else 0

    # Prvi tri dumiyi za undertaking
    for i in range(3):
        d = dumies[i]
        var = f'{d}_D_undertaking'
        db.core_fields.append(var)

        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    exists.append(utils.exists(row[d]))

            row[var] = 1 if len(exists) > 0 else 0

    # Prvi tri dumiyi za case
    for i in range(3):
        d = dumies[i]
        var = f'{d}_D_case'
        db.core_fields.append(var)

        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    exists.append(utils.exists(row[d]))

            row[var] = 1 if len(exists) > 0 else 0


def Investigation_begin_year():
    db.core_fields.append('Investigation_begin_year')
    for row in db.core:
        row['Investigation_begin_year'] = row['Investigation_begin'].year

def Dawn_raid_year():
    db.core_fields.append('Dawn_raid_year')
    for row in db.core:
        row['Dawn_raid_year'] = row['Dawn_raid'].split('/')[-1]

def Type_of_investigation_begin():
    db.core_fields.append('Type_of_investigation_begin')
    for row in db.core:
        dic = {
            "Readoption_amendment": None,
            "Notification": None,
            "Complaint": None,
            "Ex_offo": None,
            "Leniency": None
        }

        for k, v in dic.items():
            if utils.exists(row[k]):
                dic[k] = True
                row['Type_of_investigation_begin'] = k
            else:
                dic[k] = False


def RecitalsEC_per_firm():
    db.core_fields.append('RecitalsEC_per_firm')
    for row in db.core:
        reci = 0
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['Recitals']):
                    reci = float(row2['Recitals'])
                    break

        row['RecitalsEC_per_firm'] = reci / row['N_firms_within_EC_case']


def RecitalsEC_per_undertaking():
    db.core_fields.append('RecitalsEC_per_undertaking')
    for row in db.core:
        reci = 0
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['Recitals']):
                    reci = float(row2['Recitals'])
                    break

        row['RecitalsEC_per_undertaking'] = reci / row['N_undertaking_within_EC_case']


def Articles_of_remedy_per_firm():
    db.core_fields.append('Articles_of_remedy_per_firm')
    for row in db.core:
        art = int(row['Articles_of_remedy'])
        row['Articles_of_remedy_per_firm'] = art / row['N_firms_within_EC_case']


def Articles_of_remedy_per_undertaking():
    db.core_fields.append('Articles_of_remedy_per_undertaking')
    for row in db.core:
        art = int(row['Articles_of_remedy'])
        row['Articles_of_remedy_per_undertaking'] = art / row['N_undertaking_within_EC_case']


def Market_of_concern_COLUMS():
    cases = {
        'worldwide': "Worldwide_market",
        'national': 'National_market',
        'EU': 'EU_market'
    }
    for c in cases.values():
        db.core_fields.append(c)

    for row in db.core:
        moc = row['Market_of_concern']
        for k, v in cases.items():
            row[v] = 1 if moc == k else 0


def Settlement_Whistleblower_Leniency_application_Dawn_raid_F_specific():
    ds = [
        'Settlement',
        'Whistleblower',
        'Leniency_application',
        'Dawn_raid_F_specific',
    ]

    for d in ds:
        var = f'{d}_D_firm'
        db.core_fields.append(var)
        for row in db.core:
            row[var] = 1 if utils.exists(row[d]) else 0

        var = f'{d}_D_undertaking'
        db.core_fields.append(var)
        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    exists.append(utils.exists(row2[d]))

            row[var] = 1 if exists.count(True) > 0 else 0

        var = f'{d}_D_case'
        db.core_fields.append(var)
        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    exists.append(utils.exists(row2[d]))

            row[var] = 1 if exists.count(True) > 0 else 0


def Dawn_raid_USA_Japan_D():
    db.core_fields.append('Dawn_raid_USA_Japan_D')
    for row in db.core:
        row['Dawn_raid_USA_Japan_D'] = 1 if utils.exists(row['Dawn_raid_USA_Japan']) else 0


def Extra_EU_ongoing_invest_D():
    db.core_fields.append('Extra_EU_ongoing_invest_D')
    for row in db.core:
        row['Extra_EU_ongoing_invest_D'] = 1 if utils.exists(row['Extra_EU_ongoing_invest']) else 0

def GC_decision_year():
    db.core_fields.append('GC_decision_year')
    for row in db.core:
        row['GC_decision_year'] = row['GC_Decision_date'].split('/')[-1]

def GC_decision_year_minANDmax_case():
    db.core_fields.append('GC_decision_year_min_case')
    db.core_fields.append('GC_decision_year_max_case')
    for row in db.core:
        years = []
        # Get all years with the same case
        for row1 in db.core:
            if row['Case'] == row1['Case']:
                year = row1['GC_decision_year']
                if year is not None and year.isnumeric():
                    years.append(int(year))

        row['GC_decision_year_min_case'] = min(years) if len(years) > 0 else None
        row['GC_decision_year_max_case'] = max(years) if len(years) > 0 else None

def GC_decision_year_undertaking():
    db.core_fields.append('GC_decision_year_undertaking')
    for row in db.core:
        years = []
        for row1 in db.core:
            if row['Case'] == row1['Case'] and row['Undertaking'] == row1['Undertaking']:
                year = row1['GC_decision_year']
                if year is not None and year.isnumeric():
                    years.append(int(year))

        row['GC_decision_year_undertaking'] = max(years) if len(years) > 0 else None

def GC_decision_year_holding():
    db.core_fields.append('GC_decision_year_holding')
    for row in db.core:
        years = []
        for row1 in db.core:
            if row['Case'] == row1['Case'] and row['Undertaking'] == row1['Undertaking'] and row['Holding'] == row1['Holding']:
                year = row1['GC_decision_year']
                if year is not None and year.isnumeric():
                    years.append(int(year))

        row['GC_decision_year_holding'] = max(years) if len(years) > 0 else None



def GC_columns():
    cs = [
        'GC_File',
        'GC_File_summary',
        'GC_File_French',
        'GC_File_Italian',
        'GC_File_German',
        'GC_File_Spanish',
        'GC_File_Dutch',
        'GC_New_party',
        'GC_Case_number',
        'GC_Decision_date',
        'GC_Judgement_order',
        'GC_Chamber_of_3',
        'GC_Chamber_of_5',
        'GC_Grand_Chamber',
        'GC_Dismissing_action__entirely',
        'GC_Manifestly_inadmissible',
        'GC_Inadmissible',
        'GC_Total_action_success',
        'GC_No_need_to_adjudicate',
        'GC_Total_annulment_of_EC_decision',
        'GC_Partial_annulment_of_EC_decision',
        'GC_Fine_partial_change_of_EC_decision',
        'GC_Change_of_other_remedies_of_EC_decision',
        'GC_judgement_Summary'
    ]

    for c in cs:
        var = f'{c}_D_firm'
        db.core_fields.append(var)

        for row in db.core:
            if utils.exists(row['GC_Decision_date']):
                row[var] = 1 if utils.exists(row[c]) else 0

        var = f'{c}_D_undertaking'
        db.core_fields.append(var)
        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    exists.append(utils.exists(row2[c]))

            if utils.exists(row['GC_Decision_date']):
                row[var] = 1 if exists.count(True) > 0 else 0

        var = f'{c}_D_case'
        db.core_fields.append(var)
        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    exists.append(utils.exists(row2[c]))

            if utils.exists(row['GC_Decision_date']):
                row[var] = 1 if exists.count(True) > 0 else 0

        var = f'{c}_only_D_undertaking'
        db.core_fields.append(var)
        unique = set()
        for row in db.core:
            allExists = True
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    if utils.exists(row2['GC_Decision_date']):
                        if not utils.exists(row2[c]):
                            allExists = False
                        unique.add(row2[c])

            if c in ['GC_Case_number', 'GC_Decision_date']:
                row[var] = 1 if len(unique) == 1 else 0
            else:
                if utils.exists(row['GC_Decision_date']):
                    row[var] = 1 if allExists else 0

        var = f'{c}_only_D_case'
        db.core_fields.append(var)
        unique = set()
        for row in db.core:
            allExists = True
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    if utils.exists(row2['GC_Decision_date']):
                        if not utils.exists(row2[c]):
                            allExists = False
                        unique.add(row2[c])

            if c in ['GC_Case_number', 'GC_Decision_date']:
                row[var] = 1 if len(unique) == 1 else 0
            else:
                if utils.exists(row['GC_Decision_date']):
                    row[var] = 1 if allExists else 0

def ECJ_decision_year():
    db.core_fields.append('ECJ_decision_year')
    for row in db.core:
        row['ECJ_decision_year'] = row['ECJ_Decision_date'].split('/')[-1]

def ECJ_decision_year_minANDmax_case():
    db.core_fields.append('ECJ_decision_year_min_case')
    db.core_fields.append('ECJ_decision_year_max_case')
    for row in db.core:
        years = []
        # Get all years with the same case
        for row1 in db.core:
            if row['Case'] == row1['Case']:
                year = row1['ECJ_decision_year']
                if year is not None and year.isnumeric():
                    years.append(int(year))

        row['ECJ_decision_year_min_case'] = min(years) if len(years) > 0 else None
        row['ECJ_decision_year_max_case'] = max(years) if len(years) > 0 else None

def ECJ_decision_year_undertaking():
    db.core_fields.append('ECJ_decision_year_undertaking')
    for row in db.core:
        years = []
        for row1 in db.core:
            if row['Case'] == row1['Case'] and row['Undertaking'] == row1['Undertaking']:
                year = row1['ECJ_decision_year']
                if year is not None and year.isnumeric():
                    years.append(int(year))

        row['ECJ_decision_year_undertaking'] = max(years) if len(years) > 0 else None

def ECJ_decision_year_holding():
    db.core_fields.append('ECJ_decision_year_holding')
    for row in db.core:
        years = []
        for row1 in db.core:
            if row['Case'] == row1['Case'] and row['Undertaking'] == row1['Undertaking'] and row['Holding'] == row1['Holding']:
                year = row1['ECJ_decision_year']
                if year is not None and year.isnumeric():
                    years.append(int(year))

        row['ECJ_decision_year_holding'] = max(years) if len(years) > 0 else None

def ECJ_columns():
    cs = [
        'ECJ_File',
        'ECJ_File_summary',
        'ECJ_File_French',
        'ECJ_File_Italian',
        'ECJ_File_German',
        'ECJ_File_Spanish',
        'ECJ_New_party',
        'ECJ_Commission_appeal',
        'ECJ_Cross_appeal_of_Commission',
        'ECJ_Case_number',
        'ECJ_Decision_date',
        'ECJ_Judgement_order',
        'ECJ_Chamber_of_3',
        'ECJ_Chamber_of_5',
        'ECJ_Grand_Chamber',
        'AG_opinion',
        'ECJ_Dissmissing_appeal',
        'ECJ_Comm_T_Dissmiss',
        'ECJ_Comm_P_Dissmiss',
        'ECJ_Comm_T_Grant',
        'ECJ_total_referral',
        'ECJ_Partial_referral',
        'ECJ_Total_change_of_GC_judgement',
        'ECJ_Total_party_appeal_success',
        'ECJ_Partial_change_of_EC_decision',
        'ECJ_Total_confirmation_of_EC_decision_dissmisal_of_action_on_1st_instance',
        'ECJ_Total_annulment_of_EC_decision',
        'ECJ_Partial_annulment_of_EC_decision',
        'ECJ_Fine_partial_change_of_EC_decision',
        'ECJ_Change_of_other_remedies_of_EC_decision',
        'ECJ_judgement_Summary',
    ]

    for c in cs:
        var = f'{c}_D_firm'
        db.core_fields.append(var)

        for row in db.core:
            if utils.exists(row['ECJ_Decision_date']):
                row[var] = 1 if utils.exists(row[c]) else 0

        var = f'{c}_D_undertaking'
        db.core_fields.append(var)
        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    exists.append(utils.exists(row2[c]))

            if utils.exists(row['ECJ_Decision_date']):
                row[var] = 1 if exists.count(True) > 0 else 0

        var = f'{c}_D_case'
        db.core_fields.append(var)
        for row in db.core:
            exists = []
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    exists.append(utils.exists(row2[c]))

            if utils.exists(row['ECJ_Decision_date']):
                row[var] = 1 if exists.count(True) > 0 else 0

        var = f'{c}_only_D_undertaking'
        db.core_fields.append(var)
        unique = set()
        for row in db.core:
            allExists = True
            for row2 in db.core:
                if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                    if utils.exists(row2['ECJ_Decision_date']):
                        if not utils.exists(row2[c]):
                            allExists = False
                        unique.add(row2[c])

            if c in ['ECJ_Case_number', 'ECJ_Decision_date']:
                row[var] = 1 if len(unique) == 1 else 0
            else:
                if utils.exists(row['ECJ_Decision_date']):
                    row[var] = 1 if allExists else 0

        var = f'{c}_only_D_case'
        db.core_fields.append(var)
        unique = set()
        for row in db.core:
            allExists = True
            for row2 in db.core:
                if row['Case'] == row2['Case']:
                    if utils.exists(row2['ECJ_Decision_date']):
                        if not utils.exists(row2[c]):
                            allExists = False
                        unique.add(row2[c])

            if c in ['ECJ_Case_number', 'ECJ_Decision_date']:
                row[var] = 1 if len(unique) == 1 else 0
            else:
                if utils.exists(row['ECJ_Decision_date']):
                    row[var] = 1 if allExists else 0


def GC_N_judgements_on_EC_case_per_undertaking():
    db.core_fields.append('GC_N_judgements_on_EC_case_per_undertaking')
    for row in db.core:

        GC_cn = set()
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['GC_Decision_date']):
                    GC_cn.add(row2['GC_Case_number'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_N_judgements_on_EC_case_per_undertaking'] = len(GC_cn)


def GC_N_judgements_on_EC_case():
    db.core_fields.append('GC_N_judgements_on_EC_case')
    for row in db.core:

        GC_cn = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['GC_Decision_date']):
                    GC_cn.add(row2['GC_Case_number'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_N_judgements_on_EC_case'] = len(GC_cn)


def GC_Filing_action_D_firm():
    db.core_fields.append('GC_Filing_action_D_firm')
    for row in db.core:
        row['GC_Filing_action_D_firm'] = 1 if utils.exists(row['GC_Filing_action']) else 0


def GC_Filing_action_D_undertaking():
    db.core_fields.append('GC_Filing_action_D_undertaking')
    for row in db.core:

        ex = False
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['GC_Filing_action']):
                    ex = True
                    break

        row['GC_Filing_action_D_undertaking'] = 1 if ex else 0


def GC_Filing_action_D_case():
    db.core_fields.append('GC_Filing_action_D_case')
    for row in db.core:

        ex = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['GC_Filing_action']):
                    ex = True
                    break

        row['GC_Filing_action_D_case'] = 1 if ex else 0


def GC_N_firm_plaintiffs_EC_case():
    db.core_fields.append('GC_N_firm_plaintiffs_EC_case')
    for row in db.core:
        n = 0
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['GC_Filing_action']):
                    n += 1

        row['GC_N_firm_plaintiffs_EC_case'] = n


def GC_N_undertaking_plaintiffs_EC_case():
    db.core_fields.append('GC_N_undertaking_plaintiffs_EC_case')
    for row in db.core:
        u = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['GC_Filing_action']):
                    u.add(row2['Undertaking'])

        row['GC_N_undertaking_plaintiffs_EC_case'] = len(u)


def GC_EC_ratio_for_N_firm_plaintiffs_EC_case():
    db.core_fields.append('GC_EC_ratio_for_N_firm_plaintiffs_EC_case')
    for row in db.core:
        row['GC_EC_ratio_for_N_firm_plaintiffs_EC_case'] = row['GC_N_firm_plaintiffs_EC_case'] / row[
            'N_firms_within_EC_case']


def GC_EC_ratio_for_N_undertaking_plaintifs_EC_case():
    db.core_fields.append('GC_EC_ratio_for_N_undertaking_plaintiffs_EC_case')
    for row in db.core:
        row['GC_EC_ratio_for_N_undertaking_plaintiffs_EC_case'] = row['GC_N_undertaking_plaintiffs_EC_case'] / row[
            'N_undertaking_within_EC_case']


def GC_judgement_N_plaintiffs_firm():
    db.core_fields.append('GC_judgement_N_plaintiffs_firm')
    for row in db.core:
        cn = row['GC_Case_number']
        n = 0
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['GC_Case_number'] == cn:
                    n += 1

        if utils.exists(row['GC_Decision_date']):
            row['GC_judgement_N_plaintiffs_firm'] = n
        else:
            row['GC_judgement_N_plaintiffs_firm'] = None


def GC_judgement_N_plaintiffs_undertaking():
    db.core_fields.append('GC_judgement_N_plaintiffs_undertaking')
    for row in db.core:
        cn = row['GC_Case_number']
        undertakings = set()
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['GC_Case_number'] == cn:
                    undertakings.add(row2['Undertaking'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_judgement_N_plaintiffs_undertaking'] = len(undertakings)
        else:
            row['GC_judgement_N_plaintiffs_undertaking'] = None


def GC_duration_firm():
    db.core_fields.append('GC_duration_firm')
    for row in db.core:
        if utils.exists(row['GC_Decision_date']):
            row['GC_duration_firm'] = (
                    utils.parseDate(row['GC_Decision_date']) - utils.parseDate(row['GC_Filing_action'])).days
        else:
            row['GC_duration_firm'] = None


def GC_duration_undertaking():
    db.core_fields.append('GC_duration_undertaking')
    for row in db.core:
        dd = []
        fa = []

        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['GC_Decision_date']) and utils.exists(row2['GC_Filing_action']):
                    dd.append(utils.parseDate(row2['GC_Decision_date']))
                    fa.append(utils.parseDate(row2['GC_Filing_action']))

        if utils.exists(row['GC_Decision_date']):
            row['GC_duration_undertaking'] = (max(dd) - min(fa)).days
        else:
            row['GC_duration_undertaking'] = None


def GC_duration_case():
    db.core_fields.append('GC_duration_case')
    for row in db.core:
        dd = []
        fa = []

        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['GC_Decision_date']) and utils.exists(row2['GC_Filing_action']):
                    dd.append(utils.parseDate(row2['GC_Decision_date']))
                    fa.append(utils.parseDate(row2['GC_Filing_action']))

        if utils.exists(row['GC_Decision_date']):
            row['GC_duration_case'] = (max(dd) - min(fa)).days
        else:
            row['GC_duration_case'] = None


def GC_Pending():
    db.core_fields.append('GC_Pending')
    for row in db.core:
        if utils.exists(row['GC_Case_number']):
            row['GC_Pending'] = 1 if not utils.exists(row['GC_Decision_date']) else 0
        else:
            row['GC_Pending'] = None


def GC_Pending_undertaking():
    db.core_fields.append('GC_Pending_undertaking')
    for row in db.core:
        flag = False
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                if utils.exists(row2['GC_Case_number']) and not utils.exists(row2['GC_Decision_date']):
                    flag = True
                    break

        row['GC_Pending_undertaking'] = 1 if flag else 0


def GC_Pending_case():
    db.core_fields.append('GC_Pending_case')
    for row in db.core:
        flag = False
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['GC_Case_number']) and not utils.exists(row2['GC_Decision_date']):
                    flag = True
                    break

        row['GC_Pending_case'] = 1 if flag else 0


def GC_total_loss_firm():
    db.core_fields.append('GC_total_loss_firm')
    for row in db.core:
        s = [
            'GC_Dismissing_action__entirely',
            'GC_Inadmissible',
            'GC_Manifestly_inadmissible',
            'GC_No_need_to_adjudicate'
        ]

        fb = []
        for e in s:
            fb.append(row[e])

        if utils.exists(row['GC_Decision_date']):
            row['GC_total_loss_firm'] = 1 if fb.count('1') > 0 else 0
        else:
            row['GC_total_loss_firm'] = None


def GC_total_success_firm():
    db.core_fields.append('GC_total_success_firm')
    for row in db.core:
        s = [
            'GC_Total_annulment_of_EC_decision',
            'GC_Total_action_success'
        ]

        fb = []
        for e in s:
            fb.append(row[e])

        if utils.exists(row['GC_Decision_date']):
            row['GC_total_success_firm'] = 1 if fb.count('1') > 0 else 0
        else:
            row['GC_total_success_firm'] = None


def GC_partial_success_firm():
    db.core_fields.append('GC_partial_success_firm')
    for row in db.core:
        s = [
            'GC_Partial_annulment_of_EC_decision',
            'GC_Change_of_other_remedies_of_EC_decision',
            'GC_Fine_partial_change_of_EC_decision',
            'GC_Change_of_other_remedies_of_EC_decision',
        ]

        fb = []
        for e in s:
            fb.append(row[e])

        if utils.exists(row['GC_Decision_date']):
            row['GC_partial_success_firm'] = 1 if fb.count('1') > 0 else 0
        else:
            row['GC_partial_success_firm'] = None


def GC_partial_success_1():
    db.core_fields.append('GC_partial_success_1')
    for row in db.core:
        if utils.exists(row['GC_Decision_date']):
            if row['GC_total_loss_firm'] == 0 and row['GC_total_success_firm'] == 0:
                row['GC_partial_success_1'] = 1
            else:
                row['GC_partial_success_1'] = 0


def GC_total_loss_undertaking():
    db.core_fields.append('GC_total_loss_undertaking')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['GC_Decision_date']):
                    fa.append(row2['GC_total_loss_firm'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_total_loss_undertaking'] = 0 if fa.count(0) > 0 else 1
        else:
            row['GC_total_loss_undertaking'] = None


def GC_total_loss_case():
    db.core_fields.append('GC_total_loss_case')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['GC_Decision_date']):
                    fa.append(row2['GC_total_loss_firm'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_total_loss_case'] = 0 if fa.count(0) > 0 else 1
        else:
            row['GC_total_loss_case'] = None


def GC_total_success_undertaking():
    db.core_fields.append('GC_total_success_undertaking')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['GC_Decision_date']):
                    fa.append(row2['GC_total_success_firm'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_total_success_undertaking'] = 0 if fa.count(0) > 0 else 1
        else:
            row['GC_total_success_undertaking'] = None


def GC_total_success_case():
    db.core_fields.append('GC_total_success_case')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['GC_Decision_date']):
                    fa.append(row2['GC_total_success_firm'])

        if utils.exists(row['GC_Decision_date']):
            row['GC_total_success_case'] = 0 if fa.count(0) > 0 else 1
        else:
            row['GC_total_success_case'] = None


def GC_partial_success_undertaking():
    db.core_fields.append('GC_partial_success_undertaking')
    for row in db.core:
        if utils.exists(row['GC_Decision_date']):
            row['GC_partial_success_undertaking'] = 1 if row['GC_total_loss_undertaking'] == 0 and row[
                'GC_total_success_undertaking'] == 0 else 0
        else:
            row['GC_partial_success_undertaking'] = None


def GC_partial_success_case():
    db.core_fields.append('GC_partial_success_case')
    for row in db.core:
        if utils.exists(row['GC_Decision_date']):
            row['GC_partial_success_case'] = 1 if row['GC_total_loss_case'] == 0 and row[
                'GC_total_success_case'] == 0 else 0
        else:
            row['GC_partial_success_case'] = None


def GC_judgement_paragraphs_per_firm():
    db.core_fields.append('GC_judgement_paragraphs_per_firm')
    for row in db.core:

        n = 0
        for r in db.core:
            if row['GC_Case_number'] == r['GC_Case_number']:
                n += 1

        if utils.exists(row['GC_judgement_Paragraphs']):
            row['GC_judgement_paragraphs_per_firm'] = int(row['GC_judgement_Paragraphs']) / n


def GC_judgement_paragraphs_per_undertaking():
    db.core_fields.append('GC_judgement_paragraphs_per_undertaking')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if row2['GC_Case_number'] not in fa:
                    if utils.exists(row2['GC_judgement_Paragraphs']):
                        fa[row2['GC_Case_number']] = int(row2['GC_judgement_Paragraphs'])
        row['GC_judgement_paragraphs_per_undertaking'] = None if sum(fa.values()) == 0 else sum(fa.values())


def GC_judgement_paragraphs_for_EC_case():
    db.core_fields.append('GC_judgement_paragraphs_for_EC_case')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['GC_Case_number'] not in fa:
                    if utils.exists(row2['GC_judgement_Paragraphs']):
                        fa[row2['GC_Case_number']] = int(row2['GC_judgement_Paragraphs'])
        row['GC_judgement_paragraphs_for_EC_case'] = None if sum(fa.values()) == 0 else sum(fa.values())


def GC_judgement_Articles_per_firm():
    db.core_fields.append('GC_judgement_Articles_per_firm')
    for row in db.core:

        n = 0
        for r in db.core:
            if row['GC_Case_number'] == r['GC_Case_number']:
                n += 1

        if utils.exists(row['GC_judgement_Articles']):
            row['GC_judgement_Articles_per_firm'] = int(row['GC_judgement_Articles']) / n


def GC_judgement_Articles_per_undertaking():
    db.core_fields.append('GC_judgement_Articles_per_undertaking')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if row2['GC_Case_number'] not in fa:
                    if utils.exists(row2['GC_judgement_Articles']):
                        fa[row2['GC_Case_number']] = int(row2['GC_judgement_Articles'])
        row['GC_judgement_Articles_per_undertaking'] = None if sum(fa.values()) == 0 else sum(fa.values())


def GC_judgement_Articles_for_EC_case():
    db.core_fields.append('GC_judgement_Articles_for_EC_case')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['GC_Case_number'] not in fa:
                    if utils.exists(row2['GC_judgement_Articles']):
                        fa[row2['GC_Case_number']] = int(row2['GC_judgement_Articles'])
        row['GC_judgement_Articles_for_EC_case'] = None if sum(fa.values()) == 0 else sum(fa.values())


def ECJ_N_judgements_on_EC_case_per_undertaking():
    db.core_fields.append('ECJ_N_judgements_on_EC_case_per_undertaking')
    for row in db.core:

        ECJ_cn = set()
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Decision_date']):
                    ECJ_cn.add(row2['ECJ_Case_number'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_N_judgements_on_EC_case_per_undertaking'] = len(ECJ_cn)


def ECJ_N_judgements_on_EC_case():
    db.core_fields.append('ECJ_N_judgements_on_EC_case')
    for row in db.core:

        ECJ_cn = set()
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['ECJ_Decision_date']):
                    ECJ_cn.add(row2['ECJ_Case_number'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_N_judgements_on_EC_case'] = len(ECJ_cn)


def ECJ_Appeal_lodged_D_firm():
    db.core_fields.append('ECJ_Appeal_lodged_D_firm')
    for row in db.core:
        row['ECJ_Appeal_lodged_D_firm'] = 1 if utils.exists(row['ECJ_Appeal_lodged']) else 0


def ECJ_Appeal_lodged_D_undertaking():
    db.core_fields.append('ECJ_Appeal_lodged_D_undertaking')
    for row in db.core:

        ex = False
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Appeal_lodged']):
                    ex = True
                    break

        row['ECJ_Appeal_lodged_D_undertaking'] = 1 if ex else 0


def ECJ_Appeal_lodged_D_case():
    db.core_fields.append('ECJ_Appeal_lodged_D_case')
    for row in db.core:

        ex = False
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['ECJ_Appeal_lodged']):
                    ex = True
                    break

        row['ECJ_Appeal_lodged_D_case'] = 1 if ex else 0


def ECJ_N_firm_appellants_EC_case():
    db.core_fields.append('ECJ_N_firm_appellants_EC_case')
    for row in db.core:
        n = 0
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if utils.exists(row2['ECJ_Appeal_lodged']):
                    n += 1

        row['ECJ_N_firm_appellants_EC_case'] = n


def ECJ_N_undertaking_appellants_EC_case():
    db.core_fields.append('ECJ_N_undertaking_appellants_EC_case')
    for row in db.core:
        u = set()
        for row2 in db.core:
            if row['Case'] == row2['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Appeal_lodged']):
                    u.add(row2['Undertaking'])

        row['ECJ_N_undertaking_appellants_EC_case'] = len(u)


def ECJ_EC_ratio_for_N_firm_appellants_EC_case():
    db.core_fields.append('ECJ_EC_ratio_for_N_firm_appellants_EC_case')
    for row in db.core:
        row['ECJ_EC_ratio_for_N_firm_appellants_EC_case'] = row['ECJ_N_firm_appellants_EC_case'] / row[
            'N_firms_within_EC_case']


def ECJ_EC_ratio_for_N_undertaking_plaintifs_EC_case():
    db.core_fields.append('ECJ_EC_ratio_for_N_undertaking_appellants_EC_case')
    for row in db.core:
        row['ECJ_EC_ratio_for_N_undertaking_appellants_EC_case'] = row['ECJ_N_undertaking_appellants_EC_case'] / row[
            'N_undertaking_within_EC_case']


def ECJ_judgement_N_appellants_firm():
    db.core_fields.append('ECJ_judgement_N_appellants_firm')
    for row in db.core:
        cn = row['ECJ_Case_number']
        n = 0
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['ECJ_Case_number'] == cn:
                    n += 1

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_judgement_N_appellants_firm'] = n
        else:
            row['ECJ_judgement_N_appellants_firm'] = None


def ECJ_judgement_N_appellants_undertaking():
    db.core_fields.append('ECJ_judgement_N_appellants_undertaking')
    for row in db.core:
        cn = row['ECJ_Case_number']
        undertaking = set()
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['ECJ_Case_number'] == cn:
                    undertaking.add(row2['Undertaking'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_judgement_N_appellants_undertaking'] = len(undertaking)
        else:
            row['ECJ_judgement_N_appellants_undertaking'] = None


def ECJ_duration_firm():
    db.core_fields.append('ECJ_duration_firm')
    for row in db.core:
        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_duration_firm'] = (
                    utils.parseDate(row['ECJ_Decision_date']) - utils.parseDate(row['ECJ_Appeal_lodged'])).days
        else:
            row['ECJ_duration_firm'] = None


def ECJ_duration_undertaking():
    db.core_fields.append('ECJ_duration_undertaking')
    for row in db.core:
        dd = []
        fa = []

        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Decision_date']) and utils.exists(row2['ECJ_Appeal_lodged']):
                    dd.append(utils.parseDate(row2['ECJ_Decision_date']))
                    fa.append(utils.parseDate(row2['ECJ_Appeal_lodged']))

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_duration_undertaking'] = (max(dd) - min(fa)).days
        else:
            row['ECJ_duration_undertaking'] = None


def ECJ_duration_case():
    db.core_fields.append('ECJ_duration_case')
    for row in db.core:
        dd = []
        fa = []

        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['ECJ_Decision_date']) and utils.exists(row2['ECJ_Appeal_lodged']):
                    dd.append(utils.parseDate(row2['ECJ_Decision_date']))
                    fa.append(utils.parseDate(row2['ECJ_Appeal_lodged']))

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_duration_case'] = (max(dd) - min(fa)).days
        else:
            row['ECJ_duration_case'] = None


def ECJ_Pending():
    db.core_fields.append('ECJ_Pending')
    for row in db.core:
        if utils.exists(row['ECJ_Case_number']):
            row['ECJ_Pending'] = 1 if not utils.exists(row['ECJ_Decision_date']) else 0
        else:
            row['ECJ_Pending'] = None


def ECJ_Pending_undertaking():
    db.core_fields.append('ECJ_Pending_undertaking')
    for row in db.core:
        flag = False
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                if utils.exists(row2['ECJ_Case_number']) and not utils.exists(row2['ECJ_Decision_date']):
                    flag = True
                    break

        row['ECJ_Pending_undertaking'] = 1 if flag else 0


def ECJ_Pending_case():
    db.core_fields.append('ECJ_Pending_case')
    for row in db.core:
        flag = False
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['ECJ_Case_number']) and not utils.exists(row2['ECJ_Decision_date']):
                    flag = True
                    break

        row['ECJ_Pending_case'] = 1 if flag else 0


def ECJ_total_loss_firm():
    db.core_fields.append('ECJ_total_loss_firm')
    for row in db.core:
        s = [
            'ECJ_Dissmissing_appeal',
            'ECJ_Comm_T_Grant',
            'ECJ_Total_confirmation_of_EC_decision_dissmisal_of_action_on_1st_instance',
        ]

        fb = []
        for e in s:
            fb.append(row[e])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_total_loss_firm'] = 1 if fb.count('1') > 0 else 0
        else:
            row['ECJ_total_loss_firm'] = None


def ECJ_total_success_firm():
    db.core_fields.append('ECJ_total_success_firm')
    for row in db.core:
        s = [
            'ECJ_total_referral',
            'ECJ_Total_change_of_GC_judgement',
        ]
        s1 = [
            'ECJ_Comm_P_Dissmiss',
            'ECJ_Comm_T_Grant',
        ]

        fb = []
        for e in s:
            fb.append(row[e])
        fb1 = []
        for e in s1:
            fb1.append(row[e])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_total_success_firm'] = 1 if fb.count('1') > 0 and fb1.count('0') == 0 else 0
        else:
            row['ECJ_total_success_firm'] = None


def ECJ_partial_success_firm():
    db.core_fields.append('ECJ_partial_success_firm')
    for row in db.core:
        if utils.exists(row['ECJ_Decision_date']):
            if row['ECJ_total_loss_firm'] == 0 and row['ECJ_total_success_firm'] == 0:
                row['ECJ_partial_success_firm'] = 1
            else:
                row['ECJ_partial_success_firm'] = 0


def ECJ_total_loss_undertaking():
    db.core_fields.append('ECJ_total_loss_undertaking')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Decision_date']):
                    fa.append(row2['ECJ_total_loss_firm'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_total_loss_undertaking'] = 0 if fa.count(0) > 0 else 1
        else:
            row['ECJ_total_loss_undertaking'] = None


def ECJ_total_loss_case():
    db.core_fields.append('ECJ_total_loss_case')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['ECJ_Decision_date']):
                    fa.append(row2['ECJ_total_loss_firm'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_total_loss_case'] = 0 if fa.count(0) > 0 else 1
        else:
            row['ECJ_total_loss_case'] = None


def ECJ_total_success_undertaking():
    db.core_fields.append('ECJ_total_success_undertaking')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if utils.exists(row2['ECJ_Decision_date']):
                    fa.append(row2['ECJ_total_success_firm'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_total_success_undertaking'] = 0 if fa.count(0) > 0 else 1
        else:
            row['ECJ_total_success_undertaking'] = None


def ECJ_total_success_case():
    db.core_fields.append('ECJ_total_success_case')

    for row in db.core:
        fa = []
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if utils.exists(row2['ECJ_Decision_date']):
                    fa.append(row2['ECJ_total_success_firm'])

        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_total_success_case'] = 0 if fa.count(0) > 0 else 1
        else:
            row['ECJ_total_success_case'] = None


def ECJ_partial_success_undertaking():
    db.core_fields.append('ECJ_partial_success_undertaking')
    for row in db.core:
        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_partial_success_undertaking'] = 1 if row['ECJ_total_loss_undertaking'] == 0 and row[
                'ECJ_total_success_undertaking'] == 0 else 0
        else:
            row['ECJ_partial_success_undertaking'] = None


def ECJ_partial_success_case():
    db.core_fields.append('ECJ_partial_success_case')
    for row in db.core:
        if utils.exists(row['ECJ_Decision_date']):
            row['ECJ_partial_success_case'] = 1 if row['ECJ_total_loss_case'] == 0 and row[
                'ECJ_total_success_case'] == 0 else 0
        else:
            row['ECJ_partial_success_case'] = None


def ECJ_judgement_paragraphs_per_firm():
    db.core_fields.append('ECJ_judgement_paragraphs_per_firm')
    for row in db.core:

        n = 0
        for r in db.core:
            if row['ECJ_Case_number'] == r['ECJ_Case_number']:
                n += 1

        if utils.exists(row['ECJ_judgement_Paragraphs']):
            row['ECJ_judgement_paragraphs_per_firm'] = int(row['ECJ_judgement_Paragraphs']) / n


def ECJ_judgement_paragraphs_per_undertaking():
    db.core_fields.append('ECJ_judgement_paragraphs_per_undertaking')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if row2['ECJ_Case_number'] not in fa:
                    if utils.exists(row2['ECJ_judgement_Paragraphs']):
                        fa[row2['ECJ_Case_number']] = int(row2['ECJ_judgement_Paragraphs'])
        row['ECJ_judgement_paragraphs_per_undertaking'] = None if sum(fa.values()) == 0 else sum(fa.values())


def ECJ_judgement_paragraphs_for_EC_case():
    db.core_fields.append('ECJ_judgement_paragraphs_for_EC_case')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['ECJ_Case_number'] not in fa:
                    if utils.exists(row2['ECJ_judgement_Paragraphs']):
                        fa[row2['ECJ_Case_number']] = int(row2['ECJ_judgement_Paragraphs'])
        row['ECJ_judgement_paragraphs_for_EC_case'] = None if sum(fa.values()) == 0 else sum(fa.values())


def ECJ_judgement_Articles_per_firm():
    db.core_fields.append('ECJ_judgement_Articles_per_firm')
    for row in db.core:

        n = 0
        for r in db.core:
            if row['ECJ_Case_number'] == r['ECJ_Case_number']:
                n += 1

        if utils.exists(row['ECJ_judgement_Articles']):
            row['ECJ_judgement_Articles_per_firm'] = int(row['ECJ_judgement_Articles']) / n


def ECJ_judgement_Articles_per_undertaking():
    db.core_fields.append('ECJ_judgement_Articles_per_undertaking')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case'] and row['Undertaking'] == row2['Undertaking']:
                if row2['ECJ_Case_number'] not in fa:
                    if utils.exists(row2['ECJ_judgement_Articles']):
                        fa[row2['ECJ_Case_number']] = int(row2['ECJ_judgement_Articles'])
        row['ECJ_judgement_Articles_per_undertaking'] = None if sum(fa.values()) == 0 else sum(fa.values())


def ECJ_judgement_Articles_for_EC_case():
    db.core_fields.append('ECJ_judgement_Articles_for_EC_case')
    for row in db.core:
        fa = {}
        for row2 in db.core:
            if row2['Case'] == row['Case']:
                if row2['ECJ_Case_number'] not in fa:
                    if utils.exists(row2['ECJ_judgement_Articles']):
                        fa[row2['ECJ_Case_number']] = int(row2['ECJ_judgement_Articles'])
        row['ECJ_judgement_Articles_for_EC_case'] = None if sum(fa.values()) == 0 else sum(fa.values())


def NEWS_EVENT_FILE():
    col = [
        [
            'DR_Date_News',
            'DR_dec_event', 'DR_dec_2M', 'DR_dec_15d',
            'DR_dec_15d_DJN', 'DR_dec_15d_R', 'DR_dec_15d_FT',
            'DR_dec_15d_WSJ',
        ],
        [
            'EC_pre_dec_event', 'EC_dec_event', 'EC_dec_2M',
            'EC_dec_15d', 'EC_dec_15d_DJN', 'EC_dec_15d_R',
            'EC_dec_15d_FT', 'EC_dec_15d_WSJ',
        ],
        [
            'GC_pre_dec_event', 'GC_dec_event', 'GC_dec_2M', 'GC_dec_15d',
            'GC_dec_15d_DJN', 'GC_dec_15d_R', 'GC_dec_15d_FT', 'GC_dec_15d_WSJ',
        ],
        [
            'ECJ_pre_dec_event', 'ECJ_dec_event', 'ECJ_dec_2M',
            'ECJ_dec_15d', 'ECJ_dec_15d_DJN', 'ECJ_dec_15d_R',
            'ECJ_dec_15d_FT', 'ECJ_dec_15d_WSJ',
        ]
    ]

    # Firm
    for row in db.core:
        for sklop in col:
            jeVnos = False
            for c in sklop:
                if utils.exists(row[c]):
                    jeVnos = True
                    break

            if jeVnos:
                for c in sklop:
                    row[c] = 0 if not utils.exists(row[c]) else row[c]

    # Undertaking
    sameUndertakingFirm = {}

    # Vnasaanje novih stolpcev
    for i, sklop in enumerate(col):
        if i < 2:
            for c in sklop:
                db.core_fields.append(f'{c}_undertaking')

    # Iskanje same under z firm
    for row in db.core:
        if row['Undertaking'] == row['Firm']:
            if row['Firm'] not in sameUndertakingFirm:
                sameUndertakingFirm[row['Firm']] = row

    # Overridanje z ostalimi stolpci
    for row in db.core:
        for i, sklop in enumerate(col):
            if i < 2:
                for c in sklop:
                    row[f'{c}_undertaking'] = row[c]

    # Grupiranje GC same under z case
    sameUndertakingCase = {}
    for row in db.core:
        name = f'{row["Undertaking"]}_{row["Case"]}'
        if name in sameUndertakingCase:
            sameUndertakingCase[name].append(row)
        else:
            sameUndertakingCase[name] = [row]

    # Grupiranje ECJ same under z case
    sameUndertakingCase_ECJ = {}
    for row in db.core:
        name = f'{row["Undertaking"]}_{row["Case"]}'
        if name in sameUndertakingCase_ECJ:
            sameUndertakingCase_ECJ[name].append(row)
        else:
            sameUndertakingCase_ECJ[name] = [row]

    # Vnasanje GC
    stNovihStol = 0
    for n, rows in sameUndertakingCase.items():
        # Grupiranje
        decision_dates = {}
        name = 'GC_Decision_date'
        for row in rows:
            if utils.exists(row[name]):
                if row[name] not in decision_dates:
                    decision_dates[row[name]] = [row]
                else:
                    decision_dates[row[name]].append(row)

        sameUndertakingCase[n] = decision_dates

        if len(decision_dates) > stNovihStol:
            stNovihStol = len(decision_dates)

    # Vnasanje ECJ
    stNovihStol_ECJ = 0
    for n, rows in sameUndertakingCase_ECJ.items():
        # Grupiranje
        decision_dates = {}
        name = 'ECJ_Decision_date'
        for row in rows:
            if utils.exists(row[name]):
                if row[name] not in decision_dates:
                    decision_dates[row[name]] = [row]
                else:
                    decision_dates[row[name]].append(row)

        sameUndertakingCase_ECJ[n] = decision_dates

        if len(decision_dates) > stNovihStol_ECJ:
            stNovihStol_ECJ = len(decision_dates)

    # Ustvarjanje novih praznih stolpcev
    for j, sklop in enumerate(col):
        if j == 2:
            for c in sklop:
                for i in range(stNovihStol):
                    name = f'{c}_{i + 1}_undertaking'
                    db.core_fields.append(name)

                    for under_case, decision_dates in sameUndertakingCase.items():
                        for GC_case_number, rows in decision_dates.items():
                            for row in rows:
                                if i < len(decision_dates):
                                    row[name] = decision_dates[list(decision_dates.keys())[i]][0][c]
        if j == 3:
            for c in sklop:
                for i in range(stNovihStol_ECJ):
                    name = f'{c}_{i + 1}_undertaking'
                    db.core_fields.append(name)

                    for under_case, decision_dates in sameUndertakingCase_ECJ.items():
                        for GC_case_number, rows in decision_dates.items():
                            for row in rows:
                                if i < len(decision_dates):
                                    row[name] = decision_dates[list(decision_dates.keys())[i]][0][c]


def EVENT_FILES():
    col = [
        'DR_Event_File',
        'EC_Event_dec_file',
        'GC_Event_File',
        'ECJ_Event_File',
    ]
    for c in col:
        db.core_fields.append(f'{c}_D_firm')
        db.core_fields.append(f'{c}_D_undertaking')
        db.core_fields.append(f'{c}_D_case')

        for row in db.core:

            undertakingExists = False
            for row2 in db.core:
                if row2['Case'] == row['Case'] and row2['Undertaking'] == row['Undertaking']:
                    if utils.exists(row2[c]):
                        undertakingExists = True
                        break

            caseExists = False
            for row2 in db.core:
                if row2['Case'] == row['Case']:
                    if utils.exists(row2[c]):
                        caseExists = True
                        break

            row[f'{c}_D_firm'] = 1 if utils.exists(row[c]) else 0
            row[f'{c}_D_undertaking'] = 1 if undertakingExists else 0
            row[f'{c}_D_case'] = 1 if caseExists else 0


def Commissioner_for_competition_investigation_begin():
    db.core_fields.append('Commissioner_for_competition_investigation_begin')
    comm = {
        'Andriessen': ['1/1/1981', '1/1/1985'],
        'Sutherland': ['1/1/1985', '1/1/1989'],
        'Brittan': ['1/1/1989', '1/1/1993'],
        'Miert': ['1/1/1993', '9/1/1999'],
        'Monti': ['9/1/1999', '11/1/2004'],
        'Kroes': ['11/1/2004', '2/1/2010'],
        'Almunia': ['2/1/2010', '11/1/2014'],
        'Vestager': ['11/1/2014', '1/1/2018'],

    }
    for row in db.core:
        n = None
        for name, vals in comm.items():
            if utils.parseDate(vals[0]) <= row['Investigation_begin'] <= utils.parseDate(vals[1]):
                row['Commissioner_for_competition_investigation_begin'] = name


def Commissioner_for_competition_EC_decision():
    db.core_fields.append('Commissioner_for_competition_EC_decision')
    comm = {
        'Andriessen': ['1/1/1981', '1/1/1985'],
        'Sutherland': ['1/1/1985', '1/1/1989'],
        'Brittan': ['1/1/1989', '1/1/1993'],
        'Miert': ['1/1/1993', '9/1/1999'],
        'Monti': ['9/1/1999', '11/1/2004'],
        'Kroes': ['11/1/2004', '2/1/2010'],
        'Almunia': ['2/1/2010', '11/1/2014'],
        'Vestager': ['11/1/2014', '1/1/2018'],

    }
    for row in db.core:
        n = None
        for name, vals in comm.items():
            if utils.parseDate(vals[0]) <= utils.parseDate(row['EC_Date_of_decision']) <= utils.parseDate(vals[1]):
                row['Commissioner_for_competition_EC_decision'] = name


def Commission_President_investigation_begin():
    db.core_fields.append('Commission_President_investigation_begin')
    comm = {
        'Thorn': ['1/19/1981', '1/6/1985'],
        'Delors': ['1/6/1985', '1/24/1995'],
        'Santer': ['1/24/1995', '3/15/1999'],
        'Marín': ['3/15/1999', '9/17/1999'],
        'Prodi': ['9/17/1999', '11/22/2004'],
        'Barroso': ['11/22/2004', '11/1/2014'],
        'Juncker': ['11/1/2014', '1/1/2018'],
    }
    for row in db.core:
        n = None
        for name, vals in comm.items():
            if utils.parseDate(vals[0]) <= row['Investigation_begin'] <= utils.parseDate(vals[1]):
                row['Commission_President_investigation_begin'] = name


def Commission_President_EC_decision():
    db.core_fields.append('Commission_President_EC_decision')
    comm = {
        'Thorn': ['1/19/1981', '1/6/1985'],
        'Delors': ['1/6/1985', '1/24/1995'],
        'Santer': ['1/24/1995', '3/15/1999'],
        'Marín': ['3/15/1999', '9/17/1999'],
        'Prodi': ['9/17/1999', '11/22/2004'],
        'Barroso': ['11/22/2004', '11/1/2014'],
        'Juncker': ['11/1/2014', '1/1/2018'],
    }
    for row in db.core:
        n = None
        for name, vals in comm.items():
            if utils.parseDate(vals[0]) <= utils.parseDate(row['EC_Date_of_decision']) <= utils.parseDate(vals[1]):
                row['Commission_President_EC_decision'] = name


def Commission_caseload_Investigation_begin():
    db.core_fields.append('Commission_caseload_Investigation_begin')
    for row in db.core:
        for ECad in db.core_EC_annual_data:
            if ECad['RESOLVED'] == 'Formal (substantive) decisions':
                row['Commission_caseload_Investigation_begin'] = ECad.get(str(row['Investigation_begin'].year), None)
                break


def Commission_caseload_EC_decision():
    db.core_fields.append('Commission_caseload_EC_decision')
    for row in db.core:
        for ECad in db.core_EC_annual_data:
            if ECad['RESOLVED'] == 'Formal (substantive) decisions':
                d = utils.parseDate(row['EC_Date_of_decision'])
                row['Commission_caseload_EC_decision'] = ECad.get(str(d.year), None)
                break


def ECJ_pending_cases_EC_decision():
    db.core_fields.append('ECJ_pending_cases_EC_decision')
    for row in db.core:
        for ECJad in db.core_ECJ_annual_data:
            if ECJad['NAME'] == 'Cases pending':
                d = utils.parseDate(row['EC_Date_of_decision'])
                row['ECJ_pending_cases_EC_decision'] = ECJad.get(str(d.year), None)
                break


def GC_pending_cases_EC_decision():
    db.core_fields.append('GC_pending_cases_EC_decision')
    for row in db.core:
        for ECad in db.core_ECJ_annual_data:
            if ECad['NAME'] == 'GC Cases pending':
                d = utils.parseDate(row['EC_Date_of_decision'])
                row['GC_pending_cases_EC_decision'] = ECad.get(str(d.year), None)
                break


def EPP_D_Investigation_begin():
    db.core_fields.append('EPP_D_Investigation_begin')
    for row in db.core:
        row['EPP_D_Investigation_begin'] = 1 if row['Investigation_begin'].year >= 1999 else 0


def EPP_D_EC_decision():
    db.core_fields.append('EPP_D_EC_decision')
    for row in db.core:
        row['EPP_D_EC_decision'] = 1 if utils.parseDate(row['EC_Date_of_decision']).year >= 1999 else 0


def Leniency_notice_1996_D_Investigation_begin():
    db.core_fields.append('Leniency_notice_1996_D_Investigation_begin')
    d = ['7/18/1996', '2/19/2002']
    for row in db.core:
        row['Leniency_notice_1996_D_Investigation_begin'] = 1 if utils.parseDate(d[0]) <= row[
            'Investigation_begin'] <= utils.parseDate(d[1]) else 0


def Leniency_notice_2002_D_Investigation_begin():
    db.core_fields.append('Leniency_notice_2002_D_Investigation_begin')
    d = ['2/19/2002', '12/8/2006']
    for row in db.core:
        row['Leniency_notice_2002_D_Investigation_begin'] = 1 if utils.parseDate(d[0]) <= row[
            'Investigation_begin'] <= utils.parseDate(d[1]) else 0


def Leniency_notice_2006_D_Investigation_begin():
    db.core_fields.append('Leniency_notice_2006_D_Investigation_begin')
    d = '12/8/2006'
    for row in db.core:
        row['Leniency_notice_2006_D_Investigation_begin'] = 1 if utils.parseDate(d) <= row['Investigation_begin'] else 0


def Leniency_notice_1996_D_EC_decision():
    db.core_fields.append('Leniency_notice_1996_D_EC_decision')
    d = ['7/18/1996', '2/19/2002']
    for row in db.core:
        row['Leniency_notice_1996_D_EC_decision'] = 1 if utils.parseDate(d[0]) <= utils.parseDate(
            row['EC_Date_of_decision']) <= utils.parseDate(d[1]) else 0


def Leniency_notice_2002_D_EC_decision():
    db.core_fields.append('Leniency_notice_2002_D_EC_decision')
    d = ['2/19/2002', '12/8/2006']
    for row in db.core:
        row['Leniency_notice_2002_D_EC_decision'] = 1 if utils.parseDate(d[0]) <= utils.parseDate(
            row['EC_Date_of_decision']) <= utils.parseDate(d[1]) else 0


def Leniency_notice_2006_D_EC_decision():
    db.core_fields.append('Leniency_notice_2006_D_EC_decision')
    d = '12/8/2006'
    for row in db.core:
        row['Leniency_notice_2006_D_EC_decision'] = 1 if utils.parseDate(d) <= utils.parseDate(
            row['EC_Date_of_decision']) else 0


def Fining_guidelines_1998_D_Investigation_begin():
    db.core_fields.append('Fining_guidelines_1998_D_Investigation_begin')
    d = ['1/14/1998', '9/1/2006']
    for row in db.core:
        row['Fining_guidelines_1998_D_Investigation_begin'] = 1 if utils.parseDate(d[0]) <= row[
            'Investigation_begin'] <= utils.parseDate(d[1]) else 0


def Fining_guidelines_2006_D_Investigation_begin():
    db.core_fields.append('Fining_guidelines_2006_D_Investigation_begin')
    d = '9/1/2006'
    for row in db.core:
        row['Fining_guidelines_2006_D_Investigation_begin'] = 1 if utils.parseDate(d) <= row[
            'Investigation_begin'] else 0


def Fining_guidelines_1998_D_EC_decision():
    db.core_fields.append('Fining_guidelines_1998_D_EC_decision')
    d = ['1/14/1998', '9/1/2006']
    for row in db.core:
        row['Fining_guidelines_1998_D_EC_decision'] = 1 if utils.parseDate(d[0]) <= utils.parseDate(
            row['EC_Date_of_decision']) <= utils.parseDate(d[1]) else 0


def Fining_guidelines_2006_D_EC_decision():
    db.core_fields.append('Fining_guidelines_2006_D_EC_decision')
    d = '9/1/2006'
    for row in db.core:
        row['Fining_guidelines_2006_D_EC_decision'] = 1 if utils.parseDate(d) <= utils.parseDate(
            row['EC_Date_of_decision']) else 0


def Investigation_begin_May_2004():
    db.core_fields.append('Investigation_begin_May_2004')
    may = utils.parseDate('05/01/2004')
    for row in db.core:
        row['Investigation_begin_May_2004'] = 1 if row['Investigation_begin'] > may else 0


def Settlement_regulation_D_Investigation_begin():
    db.core_fields.append('Settlement_regulation_D_Investigation_begin')
    d = '07/01/2008'
    for row in db.core:
        row['Settlement_regulation_D_Investigation_begin'] = 1 if utils.parseDate(d) <= row[
            'Investigation_begin'] else 0


def Settlement_regulation_D_EC_decision():
    db.core_fields.append('Settlement_regulation_D_EC_decision')
    d = '07/01/2008'
    for row in db.core:
        row['Settlement_regulation_D_EC_decision'] = 1 if utils.parseDate(d) <= utils.parseDate(
            row['EC_Date_of_decision']) else 0


