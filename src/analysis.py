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
                    htp = row2['Holding_Ticker_parent']

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
                if str(row2['A_101']) != '1':
                    A101_only = False
                    break

        row['Case_A101_only'] = 1 if A101_only else 0

def Case_A102_only():
    db.core_fields.append('Case_A102_only')

    for row in db.core:
        A102_only = True
        for row2 in db.core:
            if row['Case'] == row2['Case']:
                if str(row2['A_102']) != '1':
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
            row['Case_cartel_VerR'] = ''


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
            row['Case_Ringleader'] = ''



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
                beginMins.append(beginMin)
                endMaxs.append(endMax)

        minBegin = min(beginMins)
        maxEnd = max(endMaxs)

        row['InfringeDurationOverallUndertaking'] = (maxEnd - minBegin).days


def Ticker_firm_D():
    db.core_fields.append('Ticker_firm_D')
    for row in db.core:
        row['Ticker_firm_D'] = 1 if utils.exists(row['Ticker_firm']) else 0


def Ticker_undertaking_D():
    db.core_fields.append('Ticker_undertaking_D')
    for row in db.core:
        row['Ticker_undertaking_D'] = 1 if utils.exists(row['Ticker_undertaking']) else 0


def Holding_Ticker_parent_D():
    db.core_fields.append('Holding_Ticker_parent_D')
    for row in db.core:
        row['Holding_Ticker_parent_D'] = 1 if utils.exists(row['Holding_Ticker_parent']) else 0

def
db.save_core()
