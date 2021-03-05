import collections
import os
import re
import shutil
import csv
from statistics import mean

from src import utils

def read_csv_core_out_tickers():
    csvEle = []
    import codecs
    csvfile = codecs.open('../data/csv/CORE/core.csv', 'rU', 'utf-16')
    reader = csv.DictReader(csvfile)
    for row in reader:
        csvEle.append(row)
    return csvEle

def read_csv_mergers():
    csvEle = []
    with open('../data/csv/CORE/core_mergers.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            csvEle.append(row)
    return csvEle


def write_csv_core_out_tickers(csvRows):
    with open('../data/pdfs/csv/EC_decision_news.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, quotechar='"', fieldnames=list(csvRows[0].keys()))
        writer.writeheader()
        writer.writerows(csvRows)


def read_pdfs():
    pdfTypes = {}
    for dir in os.listdir('../data/pdfs/raw'):
        fileDict = {}
        for file in os.listdir(f'../data/pdfs/raw/{dir}'):
            fileList = file.replace('.txt', '').split(', ')
            num = fileList[-1]
            fileNum = int(num) if num.isnumeric() else 1

            f = open(f'../data/pdfs/raw/{dir}/{file}', 'r', encoding="utf8", errors='ignore')

            if num.isnumeric():  # Ce je stevilka moram imenu filea odstraniti stevilko
                file = ', '.join(fileList[:-1]) + '.txt'

            if file not in fileDict:
                fileDict[file] = {fileNum: f.read()}
            else:
                fileDict[file][fileNum] = f.read()
            f.close()
        pdfTypes[dir] = fileDict
    return pdfTypes


def get_headlines(pdfDicts):
    print("GET HEADLIENS")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        dict[dir] = {}
        for file, fileDict in dirDict.items():
            hds = []
            for num, content in fileDict.items():
                #File with HD code in it
                for line in content.split("\n"):
                    if line.strip().startswith('HD '):
                        hd = re.sub(r'(WC|BY).*', '', line)
                        hds.append(hd.replace('HD', '').strip())
                #File without HD code in it
                if len(hds) == 0:
                    vrstice = content.split("\n")
                    for i, line in enumerate(vrstice):
                        lineEle = line.split()
                        headlineCandidates = []
                        if len(lineEle) == 2: # Vrstica kjer je word count
                            if lineEle[1] == 'words':
                                emptyLines = 0
                                for j in range(i, i-10, -1):
                                    prevLine = vrstice[j]
                                    if prevLine == '':
                                        emptyLines += 1
                                        if emptyLines == 2:
                                            break
                                    elif 'press' not in prevLine.lower() or 'by' not in prevLine.lower().split(' ')[0]:
                                        headlineCandidates.append(prevLine)

                        if len(headlineCandidates) > 0:
                            headline = sorted(headlineCandidates, key=lambda hc: len(hc), reverse=True)[0]
                            hds.append(headline)

            dict[dir][file] = hds

    return dict


def get_word_count(pdfDicts):
    print("GET WORD COUNT")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        dict[dir] = {}
        for file, fileDict in dirDict.items():
            info = []
            for num, content in fileDict.items():
                for i, line in enumerate(content.split("\n")):
                    lineEle = [ele.strip() for ele in line.split(" ")]
                    if len(lineEle) >= 2:
                        if lineEle[-1] == 'words' and lineEle[-2].replace(',', '').isnumeric():
                            info.append(int(lineEle[-2].replace(",", '')))

            dict[dir][file] = info

    return dict


def get_publisher(pdfDicts):
    print("GET PUBLISHER")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        dict[dir] = {}
        for file, fileDict in dirDict.items():
            hds = []
            for num, content in fileDict.items():
                for line in content.split("\n"):
                    if line.strip().startswith('SN '):
                        hds.append(line.replace('SN', '').strip())
                if len(hds) == 0:
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        lineEle = line.split()
                        if len(lineEle) == 2:
                            if lineEle[1] == 'words':
                                if ':' in lines[i+2] or 'pm' in lines[i+2].lower() or 'am' in lines[i+2].lower():
                                    hds.append(lines[i+3])
                                else:
                                    hds.append(lines[i+2])

            dict[dir][file] = hds

    return dict


def get_dates(pdfDicts):
    print("GET DATES")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        dict[dir] = {}
        for file, fileDict in dirDict.items():
            hds = []
            for num, content in fileDict.items():

                #File with PD code in it
                for line in content.split("\n"):
                    if line.strip().startswith('PD '):
                        lineEle = line.strip().split()
                        hds.append(' '.join(lineEle[-3:]))
                #File without PD code in it
                if len(hds) == 0:
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        lineEle = line.split()
                        if len(lineEle) == 2:
                            if lineEle[1] == 'words':
                                nextlineEle = lines[i+1].split()
                                if len(nextlineEle) == 3:
                                    if nextlineEle[0].isnumeric() and nextlineEle[-1].isnumeric():
                                        hds.append(lines[i+1])


            dict[dir][file] = hds

    return dict


def odstrani_prazne_prostore(pdfDicts):
    print("ODSTRANI PRAZNE PROSTORE")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():
                content = re.sub(r'\n{3,}', '\n\n', content).strip()
                pdfDicts[dir][file][num] = content

    return pdfDicts


def odstrani_summary(pdfDicts):
    print("ODSTRANI SUMMARY")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():
                newContent = ""
                isSummary = False
                for line in content.split('\n'):
                    if line.startswith('Search Summary'):
                        isSummary = True
                    if line.startswith('Timestamp'):
                        isSummary = False
                        continue

                    if not isSummary:
                        newContent += line + '\n'

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def odstrani_prazne_zacetke(pdfDicts):
    print("ODSTRANI PRAZNE ZACETKE")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():
                content = re.sub(r'\n{3,}', '\n\n', content).strip()
                newContent = ""
                for line in content.split("\n"):
                    newContent += line.strip() + "\n"

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def odstrani_kazala(pdfDicts):
    print("ODSTRANI KAZALA")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():
                newContent = ""
                for line in content.split("\n"):
                    if "." * 5 in line:
                        continue
                    else:
                        newContent += line + "\n"

                pdfDicts[dir][file][num] = newContent
    return pdfDicts


def odstrani_vrstico_pojavitve(pdfDicts):
    print("ODSTRANI VRSTICO POJAVITVE")
    besede = [
        'Factiva',
        '©',
        'Copyright',
        'All Rights Reserved.',
        'All rights reserved.'
    ]
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():

                newContent = ""
                for line in content.split('\n'):
                    removeLine = False
                    for b in besede:
                        if b in line:
                            removeLine = True
                            break
                    if not removeLine:
                        newContent += line + '\n'
                    else:
                        newContent += '\n'

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def odstrani_vrstico_samostojne_besede(pdfDicts):
    print("ODSTRANI VRSTICO SAMOSTOJNE BESEDE")
    besede = [
        'English'
    ]
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():

                newContent = ""
                for line in content.split('\n'):
                    removeLine = False
                    for b in besede:
                        if line.strip() == b:
                            removeLine = True
                            break
                    if not removeLine:
                        newContent += line + '\n'
                    else:
                        newContent += '\n'

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def odstrani_documents(pdfDicts):
    print("ODSTRANI DOCUMENTS")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():

                newContent = ""
                for line in content.split('\n'):
                    lineEle = line.split()
                    if len(lineEle) > 1:
                        if lineEle[0] in ['Documents', 'Document'] and len(lineEle) == 2:
                            newContent += '\n'
                            continue
                    newContent += line + '\n'

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def odstrani_besede(pdfDicts):
    print("ODSTRANI BESEDE")
    besede = [
        'BY  ', 'HD  ', 'PD  ', 'SN  ', 'SC  ',
        'CR  ', 'WC  ', 'ET  ', 'LA  ', 'CY  ',
        'LP', 'TD'
    ]

    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():
                for b in besede:
                    content = content.replace(b, "")
                pdfDicts[dir][file][num] = content
    return pdfDicts


def odstrani_vrstice(pdfDicts):
    print("ODSTRANI VRSTICE")
    besedeNaZacetku = [
        'Click Here for related articles',
        'CO  ', 'CY  ', 'LA  ', 'SC  ', 'IN  ',
        'NS  ', 'RE  ', 'IPD  ', 'IPC  ', 'PUB  ',
        'AN  ', '(c) ', 'SE  ', 'RF  ',
        'ED  ', 'PG  ', 'Lex, Page ', 'CLM  ', 'ART  '
    ]
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():

                newContent = ""
                for line in content.split('\n'):
                    removeLine = False
                    for b in besedeNaZacetku:
                        if line.lstrip().startswith(b):
                            removeLine = True
                            break
                    if not removeLine:
                        newContent += line + '\n'
                    else:
                        newContent += '\n'

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def odstrani_pages(pdfDicts):
    print("ODSTRANI PAGES")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():

                newContent = ""
                for line in content.split('\n'):
                    lineEle = line.split(' ')
                    if len(lineEle) > 1:
                        if lineEle[0] == 'Page' and lineEle[1].isnumeric():
                            newContent += '\n'
                            continue
                    newContent += line + '\n'

                pdfDicts[dir][file][num] = newContent

    return pdfDicts


def write_pdfs(pdfDicts):
    shutil.rmtree('../data/pdfs/joined')
    for dir, dirDict in pdfDicts.items():
        os.makedirs(f'../data/pdfs/joined/{dir}')
        for file, fileDict in dirDict.items():
            content = ""
            fileDictOrdered = collections.OrderedDict(sorted(fileDict.items()))
            for num, value in fileDictOrdered.items():
                content += value
            pdfDicts[dir][file] = content
            f = open(f'../data/pdfs/joined/{dir}/{file}', 'w')
            f.write(content)
            f.close()
    return pdfDicts


def ustvari_csv_with_texts(core_out_tickers, core_mergers,pdfDicts, headlines, wordCount, dates, publisher):
    newCsv = []
    errors = []
    success = 0
    mapping = {
        'merger_commission_decision': {
            'fileName': 'EC_Event_dec_file',
            'date': 'EC_Date_of_decision'
        },
        'merger_GC_decision': {
            'fileName': 'GC_Event_File',
            'date': 'GC_Decision_date'
        },
        'merger_ECJ_decision': {
            'fileName': 'ECJ_Event_File',
            'date': 'ECJ_Decision_date'
        },
        'merger_announcement': {
            'fileName': 'Event_AN',
            'date': 'Announcement'
        },
        'merger_initiation': {
            'fileName': 'EC_Event_Initiation',
            'date': 'Initiation'
        },
        'dawn_raids': {
            'fileName': 'DR_Event_File',
            'date': 'Dawn_raid'
        },
        'commission_decision': {
            'fileName': 'EC_Event_dec_file',
            'date': 'EC_Date_of_decision'
        },
        'GC_decision': {
            'fileName': 'GC_Event_File',
            'date': 'GC_Decision_date'
        },
        'ECJ_decision': {
            'fileName': 'ECJ_Event_File',
            'date': 'ECJ_Decision_date'
        },
    }
    for dir, column in mapping.items():


        core = core_out_tickers
        if dir.startswith('merger_'):
            core = core_mergers

        core_colume_name = mapping[dir]['fileName']
        core_date = mapping[dir]['date']

        for fileName, content in pdfDicts.get(dir, {}).items():

            if 'BREZ' in fileName or 'brez' in fileName:
                continue

            crow = None
            rowNum = None
            for i, core_row in enumerate(core):
                coreFile = core_row[core_colume_name].split('/')[-1].replace(".pdf", ".txt").replace(', 1.txt', '.txt')
                if coreFile == fileName:
                    success+=1
                    crow = core_row
                    rowNum = i
                    break

            if crow is not None:

                # Convert core date to pdf form...
                coreDate = utils.parseDate(crow[core_date])
                if coreDate is not None:
                    coreDatePdfStr = utils.convert2pdfDate(coreDate)
                else:
                    print(f"ERR: empty date: {dir}/{fileName}, core row num: {rowNum}, {crow[core_date]}")
                    continue
                date_matching = 1 if coreDatePdfStr in dates[dir][fileName] else 0

                firstPub = None
                firstPubDayDiff = None

                if date_matching == 0:
                    for csvDate in dates[dir][fileName]:
                        csvDATE = utils.parsePdfDate(csvDate)
                        if csvDATE >= coreDate:
                            firstPub = utils.convert2pdfDate(csvDATE)
                            firstPubDayDiff = (csvDATE - coreDate).days
                            break


                try:
                    newCsv.append({
                        'group_name': dir,
                        'file_name': fileName,
                        'headlines': ', '.join(headlines[dir][fileName]),
                        'word_count': ', '.join([str(ele) for ele in wordCount[dir][fileName]]),
                        'dates': ', '.join(dates[dir][fileName]),
                        'publishers': ', '.join(publisher[dir][fileName]),
                        'articles_sum': len(headlines[dir][fileName]),
                        'word_count_sum': sum(wordCount[dir][fileName]),
                        'word_count_ave': mean(wordCount[dir][fileName]),
                        'publisher_WSJ': 1 if 'wall street journal' in str(publisher[dir][fileName]).lower() else 0,
                        'publisher_REU': 1 if 'reuters' in str(publisher[dir][fileName]).lower() else 0,
                        'publisher_DJN': 1 if 'dow jones' in str(publisher[dir][fileName]).lower() else 0,
                        'publisher_FT': 1 if 'financial times' in str(publisher[dir][fileName]).lower() else 0,
                        'date_core_formated': coreDatePdfStr,
                        'date_core_raw': crow[core_date],
                        'date_matching': date_matching,
                        'date_first_pub': firstPub,
                        'date_first_pub_days': firstPubDayDiff,
                        'text': content,
                    })
                except Exception as err:
                    print(err, dir, fileName)
            else:
                if 'First Rumour' not in fileName:
                    errors.append(f'{dir}/{fileName}')

    print('Errors:')
    for e in errors:
        print('\t', e)
    print(f"Success rate: {len(errors)}/{success}")
    return newCsv


# PDF handling...
pdfDicts = read_pdfs()

headlines = get_headlines(pdfDicts)
dates = get_dates(pdfDicts)
publisher = get_publisher(pdfDicts)
wordCount = get_word_count(pdfDicts)

pdfDicts = odstrani_prazne_prostore(pdfDicts)
pdfDicts = odstrani_besede(pdfDicts)
pdfDicts = odstrani_vrstice(pdfDicts)
pdfDicts = odstrani_documents(pdfDicts)
pdfDicts = odstrani_vrstico_pojavitve(pdfDicts)
pdfDicts = odstrani_vrstico_samostojne_besede(pdfDicts)
pdfDicts = odstrani_pages(pdfDicts)
pdfDicts = odstrani_kazala(pdfDicts)
pdfDicts = odstrani_summary(pdfDicts)
pdfDicts = odstrani_prazne_zacetke(pdfDicts)
pdfDicts = write_pdfs(pdfDicts)

# NEW CSV handling...
core_out_tickers = read_csv_core_out_tickers()
core_mergers = read_csv_mergers()
new_core_out_tickers = ustvari_csv_with_texts(core_out_tickers, core_mergers ,pdfDicts, headlines, wordCount, dates, publisher)
write_csv_core_out_tickers(new_core_out_tickers)
