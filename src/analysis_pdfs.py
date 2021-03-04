import collections
import os
import re
import shutil
import csv


def read_csv_core_out_tickers():
    csvEle = []
    with open('../data/csv/core_out_tickers.csv', 'r') as csvfile:
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
        for file, fileDict in dirDict.items():
            hds = []
            for num, content in fileDict.items():
                #File with HD code in it
                for line in content.split("\n"):
                    if 'HD ' in line:
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

            dict[file] = ', '.join(hds)

    return dict


def get_word_count(pdfDicts):
    print("GET NEWS DATES")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            info = []
            for num, content in fileDict.items():
                for line in content.split("\n"):
                    lineEle = [ele.strip() for ele in line.split(" ")]
                    if len(lineEle) >= 2:
                        if lineEle[-1] == 'words':
                            info.append(lineEle[-2].replace(",", ''))
            dict[file] = ",".join(info)

    return dict


def get_publisher(pdfDicts):
    print("GET PUBLISHER")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            hds = []
            for num, content in fileDict.items():
                for line in content.split("\n"):
                    if 'SN ' in line:
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

            dict[file] = ', '.join(hds)

    return dict


def get_dates(pdfDicts):
    print("GET DATES")
    dict = {}  # filename: title
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            hds = []
            for num, content in fileDict.items():

                #File with PD code in it
                for line in content.split("\n"):
                    if 'PD ' in line:
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


            dict[file] = ', '.join(hds)

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


def ustvari_csv_with_texts(core_out_tickers, pdfDicts, headlines, wordCount, dates, publisher):
    newCsv = []
    errors = []
    success = 0
    mapping = {
        'decisions': 'EC_Event_dec_file',
        'dawn_raid': 'DR_Event_File'
    }
    for dir, column in mapping.items():
        core_colume_name = mapping[dir]
        for fileName, content in pdfDicts.get(dir, {}).items():

            if 'BREZ' in fileName or 'brez' in fileName:
                continue

            crow = None
            for core_row in core_out_tickers:
                coreFile = core_row[core_colume_name].split('/')[-1].replace(".pdf", ".txt").replace(', 1.txt', '.txt')
                if coreFile == fileName:
                    success+=1
                    crow = core_row
                    break

            if crow is not None:
                newCsv.append({
                    'group_name': dir,
                    'file_name': fileName,
                    'headlines': headlines[fileName],
                    'word_count': wordCount[fileName],
                    'dates': dates[fileName],
                    'publishers': publisher[fileName],
                    'text': content,
                })
            else:
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
new_core_out_tickers = ustvari_csv_with_texts(core_out_tickers, pdfDicts, headlines, wordCount, dates, publisher)
write_csv_core_out_tickers(new_core_out_tickers)
