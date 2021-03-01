import collections
import os
import re
import shutil


def read_pdfs():
    pdfTypes = {}
    for dir in os.listdir('../data/pdfs/raw'):
        fileDict = {}
        for file in os.listdir(f'../data/pdfs/raw/{dir}'):
            fileList = file.replace('.txt', '').split(', ')
            num = fileList[-1]
            fileNum = int(num) if num.isnumeric() else 0

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


def odstrani_prazne_prostore(pdfDicts):
    print("ODSTRANI PRAZNE PROSTORE")
    for dir, dirDict in pdfDicts.items():
        for file, fileDict in dirDict.items():
            for num, content in fileDict.items():
                content = re.sub(r'\n+', '\n', content).strip()
                pdfDicts[dir][file][num] = content
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


def odstrani_vrstico_pojavitve(pdfDicts):
    print("ODSTRANI VRSTICO POJAVITVE")
    besede = [
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
                        if lineEle[0] == 'Documents' and len(lineEle) == 2:
                            continue
                    newContent += line + '\n'

                pdfDicts[dir][file][num] = newContent

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


pdfDicts = read_pdfs()
pdfDicts = odstrani_prazne_prostore(pdfDicts)
pdfDicts = odstrani_besede(pdfDicts)
pdfDicts = odstrani_vrstice(pdfDicts)
pdfDicts = odstrani_documents(pdfDicts)
pdfDicts = odstrani_vrstico_pojavitve(pdfDicts)
pdfDicts = odstrani_vrstico_samostojne_besede(pdfDicts)
pdfDicts = odstrani_pages(pdfDicts)
pdfDicts = odstrani_kazala(pdfDicts)
write_pdfs(pdfDicts)
