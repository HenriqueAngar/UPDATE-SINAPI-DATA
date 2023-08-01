import os
import time

import numpy
import pandas as pd
import pyodbc


def upload(files, date, dbcon):

    path_nf = []

    for file in files:
        origin = os.path.abspath('.')
        file_path = os.path.join(origin, 'EXTRACTION', file)

        if not os.path.exists(file_path):
            path_nf.append(f'ARQUIVO NÃO ENCONTRADO {file_path}')

        else:

            state = file.split('_')
            state = state[5]
            dataframe = pd.read_excel(file_path)
            info = convert_data(dataframe)
            send_info(info, state, date, dbcon)

            print(f'COMPOSICOES SINTETICAS DE {state} de {date} CONCLUIDO')
            time.sleep(3)

    return path_nf


def convert_data(dataframe):

    dataframe = dataframe.drop(index=range(6))
    lines = dataframe.shape[0]
    dataframe = dataframe.drop(index=range(lines-3, lines))

    info = []

    for i in range(len(dataframe)-2):

        line = dataframe.iloc[i]
        line[6] = fix_number(line[6], True)
        line[0] = fix_text(line[0])
        line[2] = fix_text(line[2])
        line[7] = fix_text(line[7])
        line[8] = fix_text(line[8])
        line[10] = fix_number(line[10], False)

        info.append(composin(line[6], line[0],
                    line[2], line[7], line[8], line[10]))
    return info


def fix_text(value):
    value = str(value).replace(',', '')
    value = value.replace('  ', '')
    value = value.replace("'", "")
    return value


def fix_number(value, isint):
    value = str(value).replace('.', '')
    value = value.replace(',', '.')

    if isint:
        value = int(value)
    else:
        value = float(value)

    return value


class composin:
    def __init__(self, idsis, classe, tipo, descri, un, preco):
        self.idsis = idsis
        self.classe = classe
        self.tipo = tipo
        self.descri = descri
        self.un = un
        self.preco = preco


def send_info(info, state, date, dbcon):

    date = int(date)
    connect_it = pyodbc.connect(dbcon)
    cursor = connect_it.cursor()

    ct = 0
    print(F'UPLOADING COMPOSIN OF {state} IN {str(date)}')

    to_insert = []
    biggest = len(info)-1
    sql_comand = 'INSERT INTO COMPOSIN (ORIGEM, SETOR, CLASSE, TIPO, DESCRI, UN, PRECO, MUNICIPIO, UF, ANOMES, PUBLICO, ID_EXTERNA)'
    sql_comand = sql_comand + 'VALUES(?,?,?,?,?,?,?,?,?,?,?,?)'

    for data in info:

        if not numpy.isnan(data.idsis):
            to_insert.append((5, 12, data.classe, data.tipo, data.descri,
                             data.un, data.preco, None, state, date, 1, data.idsis))

        if ct % 500 == 0 or ct == biggest:
            print(F' {ct} OF {biggest} UPLOADED FOR COMPOSIN {state} {str(date)}')
            cursor.executemany(sql_comand, to_insert)
            connect_it.commit()
            to_insert = []

        ct += 1

    print(F'ALL REGISTERS OF INSUMOS FROM {state} UPLOADED')
