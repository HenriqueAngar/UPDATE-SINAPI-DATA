
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
            path_nf.append(f'ARQUIVO NÃO ENCONTRADO ENCONTRADO {file_path}')
        else:
            state = file.split('_')
            state = state[4]

            dataframe = pd.read_excel(file_path)
            dataframe = convert_data(dataframe)
            info = get_info(dataframe)
            send_info(info, state, date, dbcon)

            print(f'INSUMOS DE {state} de {date} CONCLUIDO')
            time.sleep(3)

    return path_nf


def convert_data(dataframe):

    dataframe = dataframe.drop(index=range(6))
    dataframe = dataframe.drop(dataframe.columns[[3]], axis=1)

    lines = dataframe.shape[0]
    dataframe = dataframe.drop(index=range(lines-3, lines))

    return dataframe


def get_info(dataframe):

    info = []

    for i in range(len(dataframe)-2):

        line = dataframe.iloc[i]

        line[0] = int(line[0])
        line[1] = str(line[1]).replace(',', '')
        line[1] = line[1].replace('  ', '')
        line[1] = line[1].replace("'", "")
        line[2] = str(line[2]).replace('  ', '')
        line[3] = str(line[3]).replace('.', '')
        line[3] = line[3].replace(',', '.')
        line[3] = float(line[3])

        content = insumo(line[0], line[1], line[2], line[3])
        info.append(content)

    return info


class insumo:
    def __init__(self, idsis, descri, un, preco):
        self.idsis = int(idsis)
        self.descri = descri
        self.un = un
        self.preco = preco


def send_info(info, state, date, dbcon):

    date = int(date)
    connect_it = pyodbc.connect(dbcon)
    cursor = connect_it.cursor()

    ct = 0
    print(F'UPLOADING INSUMOS OF {state} IN {str(date)}')

    to_insert = []
    biggest = len(info)-1
    sql_comand = 'INSERT INTO INSUMOS (ORIGEM, SETOR, CLASSE, TIPO, DESCRI, UN, PRECO, MUNICIPIO, UF, ANOMES, PUBLICO, ID_EXTERNA)'
    sql_comand = sql_comand + ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    for data in info:

        if not numpy.isnan(data.idsis):
            to_insert.append((5, 12, None, None, data.descri, data.un,
                              data.preco, None, state, date, 1, data.idsis))

        if ct % 250 == 0 or ct == biggest:
            print(F' {ct} OF {biggest} UPLOADED FOR INSUMOS {state} {str(date)}')
            cursor.executemany(sql_comand, to_insert)
            connect_it.commit()
            to_insert = []

        ct += 1

    print(F'ALL REGISTERS OF INSUMOS FROM {state} UPLOADED')
