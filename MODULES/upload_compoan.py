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
            info = convert_data(dataframe, state, date)
            send_info(info, dbcon)
            print(f'INSUMOS DE {state} de {date} CONCLUIDO')
            time.sleep(3)

    return path_nf


def convert_data(dataframe, state, date):
    dataframe = dataframe.drop(index=range(7))
    lines = dataframe.shape[0]
    dataframe = dataframe.drop(index=range(lines-3, lines))
    dataframe = dataframe.iloc[:, [6, 11, 12, 8, 16, 17]]

    info = []
    jump = False
    df = dataframe

    for i in range(len(df)-1):
        if not jump:

            actual = df.iloc[i][0]
            next = df.iloc[i+1][0]

            if actual != next:
                jump = True

            line = df.iloc[i]
            data = compoan(line[0], line[3], line[4], line[5], state, date)
            data.setid(line[1], line[2])
            info.append(data)

        else:
            jump = False

    return info


class compoan:
    def __init__(self, sin, un, qtd, price, uf, date):
        self.idsis = fix_number(sin, True)
        self.an = None
        self.ins = None
        self.un = fix_text(un)
        self.qtd = fix_number(qtd, False)
        self.price = fix_number(price, False)
        self.uf = uf
        self.date = int(date)

    def setid(self, type, value):

        if type == 'INSUMO':
            self.ins = int(value)
        else:
            self.an = int(value)


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


def send_info(info, dbcon):

    connect_it = pyodbc.connect(dbcon)
    cursor = connect_it.cursor()

    ct = 0
    to_insert = []
    biggest = len(info)-1
    sql_comand = 'INSERT INTO COMPOAN (COMPO, COMPO_REF, INSUMO_REF, ORIGEM, UN, QTDE, PRECO, UF, ANOMES, PUBLICO)'
    sql_comand = sql_comand + ' VALUES(?,?,?,?,?,?,?,?,?,?)'
    print(F'UPLOADING COMPOAN OF {info[0].uf} IN {str(info[0].date)}')

    for data in info:
        if not numpy.isnan(data.idsis):
            to_insert.append((data.idsis, data.an, data.ins, 5, data.un,
                              data.qtd, data.price, data.uf, data.date, 1))

        if ct % 500 == 0 or ct == biggest:
            print(
                F' {ct} OF {biggest} UPLOADED FOR COMPOAN {data.uf} {str(data.date)}')
            cursor.executemany(sql_comand, to_insert)
            connect_it.commit()
            to_insert = []

        ct += 1

    print(F'{ct} registers uploaded')
