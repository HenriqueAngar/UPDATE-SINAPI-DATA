# UPDATE PRICES ON DB

import os
import shutil

import pyodbc

from MODULES import dbconection as dbcon
from MODULES import dowload_files as dwf
from MODULES import extract_files as ef
from MODULES import upload_compoan as ucan
from MODULES import upload_composin as usin
from MODULES import upload_insumos as upin


def ATUSINAPI(yearmonth):

    states = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
        'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
        'SP', 'SE', 'TO'
    ]

    names = dwf.dowload_files(yearmonth, states)
    files = ef.filter_files(names, yearmonth, states)
    conection = dbcon.get_conection()

    upin.upload(files.insumos, yearmonth, conection)
    usin.upload(files.composin, yearmonth, conection)
    ucan.upload(files.compoan, yearmonth, conection)

    origin = os.path.abspath('.')
    extraction = os.path.join(origin, 'EXTRACTION')
    shutil.rmtree(extraction)

    connect_it = pyodbc.connect(conection)
    cursor = connect_it.cursor()
    sql_comand = 'EXEC ATUSINAPI '+str(yearmonth)
    cursor.execute(sql_comand)
    connect_it.commit()

    print(f'SINAPI UPDATE DONE FOR {yearmonth}')


ATUSINAPI('202306')
