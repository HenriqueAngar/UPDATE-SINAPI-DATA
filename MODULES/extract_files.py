
import os
import zipfile


def filter_files(names, yearmonth, states):

    extraction = 'EXTRACTION'
    extract_files(names, extraction)

    files = get_files(yearmonth, states)

    class paths:
        def __init__(self, insumos, composin, compoan):
            self.insumos = insumos
            self.composin = composin
            self.compoan = compoan

    return paths(files[0], files[1], files[2])


def extract_files(names, extraction):

    if not os.path.exists(extraction):
        os.makedirs(extraction)

    for file in names:
        file_path = os.path.join(os.getcwd(), file)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extraction)
        os.remove(file_path)


def get_files(yearmonth, states):

    insumos = []
    composin = []
    compoan = []

    for state in states:

        txt = f'SINAPI_Preco_Ref_Insumos_{state}_{yearmonth}_NaoDesonerado.XLS'
        insumos.append(txt)

        txt = f'SINAPI_Custo_Ref_Composicoes_Analitico_{state}_{yearmonth}_NaoDesonerado.xls'
        compoan.append(txt)

        txt = f'SINAPI_Custo_Ref_Composicoes_Sintetico_{state}_{yearmonth}_NaoDesonerado.xls'
        composin.append(txt)

    return [insumos, composin, compoan]
