
import os
import time

import requests


def dowload_files(date, states):

    urls = generate_urls(date, states)
    result = get_files(urls)

    if result[0]:
        return result[1]


def generate_urls(date, states):

    year = date[:4]
    month = date[4:]
    period = month+year

    def make_link(fu):
        return f'https://www.caixa.gov.br/Downloads/sinapi-a-partir-jul-2009-{fu.lower()}/SINAPI_ref_Insumos_Composicoes_{fu}_{period}_NaoDesonerado.zip'

    return list(map(make_link, states))


def get_files(urls):

    names = []

    for url in urls:
        print('file requested')
        request_file(url)
        file_name = url.split('/')[-1]
        verify_dowload(file_name)
        names.append(file_name)

    return [True, names]


def request_file(url):
    # Request File
    response = requests.get(url)
    if response.status_code == 200:
        file_name = url.split('/')[-1]

        # Save file in disk
        with open(file_name, 'wb') as arquivo:
            arquivo.write(response.content)
        print(f'Dowloaded {file_name}')
    else:
        print(f'Dowload failed {url}. Status code: {response.status_code}')


def verify_dowload(file_name):
    last_size = 0
    while True:
        actual_size = os.path.getsize(file_name)
        if actual_size == last_size:
            break
        last_size = actual_size
        time.sleep(0.1)
