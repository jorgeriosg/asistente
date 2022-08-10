#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import random
import pandas
import base64
import traceback
import codecs

from mods.config import SERVER_PATH
from mods import datadog as dg


def b64_generator(data, alias=None, columnas=None):
    """
    :param data:
    :param alias:
    :return:
    """
    salida = False
    try:
        nombre_archivo = alias if alias else 'temp_file_heroes'
        rand = str(random.randrange(1000000))

        if isinstance(data, list):
            output = pandas.DataFrame(data)
        else:

            output = pandas.DataFrame(list(data))
        if columnas:
            output.columns = columnas
        file_name = SERVER_PATH + nombre_archivo + rand + '.csv'
        output.to_csv(file_name, ';', index=False, encoding='UTF-8')
        csv_file = codecs.open(file_name, 'r', encoding='utf-8').read()
        b64 = str(base64.b64encode(csv_file.encode('utf-8')))

        b64 = b64.replace("b'", '').replace("'", '') if b64 else ''

        salida = "data:text/csv;base64," + b64 if b64 != '' else ''
        os.remove(file_name)
    except Exception as unknown_exception:
        dg.log_datadog(f'Unknown error at b64_generator: {unknown_exception}, Tracebak: {traceback.format_exc()}')
    return salida
