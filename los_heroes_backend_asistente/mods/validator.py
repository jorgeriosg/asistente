#!/usr/bin/env python
# -*- coding: utf-8 -*-
import doctest
import re
import datetime
import sys
import re

from itertools import cycle
from cognitiva_xss_3.xss3 import XssClean


def validate_xss2(input_text):
    # se mantiene xss2 para no tener que cambiar en muchas partes pero se usa xss3
    if input_text is None or not(isinstance(input_text, str)):
        return input_text
    return XssClean.sanitizeHtml(input_text).strip()


def validate_date(date, format='%d-%m-%Y'):
    """
    valida la fecha verificando formato y que exista
        :param date: fecha a verificar
        :format: formato de fecha
    """
    try:
        datetime.datetime.strptime(date, format)
    except ValueError:
        return False
    else:
        return True


def validate_name(name):
    """
    verifica que nombre contenga solo letras
        :param name: nombre a validar
    """
    return re.search(r'[A-Za-záéíóúñÑ ]', name)


def validate_movistar_phone(phone):
    """
    verifica que telefono tenga 9 o 15 numeros (formato movistar)
        :param phone: telefono a verificar
    """
    return re.search(r'\b(?:[0-9]{15}|[0-9]{9})\b', phone)


def verify_rut(texto):
    rut = ''
    try:
        verificador = ''
        find_digit = re.findall(r'\d+', texto)
        concat_digit = ''.join(find_digit)
        # en caso de que STT detecte millones como 000000 se escapan
        concat_digit = concat_digit.replace('000000', '')
        if ('k' in texto or 'K' in texto or 'ca' in texto or 'CA' in texto) and len(concat_digit) <= 8:
            verificador = 'K'
        if ('cero' in texto or 'CERO' in texto or 'Cero' in texto or 'zero' in texto) and len(concat_digit) <= 8:
            verificador = '0'

        rut = concat_digit + verificador
        rut = rut.upper()
        rut = rut.replace("-", "")
        rut = rut.replace(".", "")
        aux = rut[:-1]
        dv = rut[-1:]

        revertido = map(int, reversed(str(aux)))
        factors = cycle(range(2, 8))
        s = sum(d * f for d, f in zip(revertido, factors))
        res = (-s) % 11

        if len(rut) > 3 and rut[-2] != '-':
            rut = rut[0:-1] + '-' + rut[-1]
        if str(res) == dv:
            return True, rut
        elif (dv == "K" and res == 10):
            lista = list(rut)
            lista[-1] = 'K'
            rut = ''.join(lista)
            return True, rut
        else:
            return False, rut
    except Exception:
        return False, rut


def verify_folio(folio):
    ret = re.findall(r'\d{2,10}', folio)
    if ret:
        return True, ret[0]

    return False, None


def validate_email(email):
    """
        El campo email cuando este venga cargado se valida que sea un validamente
        bien escrito NombreUsuario@dominio.ext
            :param email: mail a verificar
    """
    return re.search(r'[^@]+@[^@]+\.[^@]+', email)


def validate_headers(request: dict, expected: list) -> (bool, list):
    """
    Función que valida los parametros necesarios de un request

    Args:

        request: Request en formato json
        expected (list): Parametros esperados del request

    Returns:

        missing (Bool): Devuelve True si faltan parametros en el request, de lo contrario False
        parameters (list): Parametros faltantes del request
    """
    parameters = [parameter for parameter in request.keys()]
    missing_parameters = (list(set(expected) - set(parameters)))
    if missing_parameters != []:
        missing = True
        parameters = (", ".join(missing_parameters))
    else:
        missing = False
        parameters = None
    return (missing, parameters)
