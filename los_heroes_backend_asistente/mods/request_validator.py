#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re

from mods import validator
from mods import mongo_library
from mods import response_formats


def verify_check(request):
    invalid = False
    if 'begin' not in request:
        invalid = True
    elif 'end' not in request:
        invalid = True
    elif 'type' not in request:
        invalid = True

    return invalid


def verify_form(request):
    invalid = False
    if 'name' not in request and request['type'] in (1, 2):
        invalid = True
    elif 'rut' not in request:
        invalid = True
    elif 'phone' not in request:
        invalid = True
    elif 'email' not in request:
        invalid = True
    elif 'type' not in request:
        invalid = True

    return invalid


def verify_xlsx_input(request):
    '''
    Lee la entrada del request de flask y los parametros de entrada de un csv.
    :param request: todo el cuerpo del request de flask.
    :return:
        succes: 0,
        fail: json error
    '''
    statusCode = 400
    if 'file' not in request.files:
        desc = "falta el archivo 'file'"
        return {"codigo": statusCode, "glosa": desc}
    fileup = request.files['file']
    if not fileup:
        desc = 'no se subio el archivo correctamente'
        return {"codigo": statusCode, "glosa": desc}
    # validacion de la extencion de archivo
    extension = fileup.filename.split('.')
    if extension[0] == '':
        desc = 'archivo vacio'
        return {"codigo": statusCode, "glosa": desc}
    if extension[1] != 'xlsx':
        desc = 'formato invalido'
        return {"codigo": statusCode, "glosa": desc}
    return 0


def verify_msg_input(request):
    '''
    Lee la entrada del request de flask y los parametros de entrada para conversation.
    :param request: todo el cuerpo del request de flask.
    :return:
        succes: 0
        fail: json: message
    '''
    if request.headers['Content-Type'] == 'application/json':
        # json debe tener llave 'msg'.
        if 'msg' not in request.json.keys():
            # TODO: return appropiate status codes.
            return response_formats.free_output_format(200, {"message": "json missing 'msg'."})

        # el valor de 'msg' no puede ser bool, int o float
        # (solo str, unicode(?), y None- o null en json.
        if isinstance(request.json['msg'], (bool, int, float)):
            return response_formats.free_output_format(200, {"message": "Invalid value type for 'msg'."})

        # json debe tener llave 'cid'.
        if 'cid' not in request.json.keys():
            return response_formats.free_output_format(200, {"message": "json missing 'cid'."})

        # 'cid' no puede ser bool, int o float, solo str, unicode(?) o None.
        if isinstance(request.json['cid'], (bool, int, float)):
            return response_formats.free_output_format(200, {"message": "Invalid value type for 'cid'."})

        # si 'msg' es str o unicode, no puede ser vacío o solo whitespace.
        if isinstance(request.json['msg'], (str)):
            if not request.json['msg'].strip():
                return response_formats.free_output_format(200, {"message": "'msg' is an empty or whitespace string."})

        # si 'cid' es str o unicode, no puede ser vacío o solo whitespace.
        if isinstance(request.json['cid'], (str)):
            if not request.json['cid'].strip():
                return response_formats.free_output_format(200, {"message": "'cid' is an empty or whitespace string."})

        # si 'cid' tiene un valor, 'msg' no puede ser null.
        if request.json['cid'] is not None:
            if request.json['msg'] is None:
                return response_formats.free_output_format(200, {"message": "'msg' can only be null if 'cid' is null."})

            # 'cid' id tiene que tener un patrón igual a los conversation ids de la API.
            # TODO: compile regexp.
            if not re.match(
                    r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
                    request.json['cid']):
                return response_formats.free_output_format(200, {"message": "invalid format for 'cid'."})

            # 'cid' debe estar en la base de datos.
            if len(list(mongo_library.single_find({"interaction.context.conversation_id": request.json['cid']}))) == 0:
                return response_formats.free_output_format(200, {"message": "'cid' not in db.'"})
        return False
    else:
        return response_formats.free_output_format(200, {"message": "415 Unsupported Media Type."})


def validate_header_rep(request_body: dict):
    """
    chequea si los headers entrantes coinciden con los establecidos por COBRANZA
        :param request_body: los header a verificar
    """
    expected_keys = ['RUT', 'NOMBRES', 'APELLIDO_PATERNO', 'APELLIDO_MATERNO', 'ESPECIALISTA', 'TERRITORIO', 'LINEA']
    is_missing_fields, missing_fields = validator.validate_headers(request_body, expected_keys)
    if is_missing_fields:
        error_msg = f'Error en parametros, faltan los siguientes campos: {missing_fields}'
        return {'errors': error_msg}
    return None


def validate_header_los_heroes(request_body: dict):
    """
    chequea si los headers entrantes coinciden con los establecidos por los heroes
        :param request_body: los header a verificar
    """
    expected_keys = [
        'PERFIL',
        'REGION',
        'PROVINCIA',
        'COMUNA',
        'CATEGORIA',
        'SUBCATEGORIA',
        'RUT EMPRESA',
        'DV EMPRESA',
        'RAZON SOCIAL',
        'NOMBRE',
        'DESCRIPCION',
        'INFORMACION_BENEFICIO',
        'CONDICION_BENEFICIO',
        'RANKING',
        'DIRECCION',
        'TELEFONO',
        'STATUS',
        'LINK'
    ]
    is_missing_fields, missing_fields = validator.validate_headers(request_body, expected_keys)
    if is_missing_fields:
        error_msg = f'Error en parametros, faltan los siguientes campos: {missing_fields}'
        return {'errors': error_msg}
    return None


def validate_header_producto(request_body: dict):
    """
    chequea si los headers entrantes coinciden con los establecidos por COBRANZA
        :param request_body: los header a verificar
    """
    expected_keys = ['CODIGO_PRODUCTO', 'NOMBRE_PRODUCTO', 'CODIGO_PRESENTACION', 'NOMBRE_PRESENTACION', 'CODIGO_MERCADO', 'NOMBRE_MERCADO']
    is_missing_fields, missing_fields = validator.validate_headers(request_body, expected_keys)
    if is_missing_fields:
        error_msg = f'Error en parametros, faltan los siguientes campos: {missing_fields}'
        return {'errors': error_msg}
    return None


def validate_header_kardex(request_body: dict):
    """
    chequea si los headers entrantes coinciden con los establecidos por COBRANZA
        :param request_body: los header a verificar
    """
    expected_keys = [
        'RUT',
        'NOMBRES',
        'APELLIDO_PATERNO',
        'APELLIDO_MATERNO',
        'ESPECIALIDAD',
        'SUB_ESPECIALIDAD',
        'CLASIFICACION',
        'CELULAR',
        'EMAIL',
        'TIPO_CONSULTA',
        'INSTITUCION',
        'FONO_CONSULTA',
        'PAIS',
        'REGION',
        'CIUDAD',
        'COMUNA',
        'PISO',
        'OFICINA',
        'NUMERO',
        'CALLE',
        'CODIGO_POSTAL',
        'VALOR_CONSULTA',
        'HORARIO_LUNES',
        'HORARIO_MARTES',
        'HORARIO_MIERCOLES',
        'HORARIO_JUEVES',
        'HORARIO_VIERNES',
        'HORARIO_SABADO',
        'CODIGO_TERRITORIO'
    ]

    is_missing_fields, missing_fields = validator.validate_headers(request_body, expected_keys)
    if is_missing_fields:
        error_msg = f'Error en parametros, faltan los siguientes campos: {missing_fields}'
        return {'errors': error_msg}
    return None
