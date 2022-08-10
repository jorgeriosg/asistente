#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys

from werkzeug.utils import secure_filename
from random import randint

MongoURI = 'mongodb://jmujica:Jito2040..@cognitiva-dev-shard-00-00-d21dy.mongodb.net:27017,cognitiva-dev-shard-00-01-d21dy.mongodb.net:27017,cognitiva-dev-shard-00-02-d21dy.mongodb.net:27017/etika_data_dev?ssl=true&replicaSet=cognitiva-dev-shard-0&authSource=admin'
DB_name = 'etika_mantenedor_dev'


def validador_beneficios_xlsl(values, row,categoria,subcategoria,provincia,comuna,nombre):
    """
    valida una fila de strings de valores asociados a un csv de cobranzas
        :param values: lista con los valores por columna
        :param row: fila implicada del archivo csv:
    """

    errors = []
    if len(values) == 18:
        PERFIL = values[0]
        REGION = values[1].upper()
        PROVINCIA = values[2].upper()
        COMUNA = values[3].upper()
        CATEGORIA = values[4].upper()
        SUBCATEGORIA = values[5].upper()
        RUT_EMPRESA = values[6]
        DV_EMPRESA = values[7]
        RAZON_SOCIAL = values[8]
        NOMBRE = values[9]
        DESCRIPCION = values[10]
        INFORMACION_BENEFICIO = values[11]
        CONDICION_BENEFICIO = values[12]
        RANKING = values[13]
        DIRECCION = values[14]
        TELEFONO = values[15]
        STATUS = values[16]
        LINK = values[17]

        if not (PERFIL):
            errors.append(' PERFIL VACIO ')
        if not (REGION):
            errors.append('| REGION VACIO')
        if not (PROVINCIA):
            errors.append('| PROVINCIA VACIO')
        if not (COMUNA):
            errors.append('| COMUNA VACIO')
        if not (CATEGORIA):
            errors.append('| CATEGORIA VACIO')
        if not (SUBCATEGORIA):
            errors.append('| SUBCATEGORIA VACIO')
        if not (NOMBRE):
            errors.append('| NOMBRE VACIO')
        if not (DESCRIPCION):
            errors.append('| DESCRIPCION VACIO')
        if not (INFORMACION_BENEFICIO):
            errors.append('| INFORMACION VACIO')
        if not (CONDICION_BENEFICIO):
            errors.append('| CONDICION VACIO')
        if not (RANKING):
            errors.append('| RANKING VACIO')
        if not (DIRECCION):
            errors.append('| DIRECCION VACIO')
        if not (STATUS):
            errors.append('| STATUS VACIO')
        if not (LINK):
            errors.append('| LINK VACIO')
        if not (validador_values(PROVINCIA,provincia)):
            errors.append('| PROVINCIA INVALIDO')
        if not (validador_values(COMUNA,comuna)):
            errors.append('| COMUNA INVALIDO')
        if not (validador_values(CATEGORIA,categoria)):
            errors.append('| CATEGORIA INVALIDO')
        if not (validador_values(SUBCATEGORIA,subcategoria)):
            errors.append('| SUBCATEGORIA INVALIDO')
        if not (validador_values(NOMBRE,nombre)):
            errors.append('| NOMBRE INVALIDO')
    else:
        errors.append("| NUMERO DE COLUMNAS INVALIDOS")
    if errors:
        errors = ''.join(errors)
        if errors[0] == "|":
            errors = errors[1:]
        return {'errors': "Fila:" + str(row) + "|" + errors}

    return None


def validador_beneficios(values, categoria, subcategoria, provincia, comuna,nombre):
    """
    valida una fila de strings de valores asociados a un csv de cobranzas
        :param values: lista con los valores por columna
        :param row: fila implicada del archivo csv:
    """
    row = 0
    'PERFIL', 'REGION', 'PROVINCIA', 'COMUNA', 'CATEGORIA',
    'SUBCATEGORIA', 'RUT EMPRESA', 'DV EMPRESA', 'RAZON SOCIAL',
    'NOMBRE', 'DESCRIPCION', 'INFORMACION_BENEFICIO',
    'CONDICION_BENEFICIO', 'RANKING', 'DIRECCION', 'TELEFONO',
    'STATUS', 'LINK', 'FECHA ACTUALIZACION'
    errors = []

    PERFIL1 = values['PERFIL']
    REGION1 = values['REGION']
    PROVINCIA1 = values['PROVINCIA']
    COMUNA1 = values['COMUNA']
    CATEGORIA1 = values['CATEGORIA']
    SUBCATEGORIA1 = values['SUBCATEGORIA']
    RUT_EMPRESA1 = values['RUT EMPRESA']
    DV_EMPRESA1 = values['DV EMPRESA']
    RAZON_SOCIAL1 = values['RAZON SOCIAL']

    NOMBRE1 = values['NOMBRE']
    DESCRIPCION1 = values['DESCRIPCION']
    INFORMACION_BENEFICIO1 = values['INFORMACION_BENEFICIO']
    #print INFORMACION_BENEFICIO1
    CONDICION_BENEFICIO1 = values['CONDICION_BENEFICIO']
    RANKING1 = values['RANKING']
    DIRECCION1 = values['DIRECCION']
    TELEFONO1 = values['TELEFONO']
    STATUS1 = values['STATUS']
    LINK1 = values['LINK']

    for i, PERFIL in enumerate(PERFIL1):
        if not (PERFIL):
            errors.append('FILA:{} | PERFIL VACIO'.format(i+2))
    for i, REGION in enumerate(REGION1):
        if not (REGION):
            errors.append('FILA:{} | REGION VACIA'.format(i + 2))
    for i, PROVINCIA in enumerate(PROVINCIA1):
        if not (PROVINCIA):
            errors.append('FILA:{} | PROVINCIA VACIA'.format(i + 2))
    for i, COMUNA in enumerate(COMUNA1):
        if not (COMUNA):
            errors.append('FILA:{} | COMUNA VACIA'.format(i + 2))
    for i, CATEGORIA in enumerate(CATEGORIA1):
        if not (CATEGORIA):
            errors.append('FILA:{} | CATEGORIA VACIA'.format(i + 2))
    for i, SUBCATEGORIA in enumerate(SUBCATEGORIA1):
        if not (SUBCATEGORIA):
            errors.append('FILA:{} | SUBCATEGORIA VACIA'.format(i + 2))
    '''
    for NOMBRE in NOMBRE1:
        if not (NOMBRE):
            errors.append('| NOMBRE VACIO')
    for DESCRIPCION in DESCRIPCION1:
        if not (DESCRIPCION):
            errors.append('| DESCRIPCION VACIO')
    for i,INFORMACION_BENEFICIO in enumerate(INFORMACION_BENEFICIO1):
        if (type(INFORMACION_BENEFICIO) == float):
            1
        elif (type(INFORMACION_BENEFICIO) == int):
            1
        elif (type(INFORMACION_BENEFICIO) == unicode):
            1
        elif (type(INFORMACION_BENEFICIO) == str):
            1
        else:
            errors.append(' {}| INFORMACION VACIO {}'.format(i, INFORMACION_BENEFICIO))

    for i,CONDICION_BENEFICIO in enumerate(CONDICION_BENEFICIO1):
        if (type(CONDICION_BENEFICIO) == float):
            1
        elif (type(CONDICION_BENEFICIO) == int):
            1
        elif (type(CONDICION_BENEFICIO) == unicode):
            1
        elif (type(INFORMACION_BENEFICIO) == str):
            1
        else:
            errors.append(' {}| CONDICION VACIO {}'.format(i, CONDICION_BENEFICIO))
    #for CONDICION_BENEFICIO in CONDICION_BENEFICIO1:
    #    if not (CONDICION_BENEFICIO):
    #        errors.append('| CONDICION VACIO')

    for i,RANKING in enumerate(RANKING1):
        if (type(RANKING) == float):
            1
        elif (type(RANKING) == int):
            1
        elif (type(RANKING) == unicode):
            1
        elif (type(INFORMACION_BENEFICIO) == str):
            1
        else:
            errors.append(' {}| RANKING VACIO {}'.format(i, RANKING))
    #for RANKING in RANKING1:
    #    if not (RANKING):
    #        errors.append('| RANKING VACIO')
    for DIRECCION in DIRECCION1:
        if not (DIRECCION):
            errors.append('| DIRECCION VACIO')
    for STATUS in STATUS1:
        if not (STATUS):
            errors.append('| STATUS VACIO')
    for LINK in LINK1:
        if not (LINK):
            errors.append('| LINK VACIO')
    '''
    """
    for PERFIL in PERFIL1:
        if not (validador_values(PROVINCIA,provincia)):
            errors.append('| PROVINCIA INVALIDO')
    for PERFIL in PERFIL1:
        if not (validador_values(COMUNA,comuna)):
            errors.append('| COMUNA INVALIDO')
    for PERFIL in PERFIL1:
        if not (validador_values(CATEGORIA,categoria)):
            errors.append('| CATEGORIA INVALIDO')
    for PERFIL in PERFIL1:
        if not (validador_values(SUBCATEGORIA,subcategoria)):
            errors.append('| SUBCATEGORIA INVALIDO')
    for PERFIL in PERFIL1:
        if not (validador_values(NOMBRE,nombre)):
            errors.append('| NOMBRE INVALIDO')
            """
    if errors:
        #errors = ''.join(errors)
        #if errors[0] == "|":
        #    errors = errors[1:]
        #return {'errors': "Fila:" + str(row) + "|" + errors}
        return errors

    return None


def validador_values(csv_value,bd_value):
    if csv_value != '':
        arr = []
        if csv_value not in bd_value:
            arr.append(1)
        if arr==[]:
            return True
        else:
            return False
    else:
        return True


def validate_mercado_in_producto(mercado,mercados):
    if mercado != '':
        #mercado = mercados.split(',')
        arr = []
        for codigo in mercado:
            if codigo not in mercados[1]:
                arr.append(1)
        if arr==[]:
            return True
        else:
            return False
    else:
        return True


def validate_territorios_in_array(territorio,territorios):
    if territorio != '':
        territorio = territorio.split(',')
        arr = []
        for codigo in territorio:
            if codigo not in territorios[1]:
                arr.append(1)
        if arr==[]:
            return True
        else:
            return False
    else:
        return True


def validate_lineas_in_array(linea,lineas):
    if linea != '':
        linea = linea.split(',')
        arr = []
        for codigo in linea:
            if codigo not in lineas[1]:
                arr.append(1)
        if arr==[]:
            return True
        else:
            return False
    else:
        return True


def validate_row_kardex(values, row):
    """
    valida una fila de strings de valores asociados a un csv de cobranzas
        :param values: lista con los valores por columna
        :param row: fila implicada del archivo csv:
    """
    errors = []

    if len(values) == 29:
        rut = values[0]
        name = values[1]
        apellidoP = values[2]
        apellidoM = values[3]
        especialidad = values[4]
        subEspecialidad = values[5]
        clasificacion = values[6]
        celular = values[7]
        email = values[8]
        tipoConsulta = values[9]
        institucion = values[10]
        fonoConsulta = values[11]
        pais = values[12]
        region = values[13]
        ciudad = values[14]
        comuna = values[15]
        piso = values[16]
        oficina = values[17]
        numero = values[18]
        calle = values[19]
        codigoPostal = values[20]
        valorConsulta = values[21]
        horarioLunes = values[22]
        horarioMartes = values[23]
        horarioMiercoles = values[24]
        horarioJueves = values[25]
        horarioViernes = values[26]
        horarioSabado = values[27]
        codigoTerritorio = values[28]

        if not (Validator.validateRut(rut)):
            errors.append(' RUT INVALIDO (' + rut + ')')

        # if not(Validator.validate_name(name)):
        #     errors.append('|NOMBRE INVALIDO (' + name + ')')

        if not (name):
            errors.append('| NOMBRE VACIO')
        if not (apellidoP):
            errors.append('| APELLIDO PATERNO VACIO')
        if not (apellidoM):
            errors.append('| APELLIDO MATERNO VACIO')
        if not (especialidad):
            errors.append('| ESPECIALIDAD VACIO')
        if not (Validator.validate_name(name)):
            errors.append('| NOMBRE INVALIDO')
        if not (Validator.validate_name(apellidoP)):
            errors.append('| APELLIDO PATERNO INVALIDO')
        if not (Validator.validate_name(apellidoM)):
            errors.append('| APELLIDO MATERNO INVALIDO')
        if not (Validator.validateEmail(email)):
            errors.append('| EMAIL INVALIDO')

    else:
        errors.append("| NUMERO DE COLUMNAS INVALIDOS")

    if errors:
        errors = ''.join(errors)
        if errors[0] == "|":
            errors = errors[1:]
        return {'errors': "Fila:" + str(row) + "|" + errors}

    return None


def validate_row_producto(values, row, producto, mercado):
    """
    valida una fila de strings de valores asociados a un csv de cobranzas
        :param values: lista con los valores por columna
        :param row: fila implicada del archivo csv:
    """
    errors = []

    if len(values) == 6:
        codigo_producto = values[0]
        nombre_producto = values[1]
        codigo_presentacion = values[2]
        nombre_presentacion = values[3]
        codigo_mercado = values[4]
        nombre_mercado = values[5]

        # print codigo_mercado
        # print mercado[1]
        if not (codigo_producto):
            errors.append(' CODIGO PRODUCTO VACIO (' + codigo_producto + ')')
        if not (nombre_producto):
            errors.append('| NOMBRE PRODUCTO VACIO')
        if not (codigo_presentacion):
            errors.append('| CODIGO PRESENTACION VACIO')
        if not (nombre_presentacion):
            errors.append('| NOMBRE PRESENTACION VACIO')
        if not (codigo_mercado):
            errors.append('| CODIGO MERCADO VACIO')
        if not (nombre_mercado):
            errors.append('| NOMBRE MERCADO VACIO')
        if not (validate_mercado_in_producto(codigo_mercado,mercado)):
            errors.append('| MERCADO INVALIDO')

    else:
        errors.append("| NUMERO DE COLUMNAS INVALIDOS")

    if errors:
        errors = ''.join(errors)
        if errors[0] == "|":
            errors = errors[1:]
        return {'errors': "Fila:" + str(row) + "|" + errors}

    return None


def validate_row_representante(values, row,territorios,lineas,ruts):
    """
    valida una fila de strings de valores asociados a un csv de cobranzas
        :param values: lista con los valores por columna
        :param row: fila implicada del archivo csv:
    """
    errors = []

    if len(values) == 7:
        rut = values[0]
        name = values[1]
        apellidoP = values[2]
        apellidoM = values[3]
        especialista = values[4]
        territorio = values[5]
        linea = values[6]

        if not (Validator.validateRut(rut)):
            errors.append(' RUT INVALIDO (' + rut + ')')

        if rut in ruts[1]:
            errors.append(' USUARIO YA EXISTE (' + rut + ')')
        # if not(Validator.validate_name(name)):
        #     errors.append('|NOMBRE INVALIDO (' + name + ')')

        if not (name):
            errors.append('| NOMBRE VACIO')
        if not (apellidoP):
            errors.append('| APELLIDO PATERNO VACIO')
        if not (apellidoM):
            errors.append('| APELLIDO MATERNO VACIO')
        if not (especialista):
            errors.append('| ESPECIALISTA VACIO')
        if not (Validator.validate_name(name)):
            errors.append('| NOMBRE INVALIDO')
        if not (Validator.validate_name(apellidoP)):
            errors.append('| APELLIDO PATERNO INVALIDO')
        if not (Validator.validate_name(apellidoM)):
            errors.append('| APELLIDO MATERNO INVALIDO')
        if not (validate_territorios_in_array(territorio,territorios)):
            errors.append('| TERRITORIOS INVALIDOS')
        if not (validate_lineas_in_array(linea,lineas)):
            errors.append('| LINEAS INVALIDAS')

    else:
        errors.append("| NUMERO DE COLUMNAS INVALIDOS")

    if errors:
        errors = ''.join(errors)
        if errors[0] == "|":
            errors = errors[1:]
        return {'errors': "Fila:" + str(row) + "|" + errors}

    return None


def csv_producto(MongoURI, DB_name, logs_collection, auditoria_collection, producto_col, Full_PATH_logs, User,fileup):
    rand = randint(1,1000)
    ret_error = []
    filename = secure_filename(fileup.filename)
    route='/Users/sebastianhe/Documents/mantenedor_etika/BACKEND/'
    #route='/var/www/apps/mantenedor_etika/BACKEND/'
    fileup.save(route+str(rand)+filename)
    file = route+str(rand)+filename
    csv_file = open(file, 'rU')
    header = csv_file.readline().strip().split(';')
    validate_header_result = Validator.validate_header_producto(header)
    if validate_header_result:
        ret_error.append(validate_header_result)
    idx_row = 1
    producto= Mongo_Find_Distinct_Operator(MongoURI,DB_name,'producto',{},'Codigo Territorio')
    mercado= Mongo_Find_Distinct_Operator(MongoURI,DB_name,'mercado',{},'codigoMercado')
    for line in csv_file:
        idx_row += 1
        decode_line = line.decode('utf-8')
        row = decode_line.strip().split(';')
        validate_row_result = validate_row_producto(row, idx_row,producto,mercado)
        if validate_row_result:
            ret_error.append(validate_row_result)
    csv_file.close()

    if ret_error != []:
        os.remove(file)
        return str(ret_error)
    else:
        csv_file = open(file, 'rU')
        header = csv_file.readline().strip().split(';')
        for line in csv_file:
            idx_row += 1
            decode_line = line.decode('utf-8')
            row = decode_line.strip().split(';')

            codigo_producto = values[0]
            nombre_producto = values[1]
            codigo_presentacion = values[2]
            nombre_presentacion = values[3]
            codigo_mercado = values[4]
            nombre_mercado = values[5]

            contrasena = "test"
            User = "test"
            role = "1"

            # Se realiza la funcion que hace la magia

            #output_code = AddNewRepresentanteCSV(MongoURI, DB_name, logs_collection, auditoria_collection, representantes_collection, login_col, Full_PATH_logs, rut_representante, apellido_paterno, apellido_materno, nombres, es_especialista, idrutusuario, contrasena, User, role,territorio, linea)
        os.remove(file)
        return 200


def csv_representante(MongoURI, DB_name, logs_collection, auditoria_collection, representantes_collection,login_col, Full_PATH_logs, User,fileup):
    rand = randint(1,1000)
    ret_error = []
    filename = secure_filename(fileup.filename)
    route='/Users/sebastianhe/Documents/mantenedor_etika/BACKEND/'
    route='/var/www/apps/mantenedor_etika/BACKEND/'
    fileup.save(route+str(rand)+filename)
    file = route+str(rand)+filename
    csv_file = open(file, 'rU')
    header = csv_file.readline().strip().split(';')
    validate_header_result = Validator.validate_header_rep(header)
    if validate_header_result:
        ret_error.append(validate_header_result)
    idx_row = 1
    territorios= Mongo_Find_Distinct_Operator(MongoURI,DB_name,'territorio',{},'Codigo Territorio')
    lineas= Mongo_Find_Distinct_Operator(MongoURI,DB_name,'fuerzaVenta',{},'idFzaVenta')
    ruts = Mongo_Find_Distinct_Operator(MongoURI,DB_name,'representante',{},'RUT Representante')
    for line in csv_file:
        idx_row += 1
        decode_line = line.decode('utf-8')
        row = decode_line.strip().split(';')
        validate_row_result = validate_row_representante(row, idx_row,territorios,lineas,ruts)
        if validate_row_result:
            ret_error.append(validate_row_result)
    csv_file.close()

    if ret_error != []:
        os.remove(file)
        return str(ret_error)
    else:
        csv_file = open(file, 'rU')
        header = csv_file.readline().strip().split(';')
        for line in csv_file:
            idx_row += 1
            decode_line = line.decode('utf-8')
            row = decode_line.strip().split(';')

            rut_representante = row[0]
            idrutusuario = row[0]
            nombres = row[1]
            apellido_paterno = row[2]
            apellido_materno = row[3]
            es_especialista = row[4]
            territorio = row[5].split(',')
            linea = row[6].split(',')
            if territorio == '':
                territorio = []

            if linea == '':
                linea = []

            contrasena = "test"
            User = "test"
            role = "1"

            # Se realiza la funcion que hace la magia

            output_code = AddNewRepresentanteCSV(MongoURI, DB_name, logs_collection, auditoria_collection, representantes_collection, login_col, Full_PATH_logs, rut_representante, apellido_paterno, apellido_materno, nombres, es_especialista, idrutusuario, contrasena, User, role,territorio, linea)

        os.remove(file)
        return 200
