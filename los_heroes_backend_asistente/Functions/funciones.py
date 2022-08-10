#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import pytz
import pymongo
import pymysql
import losheroes_settings as cf
import pandas
import os
import random
import codecs
import base64
import sys
import losheroes_settings as config

from mods import datadog as dg
from mods.watson_library import WatsonV1
from mods.SqlConnection import SqlConnection
from Functions.validator import Validator
from Functions.csv_functions import *

# def load_src(name, fpath):
#     import os, imp
#     return imp.load_source(name, os.path.join(os.path.dirname(__file__), fpath))

# SRC_LD = "/Users/oscarjaradiaz/Desktop/Cognitiva/LOS-HEROES/BACK_ASISTENTE/losheroes_settings.py"
# SRC_LD = "/Users/sebastianhe/Cognitiva/heroes_2019/LOS-HEROES/BACK_ASISTENTE/losheroes_settings.py"
# SRC_LD = "/var/www/apps_asistente/losheroes_settings.py"
# # SRC_LD = '/Users/ricardosalinas/Documents/cognitiva/LOS-HEROES/BACK_ASISTENTE/losheroes_settings.py'
# load_src("config", SRC_LD)

SERVER_PATH = '/tmp/'


def mysql_Connect():

    return pymysql.connect(
        host=cf.MySQL_host,
        user=cf.MySQL_user,
        password=cf.MySQL_pass,
        db=cf.DB_name,
        charset='utf8',
        use_unicode=True,
        cursorclass=pymysql.cursors.DictCursor
    )


def get_saludo():
    query = """SELECT me.mensaje as mensaje
        FROM mensajes as me
        LEFT JOIN parametros p on me.id_cliente = p.id_cliente
        where me.tipo = IF(p.pide_rut is true, 'saludo_rut', 'saludo') and me.id_cliente = 1
        ORDER BY RAND() LIMIT 1;"""
    connection = SqlConnection()
    try:
        message_found = connection.find(query)
        if isinstance(message_found, list) and len(message_found) > 0:
            return message_found[0]['mensaje']
    except Exception as unknown_exception:
        dg.log_datadog(f'Unknown exeption at get_saludo: {unknown_exception}')
    finally:
        connection.close_connection()
    return 'Aun no tiene configurado un mensaje de bienvenida!'


interactions = config.interactions
beneficios = config.beneficios
valoracion = config.valoracion

prestadores_especiales = [
    "Concierto",
    "Asignación de Matrícula",
    "Bodas de Oro",
    "Bodas de Plata",
    "Fallecimiento",
    "Natalidad o adopción",
    "Nupcialidad o AUC",
    "Estímulo escolar",
    "Conciertos",
    "Eventos temáticos",
    "Paseos",
    "Talleres"
]


def busquedaCentrosMedicos(comuna, tipo):
    if tipo == "EXAMENES":
        query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "COMUNA": comuna, "SUBCATEGORIA": {"$in": ["CENTROS MEDICOS CON BONIFICACION", "CENTROS MEDICOS", "LABORATORIO"]}}
    else:
        query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "COMUNA": comuna, "SUBCATEGORIA": {"$in": ["CENTROS MEDICOS CON BONIFICACION", "CENTROS MEDICOS"]}}
    ord = 'RANKING', 1
    validador = []

    out = busquedaMongoGenerica(query)
    if out != []:

        texto = "Los centros medicos con beneficios en {} son <br><br>".format(comuna.capitalize())
        for x, y in enumerate(out):
            if y['NOMBRE'] not in validador:
                texto += '{} '.format(str(out[x]['NOMBRE']))
                validador.append(y['NOMBRE'])
                if out[x]['LINK']:
                    texto += ' para más información haz click <a href={} target="_blank">aquí</a><br><br>'.format(out[x]['LINK'])
                else:
                    texto += ' <br><br>'

        ''' solo un output '''
        if len(validador) == 1:
            info = 'N/A'
            if out[x]['INFORMACION_BENEFICIO'] is not None:
                info = out[x]['INFORMACION_BENEFICIO']
                texto = 'Los centros medicos con beneficios en {} son <br><br>{}, {} '.format(comuna.capitalize(), str(out[0]['NOMBRE']), info)
                if out[0]['LINK']:
                    texto += 'para más información haz click <a href={} target="_blank">aquí</a><br><br>'.format(out[0]['LINK'])
                else:
                    texto += '<br><br>'
        texto = str(texto + textoSalida())
    else:
        texto = 'No tenemos beneficios de centros medicos para {}<br><br>{}'.format(comuna.capitalize(), textoSalida())
    return texto


def limpiaContexto(cid):
    last_interaction = config.interactions.find({
        'interaction.context.conversation_id': cid,
        'workspace_type': 'business'
    }).sort('datetime', -1).limit(1)

    last_interaction = list(last_interaction)[0]
    context = last_interaction['interaction']['context']
    context['workspace_type'] = 'business'
    context['categoria'] = None
    context['subcategoria'] = None
    # context['modulo'] = request.json['modulo']
    watsonV1 = WatsonV1()
    contexto = {}
    conv_response = watsonV1.watson_call(input='cl34r', context=context)

    document = {
        'interaction': conv_response,
        'datetime': datetime.datetime.utcnow(),
        'workspace_type': 'business'
    }
    config.interactions.insert_one(document)


def todosLosParametros(provincia, comuna, categoria, subcategoria, cid=None):
    '''
    busca en la base de datos cuando existen los 4 parametros de busqueda entregando los beneficios.
    :param provincia:
    :param comuna:
    :param categoria:
    :param subcategoria:
    :return:
        succes: string.
    '''

    query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "PROVINCIA": {"$in": ["NACIONAL", provincia]}, "COMUNA": {"$in": ["NACIONAL", comuna]}, "CATEGORIA": categoria, "SUBCATEGORIA": subcategoria}
    if (subcategoria == "DENTAL" or subcategoria == "CENTROS MÉDICOS"):
        if (provincia == "SANTIAGO" or provincia == "CHACABUCO" or provincia == "CORDILLERA" or provincia == "MAIPO" or provincia == "MELIPILLA" or provincia == "TALAGANTE"):
            query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "PROVINCIA": {"$in": ["NACIONAL", "SANTIAGO", "CHACABUCO", "CORDILLERA", "MAIPO", "MELIPILLA", "TALAGANTE"]}, "CATEGORIA": categoria, "SUBCATEGORIA": subcategoria}
    ord = 'RANKING'
    out = busquedaMongo(query, ord)

    texto = ''
    validador = []
    benef = textoBeneficios(comuna, categoria, subcategoria)
    ''' si retorna valor la query '''
    if out != []:
        if benef:
            texto = '{}<br><br>'.format(benef)
        ''' multiples output '''

        for x, y in enumerate(out):
            # print y['NOMBRE']
            if y['NOMBRE'] not in validador:
                texto += '{}'.format(str(out[x]['NOMBRE']))
                validador.append(y['NOMBRE'])
                if out[x]['LINK']:
                    texto += ' para más información haz click <a href={} target="_blank">aquí</a><br><br>'.format(out[x]['LINK'])
                else:
                    texto += ' <br><br>'

        ''' solo un output '''
        if len(validador) == 1:
            info = 'N/A'
            if out[x]['INFORMACION_BENEFICIO'] is not None:
                info = out[x]['INFORMACION_BENEFICIO']
            # texto = '{} <br><br>{}, {} para más información haz click <a href={} target="_blank">aquí</a><br><br>'.format(benef, str(out[0]['NOMBRE']).encode('utf-8'), info, out[0]['LINK'])
            if str(out[0]['NOMBRE']) in prestadores_especiales:
                if benef:
                    texto = '{} <br><br> {}'.format(
                        benef, info)
                else:
                    texto = info
            else:
                if benef:
                    texto = '{} <br><br>{}, {}'.format(
                        benef, str(out[0]['NOMBRE']), info)
                else:
                    texto = '{}, {}'.format(
                        str(out[0]['NOMBRE']), info)

            if out[0]['LINK']:
                texto += ' para más información haz click <a href={} target="_blank">aquí</a><br><br>'.format(
                    out[0]['LINK'])
            else:
                texto += ' <br><br>'
        if cid:
            limpiaContexto(cid)
        texto = str(texto + textoSalida())
        ''' si no encuentra por comuna, busca por provincia '''
    else:
        #texto = 'Lo sentimos, no existen beneficios de {} en la comuna {}.<br><br>'.format(subcategoria.capitalize(), comuna.capitalize())
        texto = busquedaProvinciaCategoriaSubcategoria(provincia, categoria, subcategoria, comuna)
    return texto


def busquedaProvinciaComuna(provincia, comuna, categoria):

    query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "PROVINCIA": {"$in": ["NACIONAL", provincia]}, "COMUNA": {"$in": ["NACIONAL", comuna]}}
    dist = 'CATEGORIA'
    out = busquedaMongoDistinct(query, dist)
    if out != []:
        if categoria == '':
            out = sorted(out)
            texto = '{}'.format(textoNoCategoria())
            tex = '/'.join(out).title()
            texto += '%%%' + tex + '/volver%%%'
        else:
            texto = 'Lo sentimos, no existen beneficios asociados. <br><br> Pero te invitamos a ver nuestros otros beneficios en {}<br><br>{}'.format(', '.join(out).title(), textoSalida())
    else:
        if categoria == '':
            texto = '{} <br><br>{}'.format(subcategoriaText(categoria, comuna), textoSalida())
        else:
            texto = '{} <br><br>{}'.format(subcategoriaText(categoria, comuna), textoSalida())
    return texto


def busquedaProvinciaCategoriaSubcategoria(provincia, categoria, subcategoria, comuna):
    '''
    busca en la base de datos cuando existen los 3 parametros de busqueda entregando los beneficios.
    :param provincia:
    :param comuna:
    :param categoria:
    :param subcategoria:
    :return:
        succes: string.
    '''
    query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "PROVINCIA": {"$in": ["NACIONAL", provincia]}, "CATEGORIA": categoria, "SUBCATEGORIA": subcategoria}
    ord = 'RANKING'
    out = busquedaMongo(query, ord)

    noComuna = textNoComunaSiProvincia(provincia, comuna, subcategoria)
    texto = ''  # '{}'.format(noComuna)

    validador = []
    '''en caso de encontrar resultados'''
    if out != []:

        if comuna == '':
            texto += 'los beneficios en {} son: <br><br>'.format(subcategoria.capitalize())
        # else:
            # texto += 'los beneficios en {} son: <br><br>'.format(subcategoria.capitalize())
        for x, y in enumerate(out):

            if y['NOMBRE'] not in validador:
                # info = valor[3].capitalize()
                texto += '{}'.format(
                    str(out[x]['NOMBRE']))
                validador.append(y['NOMBRE'])
                if out[x]['LINK']:
                    texto += ' para más información haz click <a href={} target="_blank">aquí</a><br><br>'.format(
                        out[x]['LINK'])
                else:
                    texto += ' <br><br>'
            '''solo un output'''
        if len(validador) == 1:
            info = 'N/A'
            if out[x]['INFORMACION_BENEFICIO'] is not None:
                info = out[x]['INFORMACION_BENEFICIO']
                if str(out[0]['NOMBRE']) in prestadores_especiales:
                    texto = '{}'.format(info)
                else:
                    texto = '{}, {}'.format(
                        str(out[x]['NOMBRE']), info)
                if out[0]['LINK']:
                    texto += ' para más información haz click <a href={} target="_blank">aquí</a>'.format(out[0]['LINK'])
                else:
                    texto += ' '

        if comuna == '':
            texto = '{}<br><br>{}<br><br>{}'.format(noComuna, texto, textoSalida())
        else:
            head = textNoComunaSiProvincia(provincia, comuna, subcategoria)
            texto = '{}<br><br>{}<br><br>{}'.format(noComuna, texto, textoSalida())

    else:

        '''si no, busca las categorias disponibles'''
        texto = busquedaProvinciaComunaCategoria(provincia, comuna, categoria, subcategoria)

    return texto


def busquedaProvinciaComunaCategoria(provincia, comuna, categoria, subcategoria, cid=None):
    query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "PROVINCIA": {"$in": ["NACIONAL", provincia]}, "COMUNA": {"$in": ["NACIONAL", comuna]}, "CATEGORIA": categoria}
    dist = 'SUBCATEGORIA'
    out = busquedaMongoDistinct(query, dist)
    texto = textoSubcategorias()

    if out != []:
        if len(out) == 1:
            subcategoria = out[0]
            texto = todosLosParametros(provincia, comuna, categoria, subcategoria, cid)
        else:
            if subcategoria != '':
                tx = 'Pero en la provincia de {} los beneficios de {} son los siguientes: '.format(
                    provincia.capitalize(), categoria.capitalize()
                )
                txt = ', '.join(out)
                texto = '{}{}<br><br>{}'.format(tx, txt.capitalize(), textoSalida())
            else:
                out = sorted(out)
                tex = '/'.join(out)
                texto += '%%%{}/volver%%%'.format(tex.title())
    else:
        texto = busquedaProvinciaComuna(provincia, comuna, categoria)
    return texto


def busquedaProvinciaComunaNombre(provincia, comuna, nombre):
    query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "PROVINCIA": {"$in": ["NACIONAL", provincia]}, "COMUNA": {"$in": ["NACIONAL", comuna]}, "NOMBRE": { "$regex": nombre, "$options":'i' }}  # re.compile(nombre, re.IGNORECASE)}

    out = busquedaMongoGenerica(query)
    texto = 'Los beneficios de {} para la comuna {} son:<br><br>'.format(nombre.title(), comuna.title())
    validador = []
    if out != []:
        for x, y in enumerate(out):
            if y['NOMBRE'] not in validador:
                texto += '{}'.format(str(out[x]['NOMBRE']))
                validador.append(y['NOMBRE'])
                if out[x]['LINK']:
                    texto += ' para más información haz click <a href={} target="_blank">aquí</a> </br>'.format(out[x]['LINK'])
                else:
                    texto += ' </br>'
            if len(validador) == 1:
                info = 'N/A'
                if out[x]['INFORMACION_BENEFICIO'] is not None:
                    info = out[x]['INFORMACION_BENEFICIO']
                texto = 'Los beneficios de {} para la comuna {} son:<br><br> {}'.format(
                    nombre.title(), comuna.title(), info)
                if out[x]['LINK']:
                    texto += ' para más información haz click <a href={} target="_blank">aquí</a> </br>'.format(out[x]['LINK'])
                else:
                    texto += ' </br>'
        texto = "{}<br><br>{}".format(texto, textoSalida())
    else:
        texto = '{}<br><br>{}'.format(subcategoriaText(nombre, comuna), textoSalida())
    return texto


def busquedaComunaNombre(comuna, nombre):
    query = {"CARGA_VIGENTE": True, "STATUS": "VIGENTE", "COMUNA": {"$in": ["NACIONAL", comuna]}, "NOMBRE": { "$regex": nombre, "$options":'i' }}   # re.compile(nombre, re.IGNORECASE)}
    out = busquedaMongoGenerica(query)

    texto = 'Las direcciones para {} en la comuna de {} son: <br><br>'.format(nombre.lower(), comuna.title())
    if out != []:
        for i, data in enumerate(out):
            texto += str(data['DIRECCION']).capitalize() + ' '

            if out[0]['LINK']:
                texto += 'Para más información haz click <a href="{}" target="_blank">aquí</a><br><br>'.format(out[0]['LINK'])

        texto = '{}<br><br>{}'.format(texto, textoSalida())

    else:
        texto = 'No encontramos direcciones para el prestador {} en la comuna {}<br><br>{}'.format(nombre.title(), comuna.title(), textoSalida())
    return texto


def identificacionVariables(convResponse):
    '''
    recibe el conversation response filtradro para devolver entidades y valores.
    :param convResponse:
    :return:
        succes: retorna arreglo y valores.
    '''
    var = convResponse  # conv_response['output']['text'][0][3:-1]
    var = var.split(', ')
    entidad = []
    valor = []
    for x, y in enumerate(var):
        entidad.append(y[:y.index(':')])
        valor.append(y[y.index(':') + 2:])
    return entidad, valor

# MONGO


def guardarValoracion(convResponse):
    var = convResponse
    var = var.split(',')
    valoracion.insert({"nodo": var[0].strip(), "valoracion": int(var[1]), "razon": var[2].strip(),
                       "fecha": datetime.datetime.utcnow()})
    textoSalida = 'Muchas gracias. <br><br>'

    return textoSalida


def busquedaMongo(query, ord):
    try:
        out = list(beneficios.find(query).sort(ord, 1))
        err = query
        # print >> sys.stderr, err
        return out
    except Exception as e:
        return str(e)


def busquedaMongoGenerica(query):
    try:
        out = list(beneficios.find(query))
        err = query
        # print >> sys.stderr, err
        return out
    except Exception as e:
        return str(e)


def busquedaMongoDistinct(query, dist, sort=''):
    try:
        if sort == '':
            out = list(beneficios.find(query).distinct(dist))
            err = query
            # print >> sys.stderr, err
            return out
        else:
            out = list(beneficios.find(query).distinct(dist).sort({'$natural': -1}))
            err = query
            # print >> sys.stderr, err
            return out
    except Exception as e:
        return str(e)


# TEXTOS
def textNoComunaSiProvincia(provincia, comuna, subcategoria):
    noComunaSiProvinciaTexto = 'No encontramos resultados para {}, pero dentro de la provincia {} los beneficios en {} son:'.format(
        comuna.title(), provincia.title(), subcategoria.title()), \
        'No contamos con beneficios en {}, pero sí tenemos para Usted, en la provincia {}, los siguientes beneficios de {}:'.format(
            comuna.title(), provincia.title(), subcategoria.title()), \
        'En {} no contamos con beneficios de {}, pero en la provincia {} tenemos lo siguiente:'.format(
            comuna.title(), subcategoria.title(), provincia.title())
    num = randint(0, len(noComunaSiProvinciaTexto) - 1)
    return noComunaSiProvinciaTexto[num]


def textoBeneficios(comuna, categoria, subcategoria):
    siComuna = "Los beneficios de la categoría {}, de {} en {} son:".format(categoria.title(), subcategoria.title(), comuna.title()), \
        "Los beneficios de la categoría {}, de {} que tenemos para usted en {} son:".format(categoria.title(), subcategoria.title(),
                                                                                            comuna.title()), \
        'En {} contamos con los siguientes beneficios de {}, asociados a la categoría {} para usted:'.format(comuna.title(), subcategoria.title(), categoria.title())
    if "bono" in categoria.lower() or "bono" in subcategoria.lower():
        return ""
    num = randint(0, len(siComuna) - 1)
    return siComuna[num]


def textoNoCategoria():
    noCategoria = "Por favor, señale alguna categoría de beneficios sobre la que le gustaría saber más", \
        "Por favor, seleccione la categoría del beneficio por el que quiere consultar"
    num = randint(0, len(noCategoria) - 1)
    return noCategoria[num]


def textoSalida():
    texto = "¿Le puedo ayudar en algo más?", \
            "¿Necesita ayuda en algún otro tema?", \
            "¿Hay algo más en que le pueda ayudar?", \
            "Espero haberle ayudado, ¿hay algo más en que le pueda ayudar?", \
            "Espero haberle ayudado, ¿tiene alguna otra consulta?"
    numeroSalida = randint(0, len(texto) - 1)
    texto = texto[numeroSalida]
    return texto


def textoValoracion():
    texto = "En una escala de 1 a 5, siendo 1 muy malo y 5 excelente. <br><br>¿Cómo valorarías la calidad de la respuesta? %%%1/2/3/4/5%%%"
    return texto


def textoSubcategorias():
    subcat = "Por favor, elija una de las siguientes subcategorías:", \
        "Por favor, seleccione la subcategoría por la que quiere consultar:"
    num = randint(0, len(subcat) - 1)
    texto = subcat[num]
    return texto


def subcategoriaText(categoria, comuna):
    if categoria == '':
        noExiste = 'Lo sentimos, no existen beneficios asociados.', \
            'Lo siento, no contamos con convenios en {}.'.format(comuna.title())
    else:
        noExiste = 'Lo sentimos, no existen beneficios asociados.', \
            'Lo siento, no contamos con convenios de {} en {}.'.format(categoria.title(), comuna.title())
    num = randint(0, len(noExiste) - 1)
    return noExiste[num]


def formateoArregloATexto(convResponse):
    msg = ''
    if not isinstance(convResponse, str):
        for x, y in enumerate(convResponse):
            msg += y
    else:
        msg += convResponse
    msg = ''.join(msg)
    return msg


def insercionMongoQuery(query):
    try:
        beneficios.insert(query)
        return 200
    except Exception as e:
        return 400


def updateMongoQuery(filtro, set):
    try:

        beneficios.update(filtro, {
            '$set': set
        }, upsert=False,
            multi=True
        )
        return 200
    except Exception as e:
        return 400


def roll(fecha):
    '''
    string de fecha de carga de base de datos.
    :param fecha: string datetime utc
    :return:
        succes: json: codigo:200
        fail: json: codigo:400
    '''
    try:
        '''setea todos los registros de la base de datos en false'''
        fecha = str(fecha)
        filtro = {}
        set = {"CARGA_VIGENTE": False}
        updateMongoQuery(filtro, set)
        '''setea los registros que corresponden a la fecha de ingreso como true'''
        filtro = {"FECHA_CARGA": fecha}
        set = {"CARGA_VIGENTE": True}
        updateMongoQuery(filtro, set)
        return {"codigo": 200, "glosa": "se realizo el rollback"}
    except Exception as e:
        cf.datadog_log('/rollback', 'fallo el rollback')
        return {"codigo": 400, "glosa": str(e)}


def arregloFechas():
    '''
    retorna arreglo de fechas de carga de la base de datos.
    :return:
        succes: json: codigo:200
    '''
    query = {}
    dist = 'FECHA_CARGA'
    out = busquedaMongoDistinct(query, dist)
    for x, y in enumerate(out):
        out[x] = str(y)
    out.sort(reverse=True)
    return {"glosa": out, "codigo": 200}


def xlsl_producto(fileup):
    '''
    valida los datos que vienen en el xlsx, y updatea base de datos de beneficios.
    :param fileup: xlsx file
    :return:
        success: 200
        fail: array errores.
    '''
    csv_file = pandas.read_excel(fileup, sheet_name='Hoja1')
    csv_file['RUT EMPRESA'] = csv_file['RUT EMPRESA'].fillna(0)
    csv_file['DV EMPRESA'] = csv_file['DV EMPRESA'].fillna(0)
    csv_file['RANKING'] = csv_file['RANKING'].fillna(99999)
    csv_file = csv_file.fillna('')
    header = csv_file.columns

    ret_error = []

    validate_header_result = Validator.validate_header_los_heroes(header)
    if validate_header_result:
        ret_error.append(validate_header_result)
    idx_row = 1
    categoria = []  # busquedaMongoDistinct({}, 'CATEGORIA')
    subcategoria = []  # busquedaMongoDistinct({}, 'SUBCATEGORIA')
    provincia = []  # busquedaMongoDistinct({}, 'PROVINCIA')
    comuna = []  # busquedaMongoDistinct({}, 'COMUNA')
    nombre = []  # busquedaMongoDistinct({}, 'NOMBRE')

    idx_row += 1
    validate_row_result = False
    validate_row_result = validador_beneficios(csv_file, categoria, subcategoria, provincia, comuna, nombre)  # Validator.validate_row_producto(row, idx_row, producto, mercado)

    if validate_row_result:
        ret_error.extend(validate_row_result)

    if ret_error != []:

        return ret_error
    else:

        filtro = {}  # {}
        set = {"CARGA_VIGENTE": False}
        out = updateMongoQuery(filtro, set)
        # fecha = str(datetime.datetime.utcnow())
        fecha = datetime.datetime.now(pytz.timezone('America/Santiago')).strftime('%Y-%m-%d %H:%M:%S')

        return actualizarBaseBeneficios(csv_file, fecha)
        # return 200


def csv_producto(fileup):
    rand = randint(1, 1000)
    ret_error = []
    filename = secure_filename(fileup.filename)
    route = '/Users/sebastianhe/los_heroes/los-heroes-ap-mw-dev/'
    # route='/var/www/apps/mantenedor_etika/BACKEND/'
    fileup.save(route + str(rand) + filename)
    file = route + str(rand) + filename
    csv_file = open(file, 'rU')

    header = csv_file.readline().strip().replace('\n', '').split(';')

    validate_header_result = Validator.validate_header_los_heroes(header)
    if validate_header_result:
        ret_error.append(validate_header_result)
    idx_row = 1

    categoria = busquedaMongoDistinct({}, 'CATEGORIA')
    subcategoria = busquedaMongoDistinct({}, 'SUBCATEGORIA')
    provincia = busquedaMongoDistinct({}, 'PROVINCIA')
    comuna = busquedaMongoDistinct({}, 'COMUNA')
    nombre = busquedaMongoDistinct({}, 'NOMBRE')

    for line in csv_file:
        idx_row += 1
        decode_line = line.decode('utf-8')
        row = decode_line.strip().split(';')

        validate_row_result = validador_beneficios(row, idx_row, categoria, subcategoria, provincia, comuna, nombre)  # Validator.validate_row_producto(row, idx_row, producto, mercado)

        if validate_row_result:
            ret_error.append(validate_row_result)
    # csv_file.close()
    if ret_error != []:
        os.remove(file)
        return str(ret_error)
    else:
        csv_file.seek(0)
        head = csv_file.readline()

        filtro = {}  # {}
        set = {"CARGA_VIGENTE": False}
        out = updateMongoQuery(filtro, set)
        fecha = str(datetime.datetime.utcnow())
        for line in csv_file:
            decode_line = line.decode('utf-8')
            values = decode_line.strip().split(';')
            actualizarBaseBeneficios(values, fecha)

        csv_file.close()
        os.remove(file)
        return 200


def actualizarBaseBeneficios(values, fecha):
    '''
    lee data frame de pandas y asigna un chunk para insercion a base de datos de beneficios.
    :param values: dataframe de pandas de beneficios.
    :param fecha: string datetimeutc
    :return:
        succes: 200
        fail: 400
    '''

    PERFIL = values['PERFIL']
    REGION = values['REGION']
    PROVINCIA = values['PROVINCIA']
    COMUNA = values['COMUNA']
    CATEGORIA = values['CATEGORIA']
    SUBCATEGORIA = values['SUBCATEGORIA']
    RUT_EMPRESA = values['RUT EMPRESA']
    DV_EMPRESA = values['DV EMPRESA']
    RAZON_SOCIAL = values['RAZON SOCIAL']
    NOMBRE = values['NOMBRE']
    DESCRIPCION = values['DESCRIPCION']
    INFORMACION_BENEFICIO = values['INFORMACION_BENEFICIO']
    # print INFORMACION_BENEFICIO1
    CONDICION_BENEFICIO = values['CONDICION_BENEFICIO']
    RANKING = values['RANKING']
    DIRECCION = values['DIRECCION']
    TELEFONO = values['TELEFONO']
    STATUS = values['STATUS']
    LINK = values['LINK']

    array = []
    for i in range(0, len(PERFIL)):
        if RANKING[i] == '':
            RANKING[i] = 999999
        if CONDICION_BENEFICIO[i] == 0 or CONDICION_BENEFICIO[i] == '0' or CONDICION_BENEFICIO[i] == '#N/A':
            CONDICION_BENEFICIO[i] = ''
        if INFORMACION_BENEFICIO[i] == 0 or INFORMACION_BENEFICIO[i] == '0' or INFORMACION_BENEFICIO[i] == '#N/A':
            INFORMACION_BENEFICIO[i] = ''
        if DESCRIPCION[i] == 0 or DESCRIPCION[i] == '0' or DESCRIPCION[i] == '#N/A':
            DESCRIPCION[i] = ''
        if NOMBRE[i] == 0 or NOMBRE[i] == '0' or NOMBRE[i] == '#N/A':
            NOMBRE[i] = ''
        query = {
            "PERFIL": PERFIL[i].upper(),
            "REGION": REGION[i].upper(),
            "PROVINCIA": PROVINCIA[i].upper(),
            "COMUNA": COMUNA[i].upper(),
            "CATEGORIA": CATEGORIA[i].upper(),
            "SUBCATEGORIA": SUBCATEGORIA[i].upper(),
            "RUT EMPRESA": RUT_EMPRESA[i],
            "DV EMPRESA": DV_EMPRESA[i],
            "RAZON SOCIAL": RAZON_SOCIAL[i],
            "NOMBRE": NOMBRE[i],
            "DESCRIPCION": DESCRIPCION[i],
            "INFORMACION_BENEFICIO": INFORMACION_BENEFICIO[i],
            "CONDICION_BENEFICIO": CONDICION_BENEFICIO[i],
            "RANKING": int(RANKING[i]),
            "DIRECCION": DIRECCION[i],
            "TELEFONO": TELEFONO[i],
            "STATUS": STATUS[i],
            "LINK": LINK[i],
            "FECHA_CARGA": fecha,
            "CARGA_VIGENTE": True
        }
        array.append(query)

    out = insercionMongoQuery(array)

    return out
