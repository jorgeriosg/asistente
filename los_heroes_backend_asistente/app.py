#!/usr/bin/env python
# -*- coding: utf-8 -*-
import middleware_utils as core
import json
import time
import copy
from Functions.funciones import *

from flask import Flask
from flask import request
from flask import jsonify
from flask_cors import cross_origin
from flask_cors import CORS

import sendgrid
from sendgrid.helpers.mail import *
import logging.config
import losheroes_settings as config

import notificaciones
import traceback

from Functions.datadog import Datadog
from mods import datadog as dg
from mods import response_formats
from mods import decorators
from mods import request_validator
from mods import base_64

from views import integration

app = Flask(__name__)
CORS(app)

SGK = config.SG_KEY


@app.route('/test', methods=['GET'])
def test():
    return response_formats.output_format(200, 'Test de app.')


@app.route('/', methods=['GET', 'POST'])
@cross_origin(cross_origin=config.ALLOWED_ORIGIN)
def home():
    return jsonify({"status": 200})


@app.route('/send_form', methods=['POST'])
@decorators.exception_message
def send_form():
    # En la actualidad se capta la información de los afiliados y se envía como email a: canalesdigitales@losheroes.cl y a internet@losheroes.cl,
    # necesitamos que sólo las solicitudes que sean de "Pensionados" mantengan ambos recipientes, pero las solicitudes de "Trabajadores"
    # sólo sean enviadas a canalesdigitales@losheroes.cl, sacando como destinatario la otra casilla de correos.

    tipo_afiliado = None
    invalid = core.verify_form(request.json)
    if invalid:
        config.datadog_log('/send_form', 'invalid json: ' + json.dumps(request.json))
        return jsonify(invalid)
    tipo_plantilla = config.xss2_func(
        request.json['type'])
    perfil_data = None
    rut = config.xss2_func(request.json['rut'])
    phone = config.xss2_func(request.json['phone'])
    email = config.xss2_func(request.json['email'])
    comuna = config.xss2_func(request.json['comuna'])
    region = config.xss2_func(request.json['region'])
    fecha = datetime.datetime.now()
    if tipo_plantilla in (1, 2):
        name = config.xss2_func(request.json['name'])

        query = """INSERT INTO formularios (fecha, nombre, rut, email, telefono, texto, tipo, region, comuna) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

        parametros = (fecha, name, rut, email, phone, '', tipo_plantilla, region, comuna)
        cadena = "<br>Tipo de cliente: Pensionado no Afiliado <br>Nombre:  %s<br>Rut: %s<br>Email: %s<br>Fono: %s<br>Región: %s<br>Comuna: %s<br>Asunto: Solicitud de Afiliación<br>Canal: Ema" % (
            name, rut, email, phone, region, comuna)
    elif tipo_plantilla == 3:

        # campos::
        perfil = 'FILL_IN_PERFIL'
        tipo = 'Solicitud de Crédito'
        sucursal = 'www.losheroes.cl'
        canal = 'Ema'
        identificador = 'EXXXXX'
        renta_liquida = config.xss2_func(request.json['renta_liquida'])
        query = """INSERT INTO formularios_credito (fecha, rut, email, telefono,region,comuna,renta_liquida,perfil) values(%s,%s,%s,%s,%s,%s,%s,%s
            )"""
        parametros = (fecha, rut, email, phone, region, comuna, renta_liquida)
        '''
            Perfil	Trabajador / Pensionado
            Tipo	Solicitud de Crédito
            Rut	12.345.678-9
            Teléfono	9xxxxxxxx
            Email	mail@mail.cl
            Sucursal	www.losheroes.cl
            Fecha Solicitud	xx/xx/20xx
            Canal	Ema
            Identificador	EXXXXX
        '''

    connection = core.mysql_Connect()
    try:
        with connection.cursor() as cursor:
            if tipo_plantilla == 3:
                query_perfil = "select Tipo from afiliados where RUT = %s"
                cursor.execute(query_perfil, (rut,))
                perfil_data = cursor.fetchone()
                # se obtiene el tipo para decidir si enviar correo o no
                tipo_afiliado = perfil_data['Tipo']
                parametros = parametros + (tipo_afiliado,)
            cursor.execute(query, parametros)
            connection.commit()
            id_insercion = str(cursor.lastrowid)
            cursor.execute(
                'select * from plantilla_correo where type = %s', tipo_plantilla)
            email_body = cursor.fetchall()

            if tipo_plantilla == 3:
                cadena = """<br>Perfil:  %s<br>Tipo:  Solicitud de Crédito<br>Rut: %s<br>Teléfono: %s<br>Email: %s<br>Región: %s<br>Comuna: %s<br>Renta Líquida: %s<br>Sucursal: www.losheroes.cl<br>Fecha Solicitud: %s<br>
                Canal: Ema<br>Identificador: %s<br>""" % (
                    perfil_data['Tipo'], rut, phone, email, region, comuna, renta_liquida, fecha, id_insercion)

    finally:
        cursor.close()
        connection.close()

    # En la actualidad se capta la información de los afiliados y se envía como email a: canalesdigitales@losheroes.cl y a internet@losheroes.cl,
    # necesitamos que sólo las solicitudes que sean de "Pensionados" mantengan ambos recipientes, pero las solicitudes de "Trabajadores"
    # sólo sean enviadas a canalesdigitales@losheroes.cl, sacando como destinatario la otra casilla de correos.
    if email_body != []:
        # sg = sendgrid.SendGridAPIClient(apikey=SGK)
        # from_email = Email('no-reply@cognitiva.la')
        # aqui es mas de uno
        for email in email_body:
            # correo = email['email']
            # to_email = Email(email['email'])
            # subject = email['subject'] + ' ' + id_insercion
            # content = Content(
            #    "text/html", email['body'].replace('texto html', cadena))

            # mail = Mail(from_email, subject, to_email, content)
            # response = sg.client.mail.send.post(request_body=mail.get())
            correo = email['email'].split(',')
            correos = []
            if email['restriccion_clave'] is not None:
                # evaluar las restricciones aca se agrega en tabla plantillas email la restriccion (se deben agregar de forma manual)
                if tipo_plantilla == 3 and tipo_afiliado is not None and tipo_afiliado.lower() == email['restriccion_valor'].lower():
                    continue

            for mail in correo:
                correos.append({'email': mail.strip()})

            message = {
                'personalizations': [
                    {
                        'to': correos,
                        'subject': email['subject'] + ' ' + id_insercion
                    }
                ],
                'from': {
                    'email': 'no-reply@cognitiva.la'
                },
                'content': [
                    {
                        'type': "text/html",
                        'value': email['body'].replace('texto html', cadena)
                    }
                ]
            }
            sg = sendgrid.SendGridAPIClient(apikey=SGK)
            response = sg.client.mail.send.post(request_body=message)
            # response = sg.send(message)

    return jsonify({"codigo": 200, "glosa": "registro guardado"})


@app.route('/check_form', methods=['POST'])
@decorators.exception_message
def check_form():

    invalid = core.verify_check(request.json)
    if invalid:
        return jsonify(invalid)

    inicio = request.json['begin']
    fin = request.json['end']
    tipo = request.json['type']
    limit = request.json['limit']
    offset = request.json['offset']
    query = """select * from formularios where DATE_FORMAT(fecha,'%%Y/%%m/%%d') between %s and %s """
    parametros = (inicio, fin)

    if tipo:
        query += """ and tipo = %s """
        parametros = parametros + tuple(str(tipo))
    query += """ limit %s offset %s """
    parametros = parametros + (limit, offset)
    connection = core.mysql_Connect()
    with connection.cursor() as cursor:
        cursor.execute(query, parametros)
        result = cursor.fetchall()
        base = ''
        if result:
            base = base_64.b64_generator(result)

    return jsonify({"codigo": 200, "glosa": "listado de registros!", "data": result, 'b64': base})


@app.route('/xlsx_up', methods=['POST'])
@decorators.exception_message
def xlsx_up():
    """
    @apiGroup Mantenedor
    @apiName xlsx_up
    @api {POST} /xlsx_up cargar xlsx
    @apiVersion 1.0.0
    @apiDescription Carga un xlsx, lo valida y lo carga en la base de datos. desabilitando todos los otros beneficios.
    @apiParam {string} fecha de la carga para rollback

    @apiExample {form-data} Example-input:
                {
                    "file":"beneficios.xlsx"
                }
    @apiExample {json} Example-Response:
                {
                    "codigo":"Base de datos actualizada",
                    "glosa":200
                }
    """

    in_verify = core.verify_xlsx_input(request)
    # si falta un dato retornar afuera
    if in_verify:
        return jsonify(in_verify)
    fileup = request.files['file']
    output = xlsl_producto(fileup)
    if output == 200:
        desc = "Base de datos actualizada"
        return jsonify({"codigo": output, "glosa": desc})
    else:
        return jsonify(
            {"codigo": 400, "glosa": output})

    '''
        try:
            if 'file' not in request.files:
                return jsonify({"error": "Falta ingresar un campo en la consulta"})
            fileup = request.files['file']
            if not fileup:
                statusCode=400
                desc='no se subio el archivo correctamente'
                return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})
            # validacion de la extencion de archivo
            extension=fileup.filename.split('.')
            if extension[0]=='':
                statusCode = 400
                desc = 'archivo vacio'
                return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})

            if extension[1]!='xlsx':
                statusCode = 400
                desc = 'formato invalido'
                return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})

            output = xlsl_producto(fileup)

            if output == 200:
                StatusOK = "OK"
                Status_code = 200
                return jsonify({"estado": {"codigoEstado": Status_code, "glosaEstado": StatusOK}})
            else:
                StatusOK = output
                Status_code = 400
                return jsonify(
                    {"estado": {"codigoEstado": Status_code, "glosaEstado": StatusOK}})
        except Exception,e:
            desc = "Fallo inesperado" + str(e)
            statusCode = 400
            return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})
        '''


@app.route('/csv_up', methods=['POST'])
def csv_up():
    '''
    funcion que recibe un csv con nombre file y lo sube a la base de dato trans validacion
    :return:
    '''
    try:
        '''validacion de archivo existente'''
        if 'file' not in request.files:
            return jsonify({"error": "Falta ingresar un campo en la consulta"})
        fileup = request.files['file']
        if not fileup:
            statusCode = 400
            desc = 'no se subio el archivo correctamente'
            return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})
        # validacion de la extencion de archivo
        extension = fileup.filename.split('.')
        if extension[0] == '':
            statusCode = 400
            desc = 'archivo vacio'
            return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})

        if extension[1] != 'csv':
            statusCode = 400
            desc = 'formato invalido'
            return jsonify({"estado": {"codigoEstado": statusCode, "glosaEstado": desc}})

        output = csv_producto(fileup)
        if output == 200:
            StatusOK = "OK"
            Status_code = 200
            return jsonify({"estado": {"codigoEstado": Status_code, "glosaEstado": StatusOK}})

        else:
            '''fallo de subida de datos'''
            StatusOK = output
            Status_code = 400
            return jsonify(
                {"estado": {"codigoEstado": Status_code, "glosaEstado": StatusOK}})

    except Exception as e:
        config.datadog_log('/csv_up')
        return jsonify({"codigo": 500, "glosa": str(e)})


@app.route('/rollback', methods=['POST'])
@decorators.exception_message
def rollback():
    """
    @apiGroup Mantenedor
    @apiName rollback
    @api {POST} /rollback Rollback
    @apiVersion 1.0.0
    @apiDescription Cambia el estado de a activo de un grupo de beneficios en base a su fecha
    @apiParam {string} fecha de la carga para rollback

    @apiExample {json} Example-input:
                {
                    "fecha": "2017-12-20 15:38:26.984825"
                }
    @apiExample {json} Example-Response:
                {
                    "glosa":"se realizo el rollback"
                }
    """

    if 'fecha' not in request.json.keys():
        return jsonify({"codigo": 400, "glosa": "json missing 'fecha'"})
    out = roll(request.json['fecha'])
    if out['codigo'] == 200:
        return jsonify(out['glosa'])
    else:
        return jsonify({"codigo": 400, "glosa": 'fallo el rollback'})


@app.route('/fechasCarga', methods=['POST'])
@decorators.exception_message
def fechasCarga():
    """
    @apiGroup Mantenedor
    @apiName fechasCarga
    @api {POST} /fechasCarga fechas de carga
    @apiVersion 1.0.0
    @apiDescription devuelve un arreglo de fechas de carga de la base de datos

    @apiExample {json} Example-input:
                {
                }
    @apiExample {json} Example-Response:
                {
                    "codigo": 200,
                    "glosa": [
                        "1.0",
                        "2017-12-20 15:38:26.984825",
                        "2017-12-20 16:09:03.242565",
                        "2018-01-02 18:46:23.637000",
                        "2018-01-08 15:48:09.870343",
                        "2018-01-08 15:52:32.718064",
                        "2018-01-08 20:32:11.573512",
                        "2018-01-08 20:42:21.234169"
                    ]
                }
    """

    return jsonify(arregloFechas())


@app.route('/valorar', methods=['POST'])
def valorar():
    """
        @apiGroup Mantenedor
        @apiName valorar
        @api {POST} /valorar valorar
        @apiVersion 1.0.0
        @apiDescription agrega la valorizacion a del asistente a un sql
        @apiParam {int} valor valor de la valoracion
        @apiParam {String} comentario comentario del porque realizo la valoracion anterior
        @apiParam {String} origen Destop o mobil segun el front indique
        @apiParam {String} cid conversation id
        @apiParam {String} pregunta input del usuario
        @apiParam {String} id_nodo nombre del nodo visitado
        @apiParam {String} like 1/0/'' si el usuario dio o no like/dislike
        @apiParam {String} intent intents de conversation
        @apiParam {String} entity entities de conversation
        @apiParam {String} respuesta texto de respuesta entregado por conversation

        @apiExample {json} Example-input:
                    {
                        "valor" : 1,
                        "comentario" : "loret ipsum",
                        "origen" : "Desktop",
                        "cid" : "334e2f5e-589f-4884-a1fe-a4d172bd5d8b",
                        "pregunta":"¿porque las rosas son rojas?",
                        "id_nodo":"Info",
                        "like":"1",
                        "intent":"",,
                        "entity":"",
                        "respuesta":"La respuesta esta en tu corazon",
                    }
        @apiExample {json} Example-Response:
                    {
                        "desc": "Valorado con exito",
                        "status": 200
                    }
        """
    try:
        in_verify = core.verify_valorar_input(request)
        # si falta un dato retornar afuera
        if in_verify:
            return jsonify(in_verify)
        ret = core.addValoracion(request)
        return jsonify(ret)
    except Exception as e:
        desc = "fallo inesperado : " + str(e) + " "
        config.datadog_log('/valorar')
        return jsonify({"status": 400, "descripcion": desc})


@app.route('/message', methods=['POST'])
@cross_origin(cross_origin=config.ALLOWED_ORIGIN)
@decorators.exception_message
def message():
    """
    @apiGroup Conversation
    @apiName message
    @api {POST} /message message
    @apiVersion 1.0.0
    @apiDescription recibe cid y msg para devolver respuesta de conversation
    @apiParam {String} cid id de la conversacion realizada.
    @apiParam {String} msg texto ingresado por usuario.

    @apiExample {json} Example-input:
                {
                    "cid":null,
                    "msg":null
                }
    @apiExample {json} Example-Response:
                {
                    "cid": "334e2f5e-589f-4884-a1fe-a4d172bd5d8b",
                    "msg": "¡Buen día! Soy Ema, la asistente de beneficios de Los Héroes. ¿En qué le puedo ayudar?"
                }
    @apiExample {json} Example-Response-2:
                {
                    "cid": "3cd76964-1e82-4620-9778-261dc1282f5a",
                    "entities": [],
                    "intents": [
                    {
                        "confidence": 1,
                        "intent": "carga"
                    }
                    ],
                    "msg": "Para informaci\u00f3n relacionada a cargas familiares haga click <a href=\"https://www.losheroes.cl/wps/wcm/connect/internet/trabajadores/beneficios/apoyo-familiar/asignacion-familiar/asignacion+familiar\" target=\"_blank\">aqu\u00ed</a>. <br><br> \u00bfLe puedo ayudar en algo m\u00e1s?",
                    "nodo_id": "node_3_1512064790003",
                    "tipo": "business"
                }
    """
    in_verify = request_validator.verify_msg_input(request)
    # si falta un dato retornar afuera
    if in_verify:
        return in_verify
    return integration.message(copy.deepcopy(request.json))


@app.route('/get_notificaciones', methods=['POST'])
@cross_origin(cross_origin=config.ALLOWED_ORIGIN)
@decorators.exception_message
def get_notificaciones():
    try:
        estado, resultado = notificaciones.find_notificaciones()
        return jsonify({"status": estado, "notificaciones": resultado})
    except Exception:
        return jsonify({"error": traceback.format_exc()})


@app.route('/update_notificaciones_usuarios', methods=['POST'])
@cross_origin(cross_origin=config.ALLOWED_ORIGIN)
@decorators.exception_message
def update_notificaciones_usuarios():
    estado, resultado = notificaciones.update_notificaciones(notificaciones=request.json['notificaciones'])
    return jsonify({"status": estado, "notificaciones": resultado})


@app.route('/update_archivo_notificaciones', methods=['POST'])
@cross_origin(cross_origin=config.ALLOWED_ORIGIN)
@decorators.exception_message
def update_archivo_notificaciones():
    estado, resultado = notificaciones.carga_usuarios_mantenedor(request)
    return jsonify({"status": estado, "carga": resultado})


@app.route('/get_notificacion', methods=['POST'])
@cross_origin(cross_origin=config.ALLOWED_ORIGIN)
@decorators.exception_message
def get_notificacion():
    try:
        id_notificacion = request.json['id'] if 'id' in request.json else ''
        estado, resultado = notificaciones.find_notificacion(id_notificacion=id_notificacion)
        return jsonify({"status": estado, "notificacion": resultado})
    except Exception:
        return jsonify({"error": traceback.format_exc()})


if __name__ == '__main__':
    app.run(debug=False)
