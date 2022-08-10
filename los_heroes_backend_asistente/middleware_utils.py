#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import pymysql
import re
import losheroes_settings as config
from Functions.funciones import *
import Functions.sql_functions as sql
from cognitiva_xss_3.xss3 import XssClean
import traceback
from Functions.datadog import Datadog
from itertools import cycle
from notificaciones import buscar_notificaciones, guardar_notificacion, guardar_notificacion_noafiliados

from mods import validator
from mods import datadog as dg
from mods import SqlConnection
from mods import mongo_library
from mods import watson_library as watson
from mods import canales


def verify_check(request):
    invalid = False
    if 'begin' not in request:
        invalid = True
    elif 'end' not in request:
        invalid = True
    elif 'type' not in request:
        invalid = True

    if invalid:
        invalid = {"codigo": 400, "glosa": 'campos invalidos'}

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

    if invalid:
        invalid = {"codigo": 400, "glosa": 'campos invalidos'}

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


def verify_valorar_input(request):
    if request.headers['Content-Type'] == 'application/json':
        # json debe tener llave 'msg'.
        if 'valor' not in request.json.keys():
            return {"message": "json missing 'valor'."}
        if 'comentario' not in request.json.keys():
            return {"message": "json missing 'comentario'."}
        if 'origen' not in request.json.keys():
            return {"message": "json missing 'origen'."}
        if 'cid' not in request.json.keys():
            return {"message": "json missing 'cid'."}
        if 'pregunta' not in request.json.keys():
            return {"message": "json missing 'pregunta'."}
        if 'id_nodo' not in request.json.keys():
            return {"message": "json missing 'id_nodo'."}
        if 'like' not in request.json.keys():
            return {"message": "json missing 'like'."}
        if 'intent' not in request.json.keys():
            return {"message": "json missing 'intent'."}
        if 'entity' not in request.json.keys():
            return {"message": "json missing 'entity'."}
        if 'respuesta' not in request.json.keys():
            return {"message": "json missing 'respuesta'."}
        return 0
    else:
        return {"message": "415 Unsupported Media Type."}


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
            return {"message": "json missing 'msg'."}

        # el valor de 'msg' no puede ser bool, int o float
        # (solo str, unicode(?), y None- o null en json.
        if isinstance(request.json['msg'], (bool, int, float)):
            return {"message": "Invalid value type for 'msg'."}

        # json debe tener llave 'cid'.
        if 'cid' not in request.json.keys():
            return {"message": "json missing 'cid'."}

        # 'cid' no puede ser bool, int o float, solo str, unicode(?) o None.
        if isinstance(request.json['cid'], (bool, int, float)):
            return {"message": "Invalid value type for 'cid'."}

        # si 'msg' es str o unicode, no puede ser vacío o solo whitespace.
        if isinstance(request.json['msg'], (str)):
            if not request.json['msg'].strip():
                return {"message": "'msg' is an empty or whitespace string."}

        # si 'cid' es str o unicode, no puede ser vacío o solo whitespace.
        if isinstance(request.json['cid'], (str)):
            if not request.json['cid'].strip():
                return {"message": "'cid' is an empty or whitespace string."}

        # si 'cid' tiene un valor, 'msg' no puede ser null.
        if request.json['cid'] is not None:
            if request.json['msg'] is None:
                return {"message": "'msg' can only be null if 'cid' is null."}

            # 'cid' id tiene que tener un patrón igual a los conversation ids de la API.
            # TODO: compile regexp.
            if not re.match(
                    r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
                    request.json['cid']):
                return {"message": "invalid format for 'cid'."}

            # 'cid' debe estar en la base de datos.
            if len(list(config.interactions.find({"interaction.context.conversation_id": request.json['cid']}).limit(1))) == 0:
                return {"message": "'cid' not in db.'"}
        return 0
    else:
        return {"message": "415 Unsupported Media Type."}


def get_ask_rut_value() -> bool:
    ask_for_rut = False
    msql = SqlConnection.SqlConnection()
    try:
        query = """SELECT pide_rut FROM parametros WHERE id_cliente = 1;"""
        db_value_rut = msql.find(query)
        if isinstance(db_value_rut, list) and len(db_value_rut) > 0:
            ask_for_rut = True if str(db_value_rut[0]['pide_rut']) == '1' else False
    except Exception as unknown_exception:
        ask_for_rut = False
        dg.log_datadog(f'Unknown exception at get_ask_rut_value: {unknown_exception}, Traceback: {traceback.format_exc()}')
    finally:
        msql.close_connection()

    return ask_for_rut


def get_special_notification_value() -> bool:
    special_notification = False
    # Commented temporarily, as this value it is added to the database unomment the process
    msql = SqlConnection.SqlConnection()
    try:
        query = """SELECT notificacion_especial FROM parametros WHERE id_cliente = 1;"""
        db_value_notification = msql.find(query)
        if isinstance(db_value_notification, list) and len(db_value_notification) > 0:
            special_notification = True if str(db_value_notification[0]['notificacion_especial']) == '1' else False
    except Exception as unknown_exception:
        special_notification = False
        dg.log_datadog(f'Unknown exception at get_special_notification_value: {unknown_exception}, Traceback: {traceback.format_exc()}')
    finally:
        msql.close_connection()

    return special_notification


def ask_watson_again(ip, context) -> dict:
    watson_lib = watson.WatsonV1()
    conv_response = watson_lib.watson_call(input='', context=context)

    document = {
        'interaction': conv_response,
        'datetime': datetime.datetime.utcnow(),
        'workspace_type': 'business',
        'clientIp': ip,
        'id_cliente': "1"
    }
    mongo_library.insert(document)
    return conv_response


def saludo_bd():
    '''
    saludo desde base de datos
    :return:
        succes: json: msg, cid
    '''
    saludo = get_saludo()
    return pre_message_return(saludo, None)


def chit_chat_interaction(msg_input, cid, ip, start_time=time.time()):
    '''
    interaccion de chitchat de conversation.
    :param msg_input: input de usuario
    :param cid: conversation id
    :param start_time:
    :return:
        succes: json: cid, msg
    '''
    # se ejecuta chit-chat
    watsonV1 = watson.WatsonV1()
    context = {
        # creo context nuevo, en vez de modificar el oultimo en
        # bd, pq chitchat son preguntas atómicas
        'conversation_id': cid,
        'workspace_type': 'chitchat'
    }
    if isinstance(msg_input, dict):
        msg_input = msg_input['text'] if 'text' in msg_input else ''
    chitchat_conv_response = watsonV1.watson_call(input=msg_input, context=context, tipo='chitchat')

    # para saber que no es chitchat
    if chitchat_conv_response['output']['text'][-1] != "NO CAMBIAR":
        # guardar la respuesta de conversation en base de dato, con fecha.
        document = {
            'interaction': chitchat_conv_response,
            'datetime': datetime.datetime.utcnow(),
            'workspace_type': 'chitchat',
            'clientIp': ip,
            'id_cliente': "1"
        }
        mongo_library.insert(document)  # check if succesful.
        return pre_message_return(chitchat_conv_response['output']['text'], chitchat_conv_response['context']['conversation_id'])

    return 0


def start_conversation(ip):
    watsonV1 = watson.WatsonV1()
    context = {'pedir_rut': get_ask_rut_value()}
    conv_response = watsonV1.watson_call(input='', context=context)

    document = {
        'interaction': conv_response,
        'datetime': datetime.datetime.utcnow(),
        'workspace_type': 'business',
        'clientIp': ip,
        'id_cliente': "1"
    }
    # config.interactions.insert_one(document)
    mongo_library.insert(document)
    return conv_response['context']['conversation_id']


def atomico_interaction(msg_input, cid, ip, start_time=time.time()):
    '''
    interaccion de chitchat de conversation.
    :param msg_input: input de usuario
    :param cid: conversation id
    :param ip: ip de donde se llamo
    :param start_time:
    :return:
        succes: json: cid, msg
    '''
    # se ejecuta chit-chat
    # config.ATOMICO_WORKSPACE = '4e0d9765-d005-4dc9-b6a8-9812adea94c4'
    watsonV1 = watson.WatsonV1()
    context = {
        'conversation_id': cid,
        'workspace_type': 'business'
    }
    if isinstance(msg_input, dict):
        msg_input = msg_input['text'] if 'text' in msg_input else ''
    atomico_conv_response = watsonV1.watson_call(input=msg_input, context=context, tipo='atomico')

    # para saber que no es chitchat
    if len(atomico_conv_response['output']['text']) > 0 and not atomico_conv_response['output']['text'][-1] == "NO CAMBIAR":
        # guardar la respuesta de conversation en base de dato, con fecha.
        document = {
            'interaction': atomico_conv_response,
            'datetime': datetime.datetime.utcnow(),
            'workspace_type': 'business',
            'clientIp': ip,
            'id_cliente': "1"
        }
        mongo_library.insert(document)  # check if succesful.
        entities = atomico_conv_response['entities']
        intents = atomico_conv_response['intents']
        nodos = atomico_conv_response['output']['nodes_visited'][-1]
        tipo = 'business'
        return pre_message_return(atomico_conv_response['output']['text'], cid, entities, intents, nodos, tipo, msg_input)

    return 0

def consentimiento_no_afiliados(conv_response, consentimiento):
    msql_connection = SqlConnection.SqlConnection()
    query = """SELECT id FROM notificaciones_historico_noafiliados ORDER BY id DESC LIMIT 1"""
    notificaciones_historico_noafiliados_id = msql_connection.find(query)
       
    query = """UPDATE notificaciones_historico_noafiliados SET interes=%s WHERE id=%s"""
    params = (consentimiento, notificaciones_historico_noafiliados_id[0]['id'],)                       
    consent_inserted =  msql_connection.alter(query, params)
    if consent_inserted:
        msql_connection.commit_and_close()
        #Interes de Afiliación de usuario guardado en la base de datos
        conv_response['context']['guardar_interes_no_afiliado'] = False
    else:
        msql_connection.rollback_and_close()
    return conv_response


def los_heroes_interacion(request_body, ip, start_time=time.time()):
    '''
    llamada de negocio conversation
    :param request_body: request_body de flask
    :param start_time:
    :return:
        succes: json: cid, msg
    '''
    try:
        inicializar = False
        rut_formulario_3 = None
        conv_interactions = list()
        ###
        message_input = {'text': request_body['msg']}

        last_interaction_2 = list(config.interactions.find({
            'interaction.context.conversation_id': request_body['cid'],
            'workspace_type': 'business'
        }).sort('datetime', -1).limit(1))

        last_interaction = last_interaction_2[0] if last_interaction_2 else {'interaction': {'context': {}}}

        context = last_interaction['interaction']['context']

        context['workspace_type'] = 'business'

        conv_interaction = {
            'IN': {
                'input': message_input,
                'context': context
            }
        }

        if request_body['cid'] is None:
            inicializar = True

        watsonV1 = watson.WatsonV1()
        notificacion_text = False

        if "consulta" in request_body and request_body["consulta"].lower() == "licencia":
            if request_body["rut"] == "" and request_body["folio"] == "":
                request_body["msg"] = request_body["consulta"]
                message_input = {'text': request_body['msg']}
            else:
                context["licencias_medicas_url"] = True
                context["rut_persona"] = request_body["rut"].replace("-", "")
                context["rut_persona_aux"] = request_body["rut"].replace("-", "")
                context["folio_persona"] = request_body["folio"]
        elif "consulta" in request_body:
            request_body["msg"] = request_body["consulta"]
            message_input = {'text': request_body['msg']}

        conv_response = watsonV1.watson_call(input=message_input['text'], context=context)

        request_body['cid'] = conv_response['context']['conversation_id'] if request_body['cid'] is None else request_body['cid']

        valor_formulario = False

        if 'formulario' in conv_response['context']:
            if conv_response['context']['formulario'] in (1, 2):
                valor_formulario = conv_response['context']['formulario']
                conv_response['context']['formulario'] = False

        if inicializar:
            conv_response['context']['contador_errores'] = 0
            conv_response['context']['max_error'] = config.MAX_ERROR

        document = {
            'interaction': conv_response,
            'datetime': datetime.datetime.utcnow(),
            'workspace_type': 'business',
            'clientIp': ip,
            'id_cliente': "1"
        }
        mongo_library.insert(document)

        if "toAnswer" in conv_response['context'] and conv_response['context']["toAnswer"] == "validar_rut":
            print("en toAnswer = validar_rut tengo en conv_response", conv_response)
            if 'rut' not in conv_response['context']:
                valid_rut = False
            else:
                valid_rut, formated_rut = validator.verify_rut(conv_response['context']['rut'])
            conv_response['context']["rut_valido"] = valid_rut
            conv_response['context']["sin_rut"] = False
            if valid_rut:
                conv_response['context']['es_afiliado'], aux_rut = sql.buscar_rut(formated_rut, 'notificacion_especial')
                conv_response['context']['rut_persona'] = formated_rut
                conv_response['context']['rut_persona_aux'] = formated_rut
                if conv_response['context']['es_afiliado']:
                    notificaciones = buscar_notificaciones(conv_response['context']['rut'])
                    if notificaciones[0]:
                        conv_response['context']["tiene_notificaciones"] = True
                        conv_response['context']["id_notificacion"] = str(notificaciones[2]["id_notificacion"])

                        conv_response['context']["desea_ser_contactado"] = True if str(notificaciones[2]["desea_ser_contactado"]) == '1' else False
                        if conv_response['context']["desea_ser_contactado"]:
                            conv_response['context']["notificacion_correo_destinatario"] = str(notificaciones[2]["correo_receptor"])
                            conv_response['context']["notificacion_correo_asunto"] = str(notificaciones[2]["correo_titulo"])
                            conv_response['context']["notificacion_correo_nombre"] = str(notificaciones[2]["nombre"])
                            conv_response['context']["pedir_telefono"] = True if str(notificaciones[2]["ingresar_telefono"]) == '1' else False
                            conv_response['context']["pedir_correo"] = True if str(notificaciones[2]["ingresar_correo"]) == '1' else False
                        else:
                            conv_response['context']["pedir_telefono"] = False
                            conv_response['context']["pedir_correo"] = False

                        notificacion_text = notificaciones[2]["mensaje"]
                        if "NOMBRE" in notificacion_text:
                            notificacion_text = notificacion_text.replace("NOMBRE", notificaciones[2]["nombre"])
                        if "CUSTOM1" in notificacion_text:
                            notificacion_text = notificacion_text.replace("CUSTOM1", notificaciones[2]["custom1"])
                        if "CUSTOM2" in notificacion_text:
                            notificacion_text = notificacion_text.replace("CUSTOM2", notificaciones[2]["custom2"])
                        if "CUSTOM3" in notificacion_text:
                            notificacion_text = notificacion_text.replace("CUSTOM3", notificaciones[2]["custom3"])
                        if "CUSTOM4" in notificacion_text:
                            notificacion_text = notificacion_text.replace("CUSTOM4", notificaciones[2]["custom4"])
                        if "CUSTOM5" in notificacion_text:
                            notificacion_text = notificacion_text.replace("CUSTOM5", notificaciones[2]["custom5"])
                        rut = conv_response['context']['rut'].replace(".", "").replace("-", "")
                        guardar_notificacion(rut, notificaciones[2]["id_notificacion"], notificaciones[2]['nombre_mostrar'], conv_response['context']["conversation_id"])
                    else:
                        conv_response['context']["tiene_notificaciones"] = False
                        conv_response['context']["desea_ser_contactado"] = False
                else:
                    msql_connection = SqlConnection.SqlConnection()
                    conv_response['context']['guardar_interes_no_afiliado'] = ''
                    conv_response['context']['notificacion_no_afiliado'] = get_special_notification_value()
                    rut_no_afiliado = conv_response['context']['rut'].replace(".", "").replace("-", "")
                    # buscamos id de la notifiacion activa para los no afiliados
                    query = """SELECT * FROM notificaciones_noafiliados WHERE activo = 1"""
                    notificacion_noafiliados_mostrar = msql_connection.find(query)
                    print(notificacion_noafiliados_mostrar,'rut:', rut_no_afiliado,"\nrut es de no afiliado: ",conv_response['context']['es_afiliado'], "\nnotificacion_no_afiliado: ", conv_response['context']['notificacion_no_afiliado'], )
                    # Notificaciones Proactivas: cuando hay una interaccion con el usuario no afiliado guardamos un registro en tabla notificaciones_historico_noafiliados
                    if conv_response['context']['notificacion_no_afiliado']:
                        guardar_notificacion_noafiliados(rut_no_afiliado, notificacion_noafiliados_mostrar[0]['id'], notificacion_noafiliados_mostrar[0]['nombre_mostrar'], conv_response['context']["conversation_id"])
                        conv_response['context']['guardar_interes_no_afiliado'] = True

                        texts = conv_response['output']['text']
                        print("Llegamos al final de notificaciones_historico_noafiliados: ", conv_response, texts)

            conv_response['context']["toAnswer"] = ''
            conv_response = ask_watson_again(ip, conv_response['context'])
            conv_response['context']['conversation_id'] = request_body['cid']
            print("al final de toAnswer = validar_rut tengo en conv_response", conv_response)

        elif 'toAnswer' in conv_response['context'] and conv_response['context']['toAnswer'] == 'guardar_telefono_afiliado':
            print("al principio de toAnswer = guardar_telefono_afiliado tengo en conv_response", conv_response)
            msql_connection = SqlConnection.SqlConnection()
            consent_params = {
                'desea_ser_contactado': 1,
                'telefono': conv_response['context']['telefono_afiliado'],
                'rut': conv_response['context']['rut_persona']
            }
            consent_query = """INSERT INTO consentimiento_usuario (desea_ser_contactado, telefono, rut_usuario)
                VALUES (%(desea_ser_contactado)s, %(telefono)s, %(rut)s);"""
            consent_inserted = msql_connection.alter(consent_query, consent_params, True)
            print("El afiliado: ", conv_response['context']['es_afiliado'], " -*- tiene_notificaciones: ", conv_response['context']["tiene_notificaciones"])
            # Notificaciones Proactivas: cuando hay una interaccion con el usuario guardamos un registro en tabla notificaciones_historico
            if conv_response['context']["tiene_notificaciones"]:
                # buscamos id del ultimo contacto_cliente agregado
                query = """SELECT id AS consentimiento_usuario_id FROM consentimiento_usuario ORDER BY id DESC LIMIT 1"""
                consentimiento_usuario_id = msql_connection.find(query)
                print("consentimiento_usuario_id: ",consentimiento_usuario_id)
                query = """SELECT id AS notificaciones_historico_id FROM notificaciones_historico ORDER BY id DESC LIMIT 1"""
                notificaciones_historico_id = msql_connection.find(query)
                print("notificaciones_historico_id: ",notificaciones_historico_id)
                query = """UPDATE notificaciones_historico SET consentimiento_usuario_id=%s WHERE id=%s"""
                params = (consentimiento_usuario_id[0]['consentimiento_usuario_id'], notificaciones_historico_id[0]['notificaciones_historico_id'],)
                print("toAnswer = guardar_telefono_afiliado >> params para UPDATE notificaciones_historico SET consentimiento_usuario_id:", params)
                resp =  msql_connection.alter(query, params)
            if consent_inserted:
                msql_connection.commit_and_close()
                conv_response['context']['telefono_guardado'] = True
                canales.enviar_correo_notificacion(
                    id_datos=consent_inserted,
                    rut_persona=conv_response['context']['rut_persona'],
                    nombre_persona=conv_response['context']['notificacion_correo_nombre'],
                    medio_notificacion='telefono',
                    dato_contacto=conv_response['context']['telefono_afiliado'],
                    receptor=conv_response['context']['notificacion_correo_destinatario'],
                    asunto=conv_response['context']['notificacion_correo_asunto']
                )
            else:
                msql_connection.rollback_and_close()
                conv_response['context']['telefono_guardado'] = False

            conv_response['context']['toAnswer'] = ''
            conv_response = watsonV1.watson_call(input='', context=conv_response['context'])

            document = {
                'interaction': conv_response,
                'datetime': datetime.datetime.utcnow(),
                'workspace_type': 'business',
                'clientIp': ip,
                'id_cliente': "1"
            }
            conv_response['context']['conversation_id'] = request_body['cid']
            mongo_library.insert(document)
            texts = conv_response['output']['text']
            print("Llegamos al final de toAnswer = guardar_telefono_afiliado con el siguiente conv_response", conv_response)

        elif 'toAnswer' in conv_response['context'] and conv_response['context']['toAnswer'] == 'guardar_email_cliente':
            print("al principio de toAnswer = guardar_email_cliente tengo en conv_response", conv_response)
            msql_connection = SqlConnection.SqlConnection()
            consent_params = {
                'desea_ser_contactado': 1,
                'email': conv_response['context']['email_afiliado'],
                'rut': conv_response['context']['rut_persona']
            }
            consent_query = """INSERT INTO consentimiento_usuario (desea_ser_contactado, email, rut_usuario)
                VALUES (%(desea_ser_contactado)s, %(email)s, %(rut)s);"""
            consent_inserted = msql_connection.alter(consent_query, consent_params, True)
            print("El afiliado: ", conv_response['context']['es_afiliado'], " -*- tiene_notificaciones: ", conv_response['context']["tiene_notificaciones"])
            # Notificaciones Proactivas: cuando hay una interaccion con el usuario guardamos un registro en tabla notificaciones_historico
            if conv_response['context']["tiene_notificaciones"]:
                # buscamos id del ultimo contacto_cliente agregado
                query = """SELECT id AS consentimiento_usuario_id FROM consentimiento_usuario ORDER BY id DESC LIMIT 1"""
                consentimiento_usuario_id = msql_connection.find(query)
                print("consentimiento_usuario_id: ",consentimiento_usuario_id)
                query = """SELECT id AS notificaciones_historico_id FROM notificaciones_historico ORDER BY id DESC LIMIT 1"""
                notificaciones_historico_id = msql_connection.find(query)
                print("notificaciones_historico_id: ",notificaciones_historico_id)
                query = """UPDATE notificaciones_historico SET consentimiento_usuario_id=%s WHERE id=%s"""
                params = (consentimiento_usuario_id[0]['consentimiento_usuario_id'], notificaciones_historico_id[0]['notificaciones_historico_id'],)
                print("toAnswer = guardar_email_cliente >> params para UPDATE notificaciones_historico SET consentimiento_usuario_id:", params)
                resp =  msql_connection.alter(query, params)
            if consent_inserted:
                msql_connection.commit_and_close()
                conv_response['context']['email_guardado'] = True
                canales.enviar_correo_notificacion(
                    id_datos=consent_inserted,
                    rut_persona=conv_response['context']['rut_persona'],
                    nombre_persona=conv_response['context']['notificacion_correo_nombre'],
                    medio_notificacion='correo',
                    dato_contacto=conv_response['context']['email_afiliado'],
                    receptor=conv_response['context']['notificacion_correo_destinatario'],
                    asunto=conv_response['context']['notificacion_correo_asunto']
                )
            else:
                msql_connection.rollback_and_close()
                conv_response['context']['email_guardado'] = False

            conv_response['context']['toAnswer'] = ''
            conv_response = watsonV1.watson_call(input='', context=conv_response['context'])

            document = {
                'interaction': conv_response,
                'datetime': datetime.datetime.utcnow(),
                'workspace_type': 'business',
                'clientIp': ip,
                'id_cliente': "1"
            }
            conv_response['context']['conversation_id'] = request_body['cid']
            mongo_library.insert(document)

            print("Llegamos al final de toAnswer = guardar_email_cliente con el siguiente conv_response", conv_response)

        if 'validar_rut' in conv_response['context'] and conv_response['context']['validar_rut']:
            rut_is_valid = False
            if "rut_persona" in conv_response["context"]:
                if conv_response["context"]["rut_persona"] == "" and "rut_persona_aux" in conv_response["context"]:
                    rut_is_valid, rut = sql.buscar_rut(conv_response['context']['rut_persona_aux'], conv_response['context']['from'])
                else:
                    rut_is_valid, rut = sql.buscar_rut(conv_response['context']['rut_persona'], conv_response['context']['from'])
            else:
                rut_is_valid, rut = sql.buscar_rut(request_body['msg'], conv_response['context']['from'])
            conv_response['context']["rut_valido"] = rut_is_valid
            conv_response['context']['user_rut'] = rut_is_valid
            if rut is not None:
                conv_response['context']['rut_persona'] = rut
                conv_response['context']['rut_persona_aux'] = rut

            conv_response['context']['conversation_id'] = request_body['cid']

            # probar la cosa
            # conv_response['context']['system']['dialog_turn_counter'] = 3
            # conv_response['context']['system']['dialog_request_body_counter'] = 3
            context = conv_response['context']
            conv_response = watsonV1.watson_call(input='', context=context)
            if 'formulario' in conv_response['context']:
                if conv_response['context']['formulario'] == 3:
                    valor_formulario = conv_response['context']['formulario']
                    rut_formulario_3 = conv_response['context']['rut_persona']
                    conv_response['context']['formulario'] = False
                # saltarse el nodo para terminar la conversa
                # conv_response = conversation.message(
                #     config.WORKSPACE,
                #     input={'text': ''},
                #     context=conv_response['context']).get_result()

            document = {
                'interaction': conv_response,
                'datetime': datetime.datetime.utcnow(),
                'workspace_type': 'business',
                'clientIp': ip,
                'id_cliente': "1"
            }
            conv_response['context']['conversation_id'] = request_body['cid']
            mongo_library.insert(document)
            texts = conv_response['output']['text']

        # Notificaciones Proactivas: No Afiliados de los Heroes
        if 'notificacion_no_afiliado' in conv_response['context'] and conv_response['context']['notificacion_no_afiliado']:
            if 'es_afiliado' in conv_response['context'] and conv_response['context']['es_afiliado'] == False:
                if 'guardar_interes_no_afiliado' in conv_response['context'] and conv_response['context']['guardar_interes_no_afiliado']:

                    if conv_response['input']['text'] == "No":
                        consentimiento = "negativo"
                        conv_response = consentimiento_no_afiliados(conv_response, consentimiento)
                        texts = conv_response['output']['text']

                    # por alguna razon esto no ocurre, mientras que su contraparte conv_response['input']['text'] == "No" siempre esta disponible en este contexto de variables
                    # if conv_response['input']['text'] == "Si":
                    else:
                        consentimiento = "positivo"
                        conv_response = consentimiento_no_afiliados(conv_response, consentimiento)
                        texts = conv_response['output']['text']

        if 'validar_folio' in conv_response['context'] and conv_response['context']['validar_folio']:
            # valido folio y te lleno la data::
            if "folio_persona" in conv_response["context"]:
                if conv_response["context"]["rut_persona"] == "" and "rut_persona_aux" in conv_response["context"]:
                    output_dict = sql.buscar_data_folio(conv_response['context']['rut_persona_aux'], conv_response["context"]["folio_persona"])
                else:
                    output_dict = sql.buscar_data_folio(conv_response['context']['rut_persona'], conv_response["context"]["folio_persona"])
            else:
                if conv_response["context"]["rut_persona"] == "" and "rut_persona_aux" in conv_response["context"]:
                    output_dict = sql.buscar_data_folio(conv_response['context']['rut_persona_aux'], request_body['msg'])
                else:
                    output_dict = sql.buscar_data_folio(conv_response['context']['rut_persona'], request_body['msg'])

            conv_response['context'].update(output_dict)
            message_input = ''
            context = conv_response['context']
            if "licencias_medicas_url" in context:
                context.pop("licencias_medicas_url")
            conv_response = watsonV1.watson_call(input=message_input, context=context)
            document = {
                'interaction': conv_response,
                'datetime': datetime.datetime.utcnow(),
                'workspace_type': 'business',
                'clientIp': ip,
                'id_cliente': "1"
            }

            mongo_library.insert(document)
            texts = conv_response['output']['text']

        texts = conversation_step(request_body, conv_response)

        # validaciones

        msg_input = request_body['msg']
        entities = conv_response['entities']
        intents = conv_response['intents']
        nodos = conv_response['output']['nodes_visited'][-1]
        tipo = 'business'

        if notificacion_text:
            if isinstance(texts, str):
                texts = notificacion_text + texts
            if isinstance(texts, list):
                texts.append(notificacion_text)
                texts = texts[::-1]
    except Exception as unknown_exception:
        dg.log_datadog(f'Unknown exception at los_heroes_interacion: {unknown_exception}, Traceback: {traceback.format_exc()}')
    return pre_message_return(texts, request_body['cid'], entities, intents, nodos, tipo, msg_input, valor_formulario, rut_formulario_3)


def pre_message_return(text, cid, entities=None, intents=None, nodos=None, tipo=None, msg_input=None, formulario=False, rut_formulario_3=None):
    '''
    formatea el json de salida con texto y cid.
    :param text:
    :param cid:
    :return:
    '''
    texto = ''
    if isinstance(text, str):
        texto = text
    if isinstance(text, list):
        texto = ' '.join(text)
    if entities is None:
        return (
            {
                'msg': texto,
                'cid': cid

            }
        )
    else:
        return (
            {
                'msg': texto,
                'cid': cid,
                'entities': entities,
                'intents': intents,
                'nodo_id': nodos,
                'tipo': tipo,
                "formulario": formulario,
                "rut_formulario_3": rut_formulario_3
            }
        )


def conversation_step(request_body, conv_response):
    '''
    interaccion entre conversation y base de datos.
    :param request_body:
    :param conv_response: json que entrega conversation
    :return:
        succes: json: cid, msg
    '''

    try:
        caracterInicial = conv_response['output']['text'][0][0]
    except Exception as e:
        caracterInicial = ''

    if caracterInicial == '%':
        convResp = conv_response['output']['text'][0][3:-1]
        entidad, valor = identificacionVariables(convResp)
        if entidad == ['provincia', 'comuna', 'categoria', 'subcategoria']:
            ''' salida completa. provincia, comuna, categoria, subcategoria '''
            texto = ''
            validador = []

            # variables
            provincia = valor[0].upper()
            comuna = valor[1].upper()
            categoria = valor[2].upper()
            subcategoria = valor[3].upper()
            # mongo

            texto = todosLosParametros(
                provincia, comuna, categoria, subcategoria)

        elif ['provincia', 'comuna', 'categoria'] == entidad:
            '''# info, cuando no viene la subcategoria'''
            provincia = valor[0].upper()
            comuna = valor[1].upper()
            categoria = valor[2].upper()
            texto = busquedaProvinciaComunaCategoria(
                provincia, comuna, categoria, '', request_body['cid'])

        elif ['provincia', 'comuna', 'nombre'] == entidad:
            '''info para un prestador por nombre'''

            provincia = valor[0].upper()
            comuna = valor[1].upper()
            nombre = valor[2].upper()
            texto = busquedaProvinciaComunaNombre(provincia, comuna, nombre)

        elif ['comuna', 'nombre'] == entidad:
            '''para la direccion de un prestador'''

            comuna = valor[0].upper()
            nombre = valor[1].upper()
            texto = busquedaComunaNombre(comuna, nombre)

        elif ['provincia', 'comuna'] == entidad:
            '''provincia y comuna da categorias'''

            provincia = valor[0].upper()
            comuna = valor[1].upper()

            texto = busquedaProvinciaComuna(provincia, comuna, '')

        elif ['comuna', 'tipo'] == entidad:
            # TODO: salida del intent #medico busca por comuna, tipo... pendiente a envio de data del cliente.
            comuna = valor[0].upper()
            tipo = valor[1].upper()

            texto = busquedaCentrosMedicos(comuna, tipo)

        else:
            texto = 'Lo sentimos, no existen beneficios asociados.<br><br>' + textoSalida()
        conv_response['output']['text'] = texto

    if caracterInicial == '&':
        convResp = conv_response['output']['text'][0][1:]
        conv_response['output']['text'] = guardarValoracion(
            convResp) + textoSalida()
        ###

    # msg = formateoArregloATexto(conv_response['output']['text'])
    msg = conv_response['output']['text']
    return msg


def addValoracion(request):
    connection = SqlConnection.SqlConnection()
    try:
        rating_params = {
            'comentario': XssClean.sanitizeHtml(request.json['comentario']),
            'origen': XssClean.sanitizeHtml(request.json['origen']),
            'cid': XssClean.sanitizeHtml(request.json['cid']),
            'pregunta': XssClean.sanitizeHtml(request.json['pregunta']),
            'nodo': XssClean.sanitizeHtml(request.json['id_nodo']),
            'like': XssClean.sanitizeHtml(request.json['like']),
            'intent': XssClean.sanitizeHtml(request.json['intent']),
            'entity': XssClean.sanitizeHtml(request.json['entity']),
            'respuesta': XssClean.sanitizeHtml(request.json['respuesta'])
        }
        valoracion = XssClean.sanitizeHtml(request.json['valor'])

        if valoracion == "":
            rating_params['valoracion'] = 0
        else:
            rating_params['valoracion'] = valoracion

        rating_query = """INSERT INTO valoracion (
            valoracion,
            comentario,
            origen,
            cid,
            input,
            output,
            nodo_id,
            like_v,
            intents,
            entities
        ) VALUES (
            %(valoracion)s,
            %(comentario)s,
            %(origen)s,
            %(cid)s,
            %(pregunta)s,
            %(respuesta)s,
            %(nodo)s,
            %(like)s,
            %(intent)s,
            %(entity)s
        );"""
        rating_inserted = connection.alter(rating_query, rating_params)
        if rating_inserted:
            connection.commit_and_close()
            return {"status": 200, "codigo": "01", "desc": "Valorado con exito"}
        else:
            connection.rollback_and_close()
            return {
                "estado": {
                    "codigo": 500,
                    "glosa": 'Error al insertar la valoracion'
                }
            }
    except Exception as unknown_exception:
        connection.roll_back_and_close()
        dg.log_datadog(f'Unknown exception at addValoracion: {unknown_exception}, Traceback: {traceback.format_exc()}')
        desc = "Error inesperado : " + str(unknown_exception)
        return {"status": 400, "descripcion": desc}


def mysql_Connect():
    return pymysql.connect(
        host=config.MySQL_host,
        user=config.MySQL_user,
        password=config.MySQL_pass,
        db=config.DB_name,
        charset='utf8',
        use_unicode=True,
        cursorclass=pymysql.cursors.DictCursor
    )
