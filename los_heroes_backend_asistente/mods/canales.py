#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
modulo de envio de correo
"""
import copy
import json
import requests
import base64
import traceback

from .SqlConnection import SqlConnection
from .config import URL_CANALES
from .datadog import log_datadog


def chunks(length, n):
    try:
        for i in range(0, len(length), n):
            yield length[i:i + n]
    except Exception:
        log_datadog(data=traceback.format_exc())


def envio_canales(mensajes=[], id_plantilla=1, custom_invoice_str: str = None):
    """
    {
        "emisor": "no-responder@anglo.pe",
        "tag": "TAG_UNICO_PARA_METRICAS",
        "nombre_email": "NOMBRE_QUE_APRECERA_EN_EMAIL",
        "canal": "2",
        "custom_args": {
            "tag": "TAG_UNICO_PARA_METRICAS"
        },
        "plantilla": "ACA VA @URL ",
        "asunto": "Seguimiento de su estado de salud COVID-19",
        "mensajes": [
            {
                "id_usuario_contactado": "{{DNI}}",
                "@URL": "https://www.asistente.cl",
                "receptor":"rardiles@cognitiva.la"
            }
        ]
    }
    """
    msql = SqlConnection()
    try:
        query = "SELECT * from plantillas_correos WHERE id = %s;"
        parametros = (id_plantilla,)
        plantilla = msql.find(query, parametros)
        if isinstance(plantilla, list):
            plantilla = plantilla[0]
            headers = {'Content-Type': 'application/json;charset=UTF-8'}
            send = {
                "emisor": plantilla['emisor'],
                "tag": plantilla['tag'],
                "nombre_email": plantilla['nombre_email'],
                "canal": str(plantilla['id_canal']),
                "custom_args": {
                    "tag": plantilla['tag']
                },
                "plantilla": plantilla['plantilla_html'],
                "asunto": plantilla['asunto'] if custom_invoice_str is None else plantilla['asunto'] % custom_invoice_str,
                "mensajes": mensajes
            }
            x = json.dumps(send)
            out_data = requests.post(URL_CANALES, headers=headers, data=x)
            return out_data
        else:
            log_datadog(plantilla)
            return False
    except Exception as unknown_exception:
        dg_log = {
            'Message': f'Unknown error at envio_canales: {unknown_exception}',
            'Traceback': traceback.format_exc()
        }
        log_datadog(dg_log)
    finally:
        msql.close_connection()


def enviar_correo_notificacion(id_datos: int, rut_persona: str, nombre_persona: str, medio_notificacion: str, dato_contacto: str, receptor: str, asunto: str):
    try:
        if (isinstance(receptor, str) and receptor != '') and (isinstance(asunto, str) and asunto != ''):
            mensajes = [
                {
                    "id_usuario_contactado": receptor,
                    "@ID_DATOS": id_datos,
                    "@RUT_PERSONA": rut_persona,
                    "@NOMBRE_PERSONA": nombre_persona,
                    "@MEDIO_NOTIFICACION": medio_notificacion,
                    "@DATO_CONTACTO": dato_contacto,
                    "receptor": receptor
                }
            ]
            asunto = asunto + f' ID:{id_datos}'
            envio_canales(mensajes=mensajes, id_plantilla=1, custom_invoice_str=asunto)
    except Exception as unknown_exception:
        dg_log = {
            'Message': f'Unknown error at enviar_correo_notificacion: {unknown_exception}',
            'Traceback': traceback.format_exc()
        }
        log_datadog(dg_log)
