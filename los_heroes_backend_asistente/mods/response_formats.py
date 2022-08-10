
from flask import jsonify, request
from mods import datadog as dg


def free_output_format(code, body, send_to_datadog: bool = True):
    """
    :param data:
    :return:
    """
    if send_to_datadog:
        data_to_datadog = {
            'request': None,
            'response': body
        }
        if request.is_json and request.method == 'POST':
            data_to_datadog['request'] = request.get_json()
        severity = 'ERROR' if 200 > code or code > 299 else 'INFO'
        dg.log_datadog(data_to_datadog, code, severity, str(request.path))
    return jsonify(body), code


def output_format(code, glosa, data=None, send_to_datadog: bool = True):
    """
    :param codigo:
    :param glosa:
    :param data:
    :return:
    """
    formated_data_object = {
        "estado": {
            "codigo": code,
            "glosa": glosa
        },
        "data": data
    }
    if send_to_datadog:
        data_to_datadog = {
            'request': None,
            'response': formated_data_object
        }
        if request.is_json and request.method == 'POST':
            data_to_datadog['request'] = request.get_json()
        severity = 'ERROR' if 200 > code or code > 299 else 'INFO'
        dg.log_datadog(data_to_datadog, code, severity, str(request.path))
    return jsonify(formated_data_object), code


def output_format_b64(codigo, glosa, data=None, b64=None, send_to_datadog: bool = True):
    """
    :param codigo:
    :param glosa:
    :param data:
    :return:
    """
    formated_data_object = {
        "estado": {
            "codigo": codigo,
            "glosa": glosa
        },
        "data": data,
        "b64": b64
    }
    if send_to_datadog:
        data_to_datadog = {
            'request': None,
            'response': formated_data_object
        }
        if request.is_json and request.method == 'POST':
            data_to_datadog['request'] = request.get_json()
        severity = 'ERROR' if 200 > code or code > 299 else 'INFO'
        dg.log_datadog(data_to_datadog, code, severity, str(request.path))
    return jsonify(formated_data_object), codigo
