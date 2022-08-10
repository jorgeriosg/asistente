#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import traceback
import codecs

from flask import jsonify

import middleware_utils as core
from mods import request_validator
from mods import datadog as dg
from mods import validator


def message(request_body: dict):
    try:
        start_time = time.time()

        # si conversacion no fue iniciada, iniciar..
        if request_body['cid'] is None and request_body['msg'] is None and "consulta" not in request_body:
            response_obj = core.saludo_bd()
            # response_obj["cid"] = core.start_conversation(0)
            return jsonify(response_obj)
        elif "consulta" in request_body and request_body["consulta"] == "":
            response_obj = core.saludo_bd()
            # response_obj["cid"] = core.start_conversation(0)
            return jsonify(response_obj)

        msg = validator.validate_xss2(request_body['msg'])

        message_input = {'text': msg}
        if 'clientIp' not in request_body.keys():
            request_body.update({'clientIp': 0})
        else:
            request_body['clientIp'] = validator.validate_xss2(request_body['clientIp'])

        if request_body['cid'] is None:
            request_body['cid'] = core.start_conversation(request_body['clientIp'])
        cid = validator.validate_xss2(request_body['cid'])
        if "consulta" in request_body and request_body["consulta"] != "":
            ret = core.los_heroes_interacion(request_body, request_body['clientIp'], start_time)
            if ret:
                return jsonify(ret)
        if "consulta" in request_body and request_body["consulta"] == "":
            request_body.pop("consulta")
        ret = core.chit_chat_interaction(message_input, cid, request_body['clientIp'], start_time)
        if ret:
            return jsonify(ret)
        ret = core.atomico_interaction(message_input, cid, request_body['clientIp'], start_time)
        if ret:
            return jsonify(ret)

        # intentamos faq-tx
        ret = core.los_heroes_interacion(request_body, request_body['clientIp'], start_time)
        if ret:
            return jsonify(ret)
    except Exception as unknown_exception:
        dg_info = {
            'Message': f'Unknown exception at integration -> message function: {unknown_exception}',
            'Traceback': traceback.format_exc()
        }
        dg.log_datadog(dg_info)
