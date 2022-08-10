#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback

from flask import jsonify
from flask import request

from mods import datadog as dg
from mods import response_formats


def exception_message(function):
    """
    A decorator that wraps the passed in function and logs
    exceptions should one occur
    """
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as e:
            cid = None
            try:
                data = request.get_json()
                cid = data['cid']
            except Exception:
                pass
            dg_info = {
                'Message': f'Unknown error at {function.__name__}',
                'Traceback': traceback.format_exc()
            }
            dg.log_datadog(dg_info)
            response_body = {
                "codigo": 200,
                "glosa": str(e),
                "msg": "Lo sentimos, en estos momentos nuestros sistemas presentan problemas. Inténtelo mas tarde.",
                "cid": cid
            }
            return response_formats.free_output_format(200, response_body)

    wrapper.__name__ = function.__name__
    return wrapper
