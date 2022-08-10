#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
modulo de envio de correo
"""
import json
import requests
import socket
import traceback
from losheroes_settings import DATADOG_URL, DATADOG_API_KEY, DATADOG_TAGS, DATADOG_SERVICE, DATADOG_SOURCE


def log_datadog(data='', status_code=500, severity: str = 'ERROR', url_path: str = None):
    try:
        url = DATADOG_URL + DATADOG_API_KEY
        headers = {
            'Content-Type': 'application/json'
        }
        try:
            hostname = socket.gethostbyname(socket.gethostname())
        except Exception as e:
            hostname = 'Local'

        body = {
            'message': severity + ': ' + str(data),
            'hostname': hostname,
            'ddtags': DATADOG_TAGS,
            'service': DATADOG_SERVICE,
            'ddsource': DATADOG_SOURCE,
            'status_code': status_code,
            'severity': severity
        }
        if url_path is not None:
            body['http'] = {
                'url_details': {
                    'path': url_path
                }
            }
        requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
    except Exception as e:
        pass
