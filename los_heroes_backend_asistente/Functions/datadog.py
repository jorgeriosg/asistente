import requests
import json
import losheroes_settings as config
import socket


class Datadog():
    def __init__(self):
        self.ddsource = 'backend'
        self.ddtags = 'env:staging,version:5.1'
        self.hostname = "local-integraciones"
        self.service = "los_heroes_dev_debug"
        self.api_key = '923862cebb7f5eabe9c8e07bd673be05'
        self.url = 'https://http-intake.logs.datadoghq.com/v1/input'

    def send_log(self, message, body=None):
        data = {
            'ddsource': self.ddsource,
            'ddtags': self.ddtags,
            'hostname': self.hostname,
            'message': message,
            'service': self.service,
        }

        if body:
            data['body'] = json.dumps(body)

        headers = {
            'Content-Type': 'application/json',
            'DD-API-KEY': self.api_key
        }

        response = requests.post(self.url, data=json.dumps(data), headers=headers)
        if response.status_code == 200:
            return True
        else:
            return False
