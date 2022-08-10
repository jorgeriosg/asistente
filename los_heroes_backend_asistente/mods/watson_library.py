import datetime
import json
import requests
import pymysql
# from ibm_watson import AssistantV1
# from ibm_watson import AssistantV2
import traceback

from mods import datadog as dg
from mods import SqlConnection
from mods.config import CONV_INTENTOS, CONV_VERSION


class WatsonV1():
    """ Clase para conectarse con proyectos que utilizan instancias con Watson V1:
        Workspace ID
        Username
        Password
     """
    CRED_CC = []
    CRED_FAQ = []
    CRED_ATO = []
    CONV_FAQ = []
    CONV_CHITCHAT = []

    def __init__(self, url=None, version='2019-02-28'):
        self.CRED_FAQ, self.CRED_CC, self.CRED_ATO = self.credenciales_watson()

    def credenciales_watson(self):
        connection = SqlConnection.SqlConnection()
        query = "SELECT * FROM credenciales ORDER BY id;"
        parametros = ()
        credentials_found = connection.find(query)
        if isinstance(credentials_found, list) and len(credentials_found) > 0:
            existen_credenciales = True
        connection.close_connection()
        chitchat_cred = {}
        business_cred = {}
        atomico_cred = {}
        chitchat = 1
        business = 1
        atomico = 1
        if existen_credenciales:
            for res in credentials_found:
                if res['tipo'] == 'chitchat':
                    nombre = 'instancia%s' % (str(chitchat))
                    chitchat_cred[nombre] = {
                        'url': res['url'],
                        'wid': res['workspace'],
                        'clave': res['pass'],
                        'usuario': res['user'],
                        'instancia': res['instancia']
                    }
                    chitchat += 1

                if res['tipo'] == 'business':
                    nombre = 'instancia%s' % (str(business))
                    business_cred[nombre] = {
                        'url': res['url'],
                        'wid': res['workspace'],
                        'clave': res['pass'],
                        'usuario': res['user'],
                        'instancia': res['instancia']
                    }
                    business += 1

                if res['tipo'] == 'atomico':
                    nombre = 'instancia%s' % (str(atomico))
                    atomico_cred[nombre] = {
                        'url': res['url'],
                        'wid': res['workspace'],
                        'clave': res['pass'],
                        'usuario': res['user'],
                        'instancia': res['instancia']
                    }
                    atomico += 1
        return chitchat_cred, business_cred, atomico_cred

    def watson_call(self, input=None, context={}, tipo='business', intentos=0, traceback=None):
        """
        :param wid:
        :param input:
        :param context:
        :param type:
        :return:
        """
        conv_response = {
            'context': context
        }
        headers = {'Content-Type': 'application/json'}

        data = json.dumps(
            {
                'input': {
                    'text': input
                },
                'context': context
            }
        )
        if intentos == CONV_INTENTOS:
            desc = str(CONV_INTENTOS) + ' intentos de conexión'
            dg.log_datadog(f'Assistant no responde: {desc}')
        else:
            if intentos % 2 == 0:
                if tipo == 'business':
                    credenciales = self.CRED_CC['instancia1']
                elif tipo == 'atomico':
                    credenciales = self.CRED_ATO['instancia1']
                else:
                    credenciales = self.CRED_FAQ['instancia1']
                try:
                    conv_response = requests.post(credenciales['url'] + credenciales['wid'] + "/message?version=" + CONV_VERSION, auth=(credenciales['usuario'], credenciales['clave']), data=data, headers=headers, timeout=5)
                    if conv_response.status_code == 200:
                        conv_response = json.loads(conv_response.text)
                    else:
                        intentos += 1
                        return self.watson_call(input, context, tipo, intentos=intentos, traceback=conv_response.text)
                except requests.exceptions.Timeout as e:
                    intentos += 1
                    return self.watson_call(input, context, tipo, intentos=intentos, traceback=str(e))
                except Exception:
                    intentos += 1
                    return self.watson_call(input, context, tipo, intentos=intentos, traceback=traceback.format_exc())
            elif intentos % 2 == 1:
                if tipo == 'business':
                    credenciales = self.CRED_CC['instancia2']
                elif tipo == 'atomico':
                    credenciales = self.CRED_ATO['instancia2']
                else:
                    credenciales = self.CRED_FAQ['instancia2']
                try:
                    conv_response = requests.post(credenciales['url'] + credenciales['wid'] + "/message?version=" + CONV_VERSION, auth=(credenciales['usuario'], credenciales['clave']), data=data, headers=headers, timeout=5)
                    if conv_response.status_code == 200:
                        conv_response = json.loads(conv_response.text)
                    else:
                        intentos += 1
                        return self.watson_call(input, context, tipo, intentos=intentos, traceback=conv_response.text)
                except requests.exceptions.Timeout as e:
                    intentos += 1
                    return self.watson_call(input, context, tipo, intentos=intentos, traceback=str(e))
                except Exception:
                    intentos += 1
                    return self.watson_call(input, context, tipo, intentos=intentos, traceback=traceback.format_exc())
        return conv_response

    def chitchat_call(self, texto='', cid=None, context={}):
        conv_response = self.watson_call(input=texto, tipo='chitchat', context=context)
        texto_salida = conv_response['output']['generic'][0]['text']
        es_chitchat = False
        if texto_salida != 'NO CAMBIAR':
            es_chitchat = True
        return es_chitchat, texto_salida, conv_response
