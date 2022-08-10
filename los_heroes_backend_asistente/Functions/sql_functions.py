import pymysql
import losheroes_settings as cf
import re
from mods import validator
from mods import SqlConnection


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

# base: ardiles rut validar...


def buscar_rut(msg_input, intent):

    verified, rut = validator.verify_rut(msg_input)
    if not verified:
        return None, None
    connection = SqlConnection.SqlConnection()

    query = "select folio from licencias_medicas where rut = %(rut)s;" if intent == 'estado_de_licencias' else \
        "select 1 from afiliados where rut = %(rut)s;"

    found = connection.find(query, {'rut': rut})

    return (True, rut) if isinstance(found, list) and len(found) > 0 else (False, None)


def guardar_consentimiento(consentimiento, email="", telefono=""):
    connection = SqlConnection.SqlConnection()
    query = "INSERT INTO consentimiento_usuario(desea_ser_contactado, telefono, email) VALUES (%s,'%s','%s')".format("1" if consentimiento else "0", telefono, email)
    result = connection.alter(query)
    return True if result else False


def buscar_data_folio(rut, msg_input):

    verified, folio = validator.verify_folio(msg_input)
    if not verified:
        print('folio invalido')
        return {
            'user_folio': None
        }
    connection = SqlConnection.SqlConnection()
    query = """select rut,
        ESTADO_LICENCIA as user_estado_licencia,
        DATE_FORMAT(FECHA_PROBABLE_PAGO,'%%Y/%%m/%%d') as user_fecha_probable_pago,
        DATE_FORMAT(FECHA_PAGO,'%%Y/%%m/%%d') as user_fecha_pago,
        SUCURSAL_PAGO as user_sucursal_pago,
        TIPO_PAGO as user_tipo_pago,
        DATE_FORMAT(FECHA_PAGO_EFECTIVO,'%%Y/%%m/%%d') as user_fecha_pago_efectivo,
        nullif(TRIM(OBSERVACIONES),'') as observaciones,
        nullif(TRIM(DOCUMENTO1),'') as documento1,
        nullif(TRIM(DOCUMENTO2),'') as documento2,
        nullif(TRIM(DOCUMENTO3),'') as documento3,
        nullif(TRIM(DOCUMENTO4),'') as documento4,
        nullif(TRIM(DOCUMENTO5),'') as documento5,
        nullif(TRIM(DOCUMENTO6),'') as documento6,
        nullif(TRIM(DOCUMENTO7),'') as documento7,
        nullif(TRIM(DOCUMENTO8),'') as documento8
        from licencias_medicas where rut = %(rut)s and folio = %(folio)s;"""
    folio_found = connection.find(query, {'rut': rut, 'folio': folio})

    if isinstance(folio_found, list) and len(folio_found) > 0:
        folio_found = folio_found[0]
        folio_found['user_folio'] = True
        return folio_found
    else:
        return {'user_folio': False}


if __name__ == '__main__':
    print(buscar_rut('mi rut es 15.392.320-5'))
