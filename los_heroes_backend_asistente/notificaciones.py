from Functions.sql_functions import mysql_Connect
import traceback
from werkzeug.utils import secure_filename

from Functions.datadog import Datadog

import pandas as pd
import numpy as np
import os

import datetime

URL_EXCEL = ''
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}
EXCEL_FOLDER = os.path.dirname(__file__) + '/'

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def find_notificaciones():
    connection = mysql_Connect()
    try:
        query = """ SELECT id, nombre_mostrar as nombre, activo, prioridad, repeticiones, DATE_FORMAT(fecha_actualizacion,'%%d-%%m-%%Y') as fechaModificacion, desea_ser_contactado as deseaSerContactado, correo_titulo as correoTitulo, correo_receptor as correoReceptor, ingresar_telefono as ingresarTelefono, ingresar_correo as ingresarCorreo, estado_carga, detalle_errores
        FROM notificaciones"""
        parametros = ()
        with connection.cursor() as cursor:
            cursor.execute(query,parametros)
            result = cursor.fetchall()
            return 200,result
        return 400,'Error en consulta a base de datos'
    except Exception as e:
        desc = "fallo inesperado : " + str(e)
        return 400,desc
    finally:
       connection.close()

def find_notificacion(id_notificacion=''):
    connection = mysql_Connect()
    try:
        query = """ SELECT id, nombre_mostrar as nombre, activo, mensaje, prioridad, repeticiones, DATE_FORMAT(fecha_actualizacion,'%%d-%%m-%%Y') as fechaModificacion, desea_ser_contactado as deseaSerContactado, correo_titulo as correoTitulo, correo_receptor as correoReceptor, ingresar_telefono as ingresarTelefono, ingresar_correo as ingresarCorreo
        FROM notificaciones WHERE id = %s """
        parametros = (id_notificacion,)
        with connection.cursor() as cursor:
            cursor.execute(query,parametros)
            result = cursor.fetchone()
            return 200,result
        return 400,'Error en consulta a base de datos'
    except Exception as e:
        desc = "fallo inesperado : " + str(e)
        return 400,desc
    finally:
       connection.close()


def update_notificaciones(notificaciones=[]):
    connection = mysql_Connect()
    try:
        query = """UPDATE notificaciones SET 
        activo = %s,
        nombre_mostrar = %s,
        prioridad = %s,
        repeticiones = %s,
        mensaje = %s,
        desea_ser_contactado = %s,
        correo_titulo = %s,
        correo_receptor = %s,
        ingresar_telefono = %s,
        ingresar_correo = %s,
        fecha_actualizacion = CURRENT_TIMESTAMP
        WHERE id = %s """
        for notificacion in notificaciones:
            if 'id' in notificacion and notificacion['id']:
                parametros = (notificacion['activo'], notificacion['nombre'], notificacion['prioridad'], notificacion['repeticiones'], notificacion['mensaje'], notificacion['deseaSerContactado'], notificacion['correoTitulo'], notificacion['correoReceptor'], notificacion['ingresarTelefono'], notificacion['ingresarCorreo'], notificacion['id'],)
                with connection.cursor() as cursor:
                    cursor.execute(query,parametros)
                    connection.commit()
        query_historial = """ DELETE FROM notificaciones_historial """
        with connection.cursor() as cursor:
            cursor.execute(query_historial)
            connection.commit()
        return 200,notificacion
    except Exception:
        if notificacion and len(notificacion) > 0:
            return 400,notificacion
        else:
            return 400,[]
    finally:
        connection.close()

def buscar_notificacion_actual(total_historial=[],repeticiones=[],repeticiones_fijas=[],index=0,encontrado=False):
    if len(total_historial) == len(repeticiones):
        while encontrado == False:
            if index == len(repeticiones):
                repeticiones = [x + y for x, y in zip(repeticiones, repeticiones_fijas)]
                index, encontrado =  buscar_notificacion_actual(total_historial=total_historial,repeticiones=repeticiones, repeticiones_fijas=repeticiones_fijas, index=0,encontrado=encontrado)
            else:
                if total_historial[index] < repeticiones[index]:
                    encontrado = True
                    return index, encontrado
                else:
                    index += 1
                    index, encontrado =  buscar_notificacion_actual(total_historial=total_historial,repeticiones=repeticiones, repeticiones_fijas=repeticiones_fijas,index=index,encontrado=encontrado)
        return index, encontrado
    else:
        return index, encontrado

def buscar_notificaciones(rut=''):
    query = """ SELECT 
    notificaciones.id as id_notificacion,
    notificaciones.nombre as nombre_notificacion,
    notificaciones.nombre_mostrar as nombre_mostrar,
    notificaciones.mensaje as mensaje,
    notificaciones.desea_ser_contactado as desea_ser_contactado,
    notificaciones.correo_titulo as correo_titulo,
    notificaciones.correo_receptor as correo_receptor,
    notificaciones.ingresar_telefono as ingresar_telefono,
    notificaciones.ingresar_correo as ingresar_correo,
    notificaciones.es_servicio_coopeuch as es_servicio_coopeuch,
    CAST(
        SUM(
            CASE 
                WHEN notificaciones_historial.rut = %s THEN 1
                ELSE 0
            END
        ) 
    AS INT) as total_historial,
    notificaciones.repeticiones as repeticiones,
    notificaciones_usuarios.rut as rut,
    IFNULL(notificaciones_usuarios.nombre,'') as nombre,
    IFNULL(notificaciones_usuarios.custom1,'') as custom1,
    IFNULL(notificaciones_usuarios.custom2,'') as custom2,
    IFNULL(notificaciones_usuarios.custom3,'') as custom3,
    IFNULL(notificaciones_usuarios.custom4,'') as custom4,
    IFNULL(notificaciones_usuarios.custom5,'') as custom5
    FROM notificaciones_usuarios
    LEFT JOIN notificaciones
    ON notificaciones.id = notificaciones_usuarios.id_notificacion
    LEFT JOIN notificaciones_historial
    ON notificaciones_historial.notificacion = notificaciones.id
    WHERE (notificaciones_usuarios.rut = %s OR notificaciones_usuarios.rut IS NULL) AND notificaciones.activo = 1
    GROUP BY notificaciones.id
    ORDER by notificaciones.prioridad ASC  """
    rut = str(rut)
    rut = rut.replace('.','').replace('-','').upper()
    parametros = (rut,rut,)
    result = []
    connection = mysql_Connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query,parametros)
            result = cursor.fetchall()
            if result and len(result) > 0:
                total_historial = [int(f['total_historial']) for f in result]
                repeticiones = [int(f['repeticiones']) for f in result]
                index, encontrado = buscar_notificacion_actual(total_historial=total_historial,repeticiones=repeticiones, repeticiones_fijas=repeticiones)
                print("resultado de buscar_notificacion_actual: ", index, encontrado)
                if encontrado:
                    return True, 'existen notificaciones', result[index]
                else:
                    return False, 'no existen notificaciones', result
            else:
                return False, 'no existen notificaciones', result
    except Exception as e:
        desc = "fallo inesperado : " + str(e)
        return False, desc, result
    finally:
        connection.close()


def carga_usuarios_mantenedor(request):
    log = Datadog()
    try:
        url_formulario = URL_EXCEL
        if request.method == 'POST':
            f = request.files['archivo'] if 'archivo' in request.files and request.files['archivo'] else None
            id_notificacion = request.form['id'] if 'id' in request.form and request.form['id'] else ''
            if f and allowed_file(f.filename):
                filename = secure_filename(f.filename)
                filename_temp = filename.rsplit('.', 1)[0].lower()
                extension_temp = '.' + filename.rsplit('.', 1)[1].lower()
                fecha = datetime.datetime.now().strftime('_%Y-%m-%d_%H:%M:%S')
                new_filename = filename_temp + fecha + extension_temp
                f.save(os.path.join(EXCEL_FOLDER, new_filename))

                vals = pd.read_excel(open(EXCEL_FOLDER + '/' + new_filename, 'rb'), sheet_name=0)
                valores_dinamicos_df = vals.replace(np.nan, '', regex=True)

                usuarios = []
                for index,k in valores_dinamicos_df.iterrows():
                    rut = ''
                    nombre = ''
                    custom1 = ''
                    custom2 = ''
                    custom3 = ''
                    custom4 = ''
                    custom5 = ''
                    if 'RUT' in k and k['RUT']:
                        rut = str(k['RUT'])
                        rut = rut.replace('.','').replace('-','')
                    if 'NOMBRE' in k and k['NOMBRE']:
                        nombre = k['NOMBRE']
                    if 'CUSTOM1' in k and k['CUSTOM1']:
                        custom1 = k['CUSTOM1']
                    if 'CUSTOM2' in k and k['CUSTOM2']:
                        custom2 = k['CUSTOM2']
                    if 'CUSTOM3' in k and k['CUSTOM3']:
                        custom3 = k['CUSTOM3']
                    if 'CUSTOM4' in k and k['CUSTOM4']:
                        custom4 = k['CUSTOM4']
                    if 'CUSTOM5' in k and k['CUSTOM5']:
                        custom5 = k['CUSTOM5']

                    usuarios.append((id_notificacion, rut, nombre, custom1, custom2, custom3, custom4, custom5,))

                if usuarios:
                    parametros_usuarios = ','.join(str(e) for e in usuarios)

                    query_eliminacion = """ DELETE FROM notificaciones_usuarios WHERE id_notificacion = %s """ %(id_notificacion)

                    query = """ INSERT INTO notificaciones_usuarios (id_notificacion,rut,nombre,custom1,custom2,custom3,custom4,custom5) VALUES %s
                    ON DUPLICATE KEY UPDATE
                    nombre = VALUES(nombre),
                    fecha_actualizacion = CURRENT_TIMESTAMP,
                    custom1 = VALUES(custom1),
                    custom2 = VALUES(custom2),
                    custom3 = VALUES(custom3),
                    custom4 = VALUES(custom4),
                    custom5 = VALUES(custom5) """ % (parametros_usuarios)
                    connection = mysql_Connect()
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(query_eliminacion)
                            connection.commit()
                    except Exception:
                        log.send_log("error en query " + traceback.format_exc() )
                        return 400, 'Archivo No cargado.'
                    finally:
                        connection.close()

                    connection = mysql_Connect()
                    try:
                        query_historial = """ DELETE FROM notificaciones_historial """
                        with connection.cursor() as cursor:
                            cursor.execute(query_historial)
                            connection.commit()
                    except Exception:
                        log.send_log("error en query " + traceback.format_exc() )
                        return 400, 'Archivo No cargado.'
                    finally:
                        connection.close()

                    connection = mysql_Connect()
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(query)
                            connection.commit()
                        return 200, 'Archivo Cargado'
                    except Exception:
                        log.send_log("error en query " + traceback.format_exc() )
                        return 400, 'Archivo No cargado.'
                    finally:
                        connection.close()

                    return 200, 'Archivo Cargado'
                else:
                    return 400, 'Sin datos para cargar.'
        else:
            return 400, 'método incorrecto'
    except Exception:
        log.send_log("error en query " + traceback.format_exc() )
        return 400, 'error: ' + traceback.format_exc()


def guardar_notificacion(rut='', id_notificacion='', nombre_notificacion='', cid=''):
    connection = mysql_Connect()
    query = """ INSERT INTO notificaciones_historial(rut,notificacion) VALUES (%s,%s) """
    query_historial = """ INSERT INTO notificaciones_historico(rut,notificacion,nombre_notificacion,cid) VALUES (%s,%s,%s,%s) """
    parametros = (rut.replace('.','').replace('-','').upper(), id_notificacion)
    parametros_historico = (rut.replace('.','').replace('-','').upper(), id_notificacion, nombre_notificacion, cid)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query,parametros)
            cursor.execute(query_historial,parametros_historico)
            connection.commit()
            return True, 'insertado'
        return False, 'no insertado'
    except Exception as e:
        desc = "fallo inesperado : " + str(e)
        return False, desc
    finally:
        connection.close()

def guardar_notificacion_noafiliados(rut='', id_notificacion='', nombre_notificacion='', cid=''):
    connection = mysql_Connect()
    query_historial = """ INSERT INTO notificaciones_historico_noafiliados(rut,notificacion,nombre_notificacion,cid) VALUES (%s,%s,%s,%s) """
    parametros_historico = (rut.replace('.','').replace('-','').upper(), id_notificacion, nombre_notificacion, cid)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query_historial,parametros_historico)
            connection.commit()
            return True, 'insertado'
        return False, 'no insertado'
    except Exception as e:
        desc = "fallo inesperado : " + str(e)
        return False, desc
    finally:
        connection.close()

