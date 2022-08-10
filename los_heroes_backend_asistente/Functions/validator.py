#!/usr/bin/env python
# -*- coding: utf-8 -*-
import doctest
import re
from itertools import cycle
import datetime
import sys
import re
from itertools import cycle

class Validator:
    @staticmethod
    def validateDate(date, format='%d-%m-%Y'):
        """
        valida la fecha verificando formato y que exista
            :param date: fecha a verificar
            :format: formato de fecha
        """
        try:
            datetime.datetime.strptime(date, format)
        except ValueError:
            return False
        else:
            return True

    @staticmethod
    def validate_name(name):
        """
        verifica que nombre contenga solo letras
            :param name: nombre a validar
        """
        return re.search(r'[A-Za-záéíóúñÑ ]', name)

    @staticmethod
    def validate_movistar_phone(phone):
        """
        verifica que telefono tenga 9 o 15 numeros (formato movistar)
            :param phone: telefono a verificar
        """
        return re.search(r'\b(?:[0-9]{15}|[0-9]{9})\b', phone)

    @staticmethod
    def validate_header_rep(header_keys):
        """
        chequea si los headers entrantes coinciden con los establecidos por COBRANZA
            :param header_Keys: los header a verificar
        """
        valid_keys = ('RUT', 'NOMBRES', 'APELLIDO_PATERNO', 'APELLIDO_MATERNO', 'ESPECIALISTA', 'TERRITORIO', 'LINEA')
        # añadir headers que son invalidos en ()#
        invalid_headers = [j for i, j in zip(
            header_keys, valid_keys) if i != j]
        if invalid_headers:
            error_msg = ("HEADERS INVALIDADOS:(" +
                         " ".join(invalid_headers) + ")").strip()
            return {'errors': error_msg}
        return None



    @staticmethod
    def validate_header_los_heroes(header_keys):
        """
        chequea si los headers entrantes coinciden con los establecidos por los heroes
            :param header_Keys: los header a verificar
        """
        header_keys=list(header_keys)
        valid_keys = ['PERFIL', 'REGION', 'PROVINCIA', 'COMUNA', 'CATEGORIA', 'SUBCATEGORIA', 'RUT EMPRESA', 'DV EMPRESA', 'RAZON SOCIAL', 'NOMBRE', 'DESCRIPCION', 'INFORMACION_BENEFICIO', 'CONDICION_BENEFICIO', 'RANKING', 'DIRECCION', 'TELEFONO', 'STATUS', 'LINK']

        if header_keys[:18]!=valid_keys:
            var=list(set(valid_keys)-set(header_keys))
            return 'HEADERS INVALIDOS: {}'.format(var)
        # añadir headers que son invalidos en ()#
        #invalid_headers = [j for i, j in zip(
        #    header_keys, valid_keys) if i != j]
        #if invalid_headers:
        #    error_msg = ("HEADERS INVALIDADOS:(" +
        #                 " ".join(invalid_headers) + ")").strip()
        #    return {'errors': error_msg}
        return None

    @staticmethod
    def validate_header_producto(header_keys):
        """
        chequea si los headers entrantes coinciden con los establecidos por COBRANZA
            :param header_Keys: los header a verificar
        """
        valid_keys = ('CODIGO_PRODUCTO', 'NOMBRE_PRODUCTO', 'CODIGO_PRESENTACION', 'NOMBRE_PRESENTACION', 'CODIGO_MERCADO', 'NOMBRE_MERCADO')
        # añadir headers que son invalidos en ()#
        invalid_headers = [j for i, j in zip(
            header_keys, valid_keys) if i != j]
        if invalid_headers:
            error_msg = ("HEADERS INVALIDADOS:(" +
                         " ".join(invalid_headers) + ")").strip()
            return {'errors': error_msg}
        return None

    @staticmethod
    def validate_header_kardex(header_keys):
        """
        chequea si los headers entrantes coinciden con los establecidos por COBRANZA
            :param header_Keys: los header a verificar
        """
        valid_keys = (
        'RUT', 'NOMBRES', 'APELLIDO_PATERNO', 'APELLIDO_MATERNO', 'ESPECIALIDAD', 'SUB_ESPECIALIDAD', 'CLASIFICACION'
        , 'CELULAR', 'EMAIL', 'TIPO_CONSULTA', 'INSTITUCION', 'FONO_CONSULTA', 'PAIS', 'REGION', 'CIUDAD',
        'COMUNA', 'PISO', 'OFICINA', 'NUMERO', 'CALLE', 'CODIGO_POSTAL', 'VALOR_CONSULTA', 'HORARIO_LUNES',
        'HORARIO_MARTES', 'HORARIO_MIERCOLES', 'HORARIO_JUEVES', 'HORARIO_VIERNES', 'HORARIO_SABADO',
        'CODIGO_TERRITORIO')
        # añadir headers que son invalidos en ()#
        invalid_headers = [j for i, j in zip(
            header_keys, valid_keys) if i != j]
        if invalid_headers:
            error_msg = ("HEADERS INVALIDADOS:(" +
                         " ".join(invalid_headers) + ")").strip()
            return {'errors': error_msg}
        return None

    @staticmethod
    def validaRut2(texto):
        
        rut = ''
        try:
            verificador = ''
            find_digit = re.findall(r'\d+', texto)
            concat_digit = ''.join(find_digit)
    
            #en caso de que STT detecte millones como 000000 se escapan
            concat_digit = concat_digit.replace('000000','')
    
            if ('k' in texto or 'K' in texto or 'ca' in texto or 'CA' in texto) and len(concat_digit)<= 8:
                verificador = 'K'
            if ('cero' in texto or 'CERO' in texto or 'Cero' in texto or 'zero' in texto) and len(concat_digit)<= 8:
                verificador = '0'
            
            rut = concat_digit + verificador
            rut = rut.upper()
            # rut = rut.replace("-","")
            rut = rut.replace(".","")
            aux = rut[:-1]
            dv = rut[-1:]

            revertido = map(int, reversed(str(aux)))
            factors = cycle(range(2,8))
            s = sum(d * f for d, f in zip(revertido,factors))
            res = (-s)%11
            
            if len(rut) > 3 and rut[-2] != '-':
                rut = rut[0:-1]+'-'+rut[-1]
            if str(res) == dv:
                return True,rut
            elif (dv=="K" and res==10):
                lista = list(rut)
                lista[-1] = 'K'
                rut = ''.join(lista)
                return True,rut
            else:
                return False,rut
        except:
            return False,rut


    @staticmethod
    def validateRut(rut):
        """
        chequeo de rut que tenga la estructura de numeros guion y digito verificador
        y verificarlo con el digito o K final
            :param rut: rut en formato string
        """
        

        if not (re.search(r'^[0-9]+[-|‐]{1}[0-9kK]{1}$', rut)):
            return False;
        rut = rut.upper()
        # rut = rut.replace("-", "")
        rut = rut.replace(".", "")
        aux = rut[:-1]
        dv = rut[-1:]

        revertido = map(int, reversed(str(aux)))
        factors = cycle(range(2, 8))
        s = sum(d * f for d, f in zip(revertido, factors))
        res = (-s) % 11
    
        if str(res) == dv:
            return True
        elif dv == "K" and res == 10:
            return True
        else:
            return False

    @staticmethod
    def validateEmail(email):
        """
        El campo email cuando este venga cargado se valida que sea un validamente
        bien escrito NombreUsuario@dominio.ext
            :param email: mail a verificar
        """
        return re.search(r'[^@]+@[^@]+\.[^@]+', email)


    @staticmethod
    def validaFolio(folio):
        """
        El campo email cuando este venga cargado se valida que sea un validamente
        bien escrito NombreUsuario@dominio.ext
            :param email: mail a verificar
        """
        ret = re.findall(r'\d{2,10}',folio)
        if ret:
            return True,ret[0]
       
        return False,None
        
if __name__=='__main__':
    print(Validator.validaFolio('mi folio es 123456789 y vo vela'))