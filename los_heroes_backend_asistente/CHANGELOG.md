# Changelog

Versiones estables de Los Heroes

## QA [2.0.11] - 2021-06-18
### Changed
- Se modifica el flujo de notificaciones para validar que este activa la solicitud de contacto de estas, es decir que cuando en el front este activado o desactivado la solicitud de contacto o la solicitud de correo o telefono (todo esto se activa en el front con unos botones, en base de datos 1 = Activado, 2 = Desactivado)

### Added
- Se agrega proceso de envio de correos con el mod canales, en donde se genera el correo en base a plantillas extraido de Anglo, las plantillas seran almacenadas en una tabla de base de datos de manera que se puedan modificar sin necesidad de modificar codigo

- Base de datos:
    - Ejecutar script del archivo ./sql/plantillas_correos.sql marcado como RELEASE 2.0.11:
        - Create or replace de tabla plantillas_correos.
        - Insert de la primera plantilla asociada a las notificaciones.

de [@acastillo].

## QA [2.0.10] - 2021-06-15
### Added
- Se agrega registro en tablas de etl para la nueva columna observaciones de la tabla licencias_medicas, la carga de archivos la genera un codigo de github que recibe los archivos desde un cron automatico, insertando la data en la base de datos correspondiente con la estructura de datos de unas tablas de template descritas en el confluence de los heroes en la siguiente url:
    - https://cognitivacl.atlassian.net/wiki/spaces/LO/pages/617349121/Los+Heroes+-+Lectura+y+carga+de+datos+Licencias+M+dicas
- Se agrega nueva columna en base de datos de los heroes para incluir la columna observaciones.

- Base de datos:
    - Ejecutar el script del archivo ./sql/licencias_medicas.sql marcado como release 2.0.10:
        - Alter table que agrega columna observaciones al final de la tabla licencias_medicas.

de [@acastillo].

## QA [2.0.9] - 2021-06-15
### Changed
- Se modifica flujo de datos para la incorporacion de no afiliados, se mueve la validacion de rut posterior al ingreso de este, ya que inicialmente se pregunta si quiere o no entregarlo
- Se agrega proceso de si esta activo por base de datos la pregunta de rut inicial y la entrega de la notificacion especial

de [@acastillo].
## QA [2.0.8] - 2021-06-08
### Changed
- Se agrega nueva columna a tabla consentimientos_usuario para realizar la vinculacion del rut con el dato del contacto a utilizar.
- Se refactoriza codigo, este cambio es masivo ya que se necesita recatorizar practicamente todo el codigo, se parte por el endpoint de /mensajes y se pretende extraer las integraciones de los toAnswers
- Se modifica datadog para utilizar la libreria transversal para eliminar la utilizada como clase sin sentido.
- Se mueve la libreria de watson a mods eliminando la que existia en Functions

- Base de datos:
    - Ejecutar script del archivo ./sql/consentimiento_usuarios.sql marcado como release 2.0.8
        - Alter table de consentimiento_usuarios

### Added
- Carpeta completa de mods con librerias correspondientes.
- Se agrega nuevo configs para eliminar el settings en carpeta raiz.
- Se agrega carpeta sql para las modificacioens progresivas de base de datos.

de [@acastillo].

## [2.0.0] - 2021-02-09
### Changed
- Se cambia versión de python2.7 a python3.8

### Added
- Alta disponibilidad 
- Datadog para loggear información de los servicios
- Modularización de Assistant

de [@rardilesg].


