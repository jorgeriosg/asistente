/* ----- RELEASE 2.0.11 ------ */
CREATE OR REPLACE TABLE plantillas_correos (
    id int(11) unsigned auto_increment PRIMARY KEY,
    id_canal int NULL,
    emisor varchar(100) NULL,
    tag varchar(100) NULL,
    nombre_email varchar(100) NULL,
    plantilla_html text NULL,
    asunto varchar(100) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/* NOTE: The invoice it is handled from the front, as the process of making the email need to replace some text, %s is added as invoice */
INSERT INTO plantillas_correos (id_canal, emisor, tag, nombre_email, plantilla_html, asunto)
VALUES (
    2,
    'no-responder@cognitiva.la',
    'LOS_HEROES_QA',
    'Ema, asistente virtual Los Héroes',
    'Rut de afiliado: @RUT_PERSONA <br> Nombre del afiliado: @NOMBRE_PERSONA <br> Medio por el que quiere ser contactado: @MEDIO_NOTIFICACION <br> Dato de contacto: @DATO_CONTACTO <br> Id: @ID_DATOS <br> Saludos!',
    '%s'
);

/* ----- ------ */