/* --- RELEASE 2.0.8 --- */

ALTER TABLE consentimiento_usuario
    ADD COLUMN rut_usuario varchar(20),
    ADD COLUMN fecha_creacion datetime default now();

/* --- */
