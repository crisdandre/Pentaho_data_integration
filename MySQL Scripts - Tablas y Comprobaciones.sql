# Crear esquema
CREATE SCHEMA `pec_dataset`;

# Crear campos de tabla staging para conexion con dataset via Pentaho
CREATE TABLE `pec_dataset`.`staging` (
  `fechacreacion` DATE NULL,
  `numeroincidente` VARCHAR(25) NULL,
  `descripcion` TEXT(105) NULL,
  `servicio` VARCHAR(50) NULL,
  `tipodeservicio` VARCHAR(50) NULL,
  `prioridad` VARCHAR(25) NULL,
  `estado` VARCHAR(25) NULL,
  `torre` VARCHAR(50) NULL,
  `entorno` TEXT(105) NULL,
  `estadosla` VARCHAR(50) NULL,
  `duraciondias` FLOAT NULL);

# Verificar conexion y mapeo correcto de datos de la table staging con Pentaho/MySQL

SELECT *
from staging;

SELECT count(numeroincidente)
FROM staging;

SELECT prioridad, count(prioridad)
FROM staging
GROUP BY prioridad
ORDER BY prioridad;

SELECT prioridad, estado, count(prioridad)
FROM staging
GROUP BY prioridad, estado
ORDER BY prioridad, estado;


# ------ TABLAS DE DIMENSIONES ---------

# dservicio
CREATE TABLE `pec_dataset`.`dservicio` (
  `idservicio` INT NOT NULL,
  `servicio` VARCHAR(50) NULL,
  `torre` VARCHAR(50) NULL,
  PRIMARY KEY (`idservicio`));
  
SELECT *
FROM dservicio;
  
# dtipodeservicio
  CREATE TABLE `pec_dataset`.`dtipodeservicio` (
  `idtipodeservicio` INT NOT NULL,
  `tipodeservicio` VARCHAR(50) NULL,
  `clasificacion_tiposervicio` VARCHAR(25) NULL,
  PRIMARY KEY (`idtipodeservicio`));
  
SELECT *
FROM dtipodeservicio;
  
# dentorno
  CREATE TABLE `pec_dataset`.`dentorno` (
  `identorno` INT NOT NULL,
  `entorno` VARCHAR(25) NULL,
  PRIMARY KEY (`identorno`));
  
SELECT *
FROM dentorno;

# dprioridad
CREATE TABLE `pec_dataset`.`dprioridad` (
  `idprioridad` INT NOT NULL,
  `prioridad` VARCHAR(25) NULL,
  PRIMARY KEY (`idprioridad`));
  
SELECT *
FROM dprioridad;
  
# destado
  CREATE TABLE `pec_dataset`.`destado` (
  `idestado` INT NOT NULL,
  `estado` VARCHAR(25) NULL,
  `estadosla` VARCHAR(50) NULL,
  PRIMARY KEY (`idestado`));

SELECT *
FROM destado;

# ------ TABLA DE HECHOS ---------

CREATE TABLE `pec_dataset`.`fincidentes` (
  `fechacreacion` DATE NULL,
  `numeroincidente` VARCHAR(25) NULL,
  `duraciondias` FLOAT NULL,
  `idservicio` INT NOT NULL,
  `idtipodeservicio` INT NOT NULL,
  `idprioridad` INT NOT NULL,
  `idestado` INT NOT NULL,
  `identorno` INT NOT NULL,
  PRIMARY KEY (`numeroincidente`, `idservicio`, `idtipodeservicio`, `idprioridad`, `idestado`, `identorno`));

SELECT *
FROM fincidentes;

SELECT count(numeroincidente)
FROM fincidentes
WHERE idestado <> 5 and idestado <> 6 and idestado <> 7 and idestado <> 8


# ------ COMPROBACIONES --------

# Volumetría de tickets según el entorno

SET @total := (SELECT count(numeroincidente) FROM fincidentes 
			   WHERE idestado <> 5 and idestado <> 6 and idestado <> 7 and idestado <> 8);

SELECT dent.entorno as entorno, 
	   ROUND(((count(f.numeroincidente)/@total)*100),2) as Porcentaje
FROM fincidentes f
JOIN dentorno dent on f.identorno = dent.identorno
WHERE f.idestado <> 5 and f.idestado <> 6 and f.idestado <> 7 and f.idestado <> 8
GROUP BY entorno
ORDER BY Porcentaje DESC;


# Evolución número de tickets y por incidencias/peticiones

#total
SELECT DATE_FORMAT(fechacreacion, '%Y-%m') as anyo_mes, 
count(numeroincidente) as conteo
FROM fincidentes f
JOIN destado dest on f.idestado = dest.idestado
WHERE estado <> 'Cancelado'
GROUP BY anyo_mes
ORDER BY anyo_mes;

#total por incidencias/peticiones
SELECT DATE_FORMAT(f.fechacreacion, '%Y-%m') as anyo_mes, 
       dtipo.clasificacion_tiposervicio as tipo,
	   count(f.numeroincidente) as conteo 
FROM fincidentes f
JOIN dtipodeservicio dtipo on f.idtipodeservicio = dtipo.idtipodeservicio
JOIN destado dest on f.idestado = dest.idestado
WHERE estado <> 'Cancelado'
GROUP BY anyo_mes, tipo
ORDER BY anyo_mes, tipo DESC;

# Servicios con mayor cantidad de tickets y su porcentaje de cumplimiento de SLA

SELECT totales_servicios.servicio, total_tickets,
       ROUND((tickets_slaincumplidos/total_tickets)*100, 2) as procentaje_incumplimiento
FROM (
SELECT servicio, count(tickets) as total_tickets
FROM (
SELECT dser.servicio as servicio, dest.estado as estado, 
f.numeroincidente as tickets
FROM fincidentes f
JOIN dservicio dser on f.idservicio = dser.idservicio
JOIN destado dest on f.idestado = dest.idestado
) as servicio_estado
WHERE estado <> 'Cancelado'
GROUP BY servicio
ORDER BY total_tickets DESC
) as totales_servicios 

JOIN (

SELECT servicio, count(tickets) as tickets_slaincumplidos
FROM (
SELECT dser.servicio as servicio, dest.estado as estado, 
f.numeroincidente as tickets, dest.estadosla as sla
FROM fincidentes f
JOIN dservicio dser on f.idservicio = dser.idservicio
JOIN destado dest on f.idestado = dest.idestado
) as incumplidos_servicios
WHERE estado <> 'Cancelado' and sla = 'Incumplido'
GROUP BY servicio) as incumplidos 
on totales_servicios.servicio = incumplidos.servicio
ORDER BY total_tickets DESC
LIMIT 5;

# Servicios con mayor backlog

SELECT servicio, count(tickets) as conteo_tickets
FROM (
SELECT dser.servicio as servicio, dest.estado as estado, 
f.numeroincidente as tickets
FROM fincidentes f
JOIN dservicio dser on f.idservicio = dser.idservicio
JOIN destado dest on f.idestado = dest.idestado
) as servicio_estado
WHERE estado = 'Asignado' or estado = 'Pendiente' and estado <> 'Cancelado'
GROUP BY servicio
ORDER BY conteo_tickets DESC
LIMIT 5;

# Servicios con mayor y menor tiempo de resolución

#con mayor tiempo
SELECT servicio, ROUND(avg(duracion), 2) as  promedio_duracion
FROM (
SELECT dser.servicio as servicio, f.duraciondias as duracion, 
       dest.estado as estado	  
FROM fincidentes f
JOIN dservicio dser on f.idservicio = dser.idservicio
JOIN destado dest on f.idestado = dest.idestado
) as servicio_estado
WHERE estado <> 'Cancelado'
GROUP BY servicio
ORDER BY promedio_duracion DESC
LIMIT 5;

#con menor tiempo
SELECT servicio, ROUND(avg(duracion), 2) as promedio_duracion
FROM (
SELECT dser.servicio as servicio, f.duraciondias as duracion, 
       dest.estado as estado	  
FROM fincidentes f
JOIN dservicio dser on f.idservicio = dser.idservicio
JOIN destado dest on f.idestado = dest.idestado
) as servicio_estado
WHERE estado <> 'Cancelado'
GROUP BY servicio
ORDER BY promedio_duracion
LIMIT 5;

# Variación de la prioridad de los tickets en los últimos meses

SELECT total_por_fechaprioridad.anyo_mes, prioridad,
       ROUND((conteo_mes_por_prioridad/conteo_total_mes)*100, 2) as procentaje_tickets
FROM (
SELECT anyo_mes, prioridad, count(tickets) as conteo_mes_por_prioridad
FROM (
SELECT DATE_FORMAT(fechacreacion, '%Y-%m') as anyo_mes, dest.estado as estado, 
f.numeroincidente as tickets, dpri.prioridad as prioridad
FROM fincidentes f
JOIN dprioridad dpri on f.idprioridad = dpri.idprioridad
JOIN destado dest on f.idestado = dest.idestado
) as fecha_prioridad
WHERE estado <> 'Cancelado'
GROUP BY anyo_mes, prioridad
ORDER BY anyo_mes
) as total_por_fechaprioridad

JOIN (

SELECT DATE_FORMAT(fechacreacion, '%Y-%m') as anyo_mes, 
count(numeroincidente) as conteo_total_mes
FROM fincidentes f
JOIN destado dest on f.idestado = dest.idestado
WHERE estado <> 'Cancelado'
GROUP BY anyo_mes
ORDER BY anyo_mes) as total_por_fecha
on total_por_fechaprioridad.anyo_mes = total_por_fecha.anyo_mes
ORDER BY total_por_fechaprioridad.anyo_mes DESC, procentaje_tickets DESC
LIMIT 24;