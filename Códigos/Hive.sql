select * from datos_clima.ideam
;

select
select 
t1.anio
, avg(t1.co2) as co2
, avg(t2.carbon) as carbon
, avg(t2.petroleo) as petroleo 
from datos_clima.asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera t1
inner join datos_clima.evolucion_en_las_emisiones_globales_de_co2_por_origen t2
on t1.anio = t2.anio
group by t1.anio
order by t1.anio
limit 5
;



-- Consulta Hive creando tabla apuntando a Glue:
    create table hive_datos_clima.query1 as
    select 
    t1.anio
    , avg(t1.co2) as co2
    , avg(t2.carbon) as carbon
    , avg(t2.petroleo) as petroleo 
    from datos_clima.asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera t1
    inner join datos_clima.evolucion_en_las_emisiones_globales_de_co2_por_origen t2
    on t1.anio = t2.anio
    group by t1.anio
    order by t1.anio
    limit 5