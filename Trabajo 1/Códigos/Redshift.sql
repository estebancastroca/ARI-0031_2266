-- Manejo Tablas Externas:
    create external schema clima_schema 
    from data catalog 
    database 'clima_db' 
    iam_role 'arn:aws:iam::317092156931:role/LabRole'
    create external database if not exists;

    drop table if exists clima_schema.evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten ;

    create external table clima_schema.evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten (
        Anio	int	,
        Periodo	char(4)	,
        China	decimal(8,2)	,
        Alemania	decimal(8,2)	,
        India	decimal(8,2)	,
        Indonesia	decimal(8,2)	,
        Japon	decimal(8,2)	,
        Rusia	decimal(8,2)	,
        Sudafrica	decimal(8,2)	,
        Corea_del_Sur	decimal(8,2)	,
        EEUU	decimal(8,2)	,
        Kazajistan	decimal(8,2)	,
        Resto_del_mundo	decimal(8,2))
    row format delimited
    fields terminated by ';'
    stored as textfile
    location 's3://testbucketari-2022/raw/epdata/asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera/'
    table properties ('numRows'='172000');

    -- Kaggle
    drop table if exists clima_schema.CO2_emission_of_cars_dataset;
    create external table clima_schema.CO2_emission_of_cars_dataset (
        Car char(15)    ,
        Model char(15)  ,
        Volume char(15)  ,
        Weight int  ,
        CO2 int)
    row format delimited
    fields terminated by ','
    stored as textfile
    location 's3://testbucketari-2022/raw/kaggle/CO2_emission_of_cars_dataset/'
    table properties ('numRows'='172000');

    select * from clima_schema.CO2_emission_of_cars_dataset;

    ---

    drop table if exists clima_schema.DATABANK_CO2_emissions;
    create external table clima_schema.DATABANK_CO2_emissions (
        Time int	,
        Time_Code char(6)	,
        Country_Name char(55)	,
        Country_Code char(3)	,
        CO2_emissions char(2))
    row format delimited
    fields terminated by ','
    stored as textfile
    location 's3://testbucketari-2022/raw/kaggle/DATABANK_CO2_emissions/'
    table properties ('numRows'='172000');

    select * from clima_schema.DATABANK_CO2_emissions ;

    -- IDEAM
    drop table if exists clima_schema.ideam;
    create external table clima_schema.ideam (
        cod_div             	bigint,              	                    
        latitud             	decimal(8,2)	,              	                    
        longitud            	decimal(8,2)	,              	                    
        region              	char(55) ,              	                    
        departamento        	char(55),              	                    
        municipio           	char(55),              	                    
        fecha               	char(55),              	                    
        hora                	char(55),              	                    
        temperatura         	decimal(8,2)	,                	                    
        velocidad_viento	decimal(8,2)	,                 	                    
        direccion_viento	decimal(8,2)	,                 	                    
        presion             	decimal(8,2)	,               	                    
        punto_rocio      	decimal(8,2)	,                	                    
        cobertura_total_nubosa	decimal(8,2)	,                	                    
        precipitacion_mm_h	decimal(8,2)	,                 	                    
        probabilidad_tormenta	decimal(8,2)	,               	                    
        humedad             	decimal(8,2)	,                	                    
        pronostico          	char(55))
    row format delimited
    fields terminated by ','
    stored as textfile
    location 's3://testbucketari-2022/raw/ideam/'
    table properties ('numRows'='172000');

    select * from clima_schema.ideam;

-- Manejo tablas Nativas:

    -- Kaggle 1
    drop table if exists CO2_emission_of_cars_dataset;

    create table CO2_emission_of_cars_dataset (
        Car char(15)    ,
        Model char(15)  ,
        Volume char(15)  ,
        Weight int  ,
        CO2 int     ,
        year int
    );


    COPY CO2_emission_of_cars_dataset FROM 's3://testbucketari-2022/raw/kaggle/CO2_emission_of_cars_dataset/CO2_emission_cars_dataset.csv'
    iam_role 'arn:aws:iam::317092156931:role/LabRole'
    delimiter ';' timeformat 'YYYY-MM-DD HH:MI:SS' region 'us-east-1';

-- Realizar una consulta con tablas externas y nativas:

select top 10 clima_schema.evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten.anio, 
        count(*)
from clima_schema.evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten, CO2_emission_of_cars_dataset
where clima_schema.evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten.anio = CO2_emission_of_cars_dataset.year
group by clima_schema.evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten.anio
order by 1 desc;


   



        
