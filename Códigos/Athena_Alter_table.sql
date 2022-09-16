-- DATA SOURCE: EPDATA
alter table evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten
    replace columns (
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
    Resto_del_mundo	decimal(8,2)
    );

describe evolucion_de_las_emisiones_de_co2_procedentes_del_carbon_en_los_paises_que_mas_emiten;

alter table asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera
    replace columns (
    Anio	int	,
    Periodo	char(11)	,
    CO2	decimal(8,2)	    
    );

describe asi_ha_aumentado_la_concentracion_de_co2_en_la_atmosfera;

alter table evolucion_de_la_concentracion_de_dioxido_de_carbono__co2__en_la_atmosfera
    replace columns (
    Anio	int	,
    Periodo	char(11)	,
    Media_mensual decimal(8,2)
    );

describe evolucion_de_la_concentracion_de_dioxido_de_carbono__co2__en_la_atmosfera;

alter table evolucion_en_las_emisiones_de_co2_procedentes_de_combustibles_fosiles_en_china__eeuu__india__rusia
    replace columns (
    Anio	int	,
    Periodo	char(4)	,
    China	decimal(8,2)	,
    EEUU	decimal(8,2)	,
    India	decimal(8,2)	,
    Rusia	decimal(8,2)	,
    Japon	decimal(8,2)	,
    Resto_del_mundo	decimal(8,2)
    );

describe evolucion_en_las_emisiones_de_co2_procedentes_de_combustibles_fosiles_en_china__eeuu__india__rusia;

alter table evolucion_en_las_emisiones_globales_de_co2_por_origen
    replace columns (
    anio	int	,
    Periodo	char(4)	,
    Carbon	decimal(8,2)	,
    Petroleo	decimal(8,2)	,
    Gas	decimal(8,2)	,
    Cemento	decimal(8,2)	,
    Combustion	decimal(8,2)	,
    Otros	decimal(8,2)	    
    );

describe evolucion_en_las_emisiones_globales_de_co2_por_origen;

alter table evolucion_en_las_emisiones_globales_de_co2_procedentes_de_combustibles_fosiles
    replace columns (
    anio	int	,
    Periodo	char(4)	,
    CO2_Millones decimal(8,2)
    );

describe evolucion_en_las_emisiones_globales_de_co2_procedentes_de_combustibles_fosiles;

alter table variacion_de_las_emisiones_de_gases_de_efecto_invernadero_respecto_a_1984
    replace columns (
    anio	int	,
    Periodo	char(4)	,
    CO2 decimal(8,2),
    CH4 decimal(8,2),
    N2O decimal(8,2)
    );

describe variacion_de_las_emisiones_de_gases_de_efecto_invernadero_respecto_a_1984;


-- DATA SORCE: IDEAM
alter table ideam
    replace columns (
    cod_div             	bigint,              	                    
    latitud             	double,              	                    
    longitud            	double,              	                    
    region              	string,              	                    
    departamento        	string,              	                    
    municipio           	string,              	                    
    fecha               	string,              	                    
    hora                	string,              	                    
    temperatura         	double,              	                    
    velocidad_viento	double,              	                    
    direccion_viento	double,              	                    
    presion             	double,              	                    
    punto_rocio      	double,              	                    
    cobertura_total_nubosa	double,              	                    
    precipitacion_mm_h	double,              	                    
    probabilidad_tormenta	double,              	                    
    humedad             	double,              	                    
    pronostico          	string  
    );

describe ideam;

-- DATA SOURCE: KAGGLE
alter table CO2_emission_of_cars_dataset
    replace columns (
    Car char(15)    ,
    Model char(15)  ,
    Volume char(15)  ,
    Weight int  ,
    CO2 int
    );

describe CO2_emission_of_cars_dataset;

alter table DATABANK_CO2_emissions
    replace columns (
    Time int	,
    Time_Code char(6)	,
    Country_Name char(55)	,
    Country_Code char(3)	,
    CO2_emissions char(2)   ,
    year  int
    );

describe DATABANK_CO2_emissions;

-- Consultas Exploración Datos

select Country_Name, count(*) 
from DATABANK_CO2_emissions
group by 1
order by 2
;

select *
from DATABANK_CO2_emissions
where Country_code = 'COL'
;

SELECT car, sum(co2) as co2 from
co2_emission_of_cars_dataset
group by 1
order by 2 desc
;

