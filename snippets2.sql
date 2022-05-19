--busqueda de fuentes
SELECT distinct proname
FROM pg_catalog.pg_namespace namespace
         JOIN pg_catalog.pg_proc proc
              ON pronamespace = namespace.oid
WHERE nspname = 'public'
  and prosrc like '%insert into tbl_base_control_calidad_corp%';

call pa_act_func_bi();

--querie para reprocesar los KPIS DE PLEX
call pdw_servicios_activos_fallas('31-dec-2021');
call pdw_kpi_plex_llamado_total('31-dec-2021');
call kpi_metricas_empleados_mensuales_plex_v2('31-dec-2021');
call pdw_comisiones_plex('202112');

--querie para obtener el listado de metas de los empleados con su region
with empleados_general as (
    select distinct id_employee, full_name, region
    from bicc.vw_regiones_app_empleado_2 a
    where full_name not in ('HONDURAS OPERACIONES', 'HONDURAS PLEX')
      and job_pos in ('Supervisor Planta Externa HFC',
                      'Supervisor Planta Externa HFC - Múltiple',
                      'Técnico Planta Externa HFC')),
     metas_empleados as (
         select uuid, full_name, puesto, nombre_kpi, target, weight
         from bicc.vw_reporte_metas_app_kpi a
         where full_name not in ('HONDURAS OPERACIONES', 'HONDURAS PLEX'))
select a.id_employee, a.region, a.full_name, b.puesto, b.nombre_kpi, b.target, b.weight
from empleados_general a
         inner join metas_empleados b on a.id_employee = b.uuid;

--listado de empleados donde se aplicara el cambio
select distinct id_employee,
                full_name,
                case
                    when region in ('CENTRO', 'SUR', 'ORIENTE', 'NORORIENTE') then 'CENTRO-SUR'
                    when region in ('NORTE', 'LITORAL', 'OCCIDENTE') then 'NOR-OCCIDENTE' end zona
from bicc.vw_regiones_app_empleado_2 a
where full_name not in ('HONDURAS OPERACIONES', 'HONDURAS PLEX')
  and job_pos in ('Supervisor Planta Externa HFC',
                  'Supervisor Planta Externa HFC - Múltiple',
                  'Técnico Planta Externa HFC');

grant all privileges on vw_regiones_app_empleado_2 to "ruth.castaneda";

--metas por zona
select distinct full_name, nombre_kpi,target, weight, case
                    when full_name = 'Luis Miguel Dávila Ruiz' then 'CENTRO-SUR'
                    when full_name = 'Kelvin Joel Montes Gonzalez' then 'NOR-OCCIDENTE' end zona
from bicc.vw_reporte_metas_app_kpi
where puesto like '%Jefe de Planta Externa HFC%'
and nombre_kpi = 'AFECTACIÓN ALERTAS';

--queria de valicacion facturacion momv2
with facturacion_public as (
    select tipoingreso, sum(montomesactual) mes_actual, 'PUBLIC'
    from public.facturacion_momv2
    group by tipoingreso),
     facturacion_bicc as (
         select tipoingreso, sum(montomesactual) mes_actual, 'BICC'
         from bicc.facturacion_momv2
         group by tipoingreso)
select a.tipoingreso,
       b.tipoingreso,
       a.mes_actual                  mes_actual_public,
       b.mes_actual                  mes_actual_bicc,
       (a.mes_actual - b.mes_actual) dif_montos
from facturacion_public a
         full outer join facturacion_bicc b on a.tipoingreso = b.tipoingreso;

--queria de validacion facturacion momv2 y kpi de ingresos
with facturacion_mom_v2 as (
    select region, sum(montomesactual) monto_actual
    from bicc.facturacion_momv2
    where udn = 'Residencial'
      and estadopago = 'PAGADA'
    group by region),
     facturacion_kpis as (
         select region, sum(monto) ingresos
         from public.tbl_kpis_resumen_v2 a
         where a.pais = 'HONDURAS'
           and unidad_de_negocios = 'Residencial'
           and anio_mes = '202205'
         group by region)
select a.region, b.region, a.monto_actual, b.ingresos, (a.monto_actual - b.ingresos) dif_monto
from facturacion_mom_v2 a
         inner join facturacion_kpis b on a.region = b.region;








