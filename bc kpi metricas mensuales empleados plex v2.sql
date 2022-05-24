create procedure kpi_metricas_empleados_mensuales_plex_v2(p_fecha date DEFAULT (CURRENT_DATE - '1 day'::interval))
    language plpgsql
as
$$
DECLARE
    v_flag          BOOLEAN = FALSE;
    v_log           VARCHAR;
    v_month         VARCHAR = to_char(p_fecha, 'yyyymm');
    data            RECORD;
    v_count         INTEGER = 0;
    v_rows_affected INTEGER;
BEGIN

    delete
    from tbl_kpi_plex_ejecutado_empleado_v2
    where ejecutado_mes = v_month;

    delete
    from tbl_kpi_plex_cumplimiento_empleado_v2
    where kpi_mes = v_month;

    --contamos si hay registros en la tabla de ejecutado para el mes en cuestión
    v_log := 'Contando si existen registros para el mes % tabla tbl_kpi_plex_ejecutado_empleado_v2' v_month;
    RAISE NOTICE '%',v_log;
    SELECT count(*) INTO v_count FROM tbl_kpi_plex_ejecutado_empleado_v2 WHERE ejecutado_mes = v_month;

    --validamos si no hay registros previamente cargados en la tabla
    IF v_count = 0 THEN
        --al no haber registros, cargamos los del mes en curso
        v_log := 'Insertando datos de empleados y su ejecución por Ciudad';
        INSERT INTO tbl_kpi_plex_ejecutado_empleado_v2
        (ejecutado_mes, employee_id, employee_full_name,
         total_ordenes, ordenes_atendida_24_horas, cantidad_ordenes_plex,
         cantidad_fallas, cantidad_servicios_primarios,
         cantidad_recurrentes, numero_de_alertas, recurrentes_ordenes_plex,
         fallas_ordenes_plex, primarios_afectados, cortes, cortes_plex,
         primarios_hfc)
        SELECT v_month                               ejecutado_mes,
               ae.uuid                               employee_id,
               ae.full_name                          employee_full_name,
               sum(kpi.ordenes_atendida_24_horas) + sum(kpi.ordenes_no_atendidas_24) + sum(kpi.ordenes_anuladas) +
               sum(kpi.ordenes_pendientes)           total_ordenes,
               sum(kpi.ordenes_atendida_24_horas)    ordenes_atendida_24_horas,
               sum(kpi.cantidad_ordenes_plex)        cantidad_ordenes_plex,
               sum(kpi.cantidad_fallas)              cantidad_fallas,
               sum(kpi.cantidad_servicios_primarios) cantidad_servicios_primarios,
               sum(kpi.cantidad_recurrentes)         cantidad_recurrentes,
               sum(kpi.numero_de_alertas)            numero_de_alertas,
               sum(kpi.recurrentes_ordenes_plex)     recurrentes_ordenes_plex,
               sum(kpi.fallas_ordenes_plex)          fallas_ordenes_plex,
               sum(kpi.primarios_afectados)          primarios_afectados,
               sum(kpi.cortes)                       cortes,
               sum(kpi.cortes_plex)                  cortes_plex,
               sum(kpi.primarios_hfc)                primarios_hfc
        FROM bicc.app_employee ae
                 inner join bicc.vw_regiones_app_empleado b
                        on ae.uuid = b.id_employee
                 INNER JOIN public.tbl_kpis_plex kpi
                            ON upper(b.ciudad) = upper(kpi.ciudad)
                               and upper(b.pais) = upper(kpi.pais)
        WHERE kpi.anio_mes = v_month
              and ae.is_active = TRUE
          and ae.job_id in ('f229ae0c-e72b-4d94-9f03-c41388e3b7b0',
                            '521af091-4a73-4a1e-a3dc-2bf6596d64d9',
                            'ea69e43c-0268-482b-8b16-b75d7cd5f039',
                            '069a9aee-2ff6-4309-832f-dbb6c5681037',
                            '718987d0-2e9f-4ca5-bd7e-c45610d308c0'
            )
        and kpi.region != 'ISLAS'
        group by v_month, ae.uuid, ae.full_name;


        update tbl_kpi_plex_ejecutado_empleado_v2
        set region = reg,
            puesto = job_pos
        from (
                 select id_employee id, region reg, job_pos, count(1) regs
                 from bicc.vw_regiones_app_empleado
                 where job_pos_id in ('069a9aee-2ff6-4309-832f-dbb6c5681037',
                                      'ea69e43c-0268-482b-8b16-b75d7cd5f039')
                 group by id_employee, region, job_pos) b
        where employee_id = id
          and ejecutado_mes = v_month;


        update tbl_kpi_plex_ejecutado_empleado_v2
        set region = (case
                          when employee_id = 'fcdcbd8f-8d07-4dc9-9632-87839ee8ad7e' then 'HONDURAS'
                          when employee_id = 'd3d2c99d-b08a-429d-a724-9fdd1de99123' then 'NOR-OCCIDENTE'
                          when employee_id = '7ce617e4-1838-47b6-84e4-24708514b570' then 'CENTRO-SUR'
                          when employee_id = '8c62a910-faab-4c65-92e9-d4e8fac2a22c' then 'NORORIENTE,ORIENTE'
            end
            ),
            puesto = (case
                          when employee_id = 'fcdcbd8f-8d07-4dc9-9632-87839ee8ad7e' then 'Gerente de Planta Externa HFC'
                          when employee_id = 'd3d2c99d-b08a-429d-a724-9fdd1de99123' then 'Jefe de Planta Externa HFC'
                          when employee_id = '7ce617e4-1838-47b6-84e4-24708514b570' then 'Jefe de Planta Externa HFC'
                          when employee_id = '8c62a910-faab-4c65-92e9-d4e8fac2a22c'
                              then 'Supervisor Planta Externa HFC - Múltiple'
                end
                )
        where ejecutado_mes = v_month
          and employee_id in ('fcdcbd8f-8d07-4dc9-9632-87839ee8ad7e',
                              'd3d2c99d-b08a-429d-a724-9fdd1de99123',
                              '7ce617e4-1838-47b6-84e4-24708514b570',
                              '8c62a910-faab-4c65-92e9-d4e8fac2a22c');

        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        RAISE NOTICE '% , filas insertadas: %', v_flag, v_rows_affected;
    END IF;

    --en el caso que existan datos, para el mes en cuestión, se borrarán para ser
    --  sustituidos por los nuevos
    v_log = 'Borrando registros de la tabla tbl_kpi_plex_cumplimiento_empleado_v2 para el mes';
    RAISE NOTICE '%',v_log;
    DELETE
    FROM tbl_kpi_plex_cumplimiento_empleado_v2
    WHERE kpi_mes = v_month;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
    RAISE NOTICE '% , filas borradas: %', v_flag, v_rows_affected;

    --listamos cada empleado y sus kpis
    v_log = 'Lista de Empleados y sus KPIS vigentes';
    RAISE NOTICE '%',v_log;
    FOR data IN --kpis por empleado
        SELECT ae.uuid        employee_id,
               ae.full_name,
               aek.uuid       employee_kpi_id,
               ak.name        kpi_name,
               ak.description kpi_description,
               ak.unit_type,
               aek.start_date,
               aek.end_date,
               aek.weight,
               aek.target
        FROM bicc.app_employee ae
                 INNER JOIN bicc.app_employeekpi aek
                            ON ae.uuid = aek.employee_id AND aek.is_active = TRUE AND aek.deleted_at IS NULL
                 INNER JOIN bicc.app_kpi ak
                            ON aek.kpi_id = ak.uuid AND ak.is_available IS TRUE AND ak.deleted_at IS NULL
        WHERE ae.is_active IS TRUE
          and ae.job_id in ('f229ae0c-e72b-4d94-9f03-c41388e3b7b0',
                            '521af091-4a73-4a1e-a3dc-2bf6596d64d9',
                            'ea69e43c-0268-482b-8b16-b75d7cd5f039',
                            '069a9aee-2ff6-4309-832f-dbb6c5681037',
                            '718987d0-2e9f-4ca5-bd7e-c45610d308c0'
            )
        ORDER BY ae.uuid
        LOOP
            --insertamos el resultado del kpi para el empleado
            IF data.kpi_name = 'PEXTERNO 24 HORAS' THEN
                --si el KPI es Servicios Primarios, buscamos el empleado y sumamos su resultados para este kpi
                v_log = 'IF PEXTERNO 24 HORAS Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name,
                 employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado,
                 cumplimiento, resultado, resultado_2, region, puesto)
                SELECT a.ejecutado_mes,
                       a.employee_id,
                       a.employee_full_name,
                       a.employee_kpi_name,
                       a.kpi_description,
                       a.start_date,
                       a.weight,
                       a.target,
                       a.unit_type,
                       a.target_amount,
                       round(a.ejecutado * 100, 2),
                       a.cumplimiento,
                       CASE
                           WHEN (weight * a.cumplimiento) > weight THEN weight
                           ELSE (weight * a.cumplimiento) END              resultado,
                       CASE
                           WHEN a.cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * a.cumplimiento) > weight THEN weight
                                    ELSE (weight * a.cumplimiento) END END resultado_2,
                       region,
                       puesto
                FROM (
                         SELECT v_month                                               ejecutado_mes,
                                kpi.employee_id,
                                kpi.employee_full_name,
                                data.kpi_name                                         employee_kpi_name,
                                data.kpi_description                                  kpi_description,
                                data.start_date                                       start_date,
                                data.weight,
                                data.target,
                                data.unit_type,
                                round(sum(kpi.total_ordenes) * ((data.target / 100))) target_amount,
                                case
                                    when sum(kpi.ordenes_atendida_24_horas) = 0 then 0
                                    else round(sum(kpi.ordenes_atendida_24_horas) /
                                                      (sum(kpi.total_ordenes)), 2)
                                    end                                               ejecutado,
                                case
                                    when sum(kpi.ordenes_atendida_24_horas) = 0 then 0
                                    else (
                                                round(sum(kpi.ordenes_atendida_24_horas) /
                                                      (sum(kpi.total_ordenes)), 2)
                                                ) /
                                            (data.target / 100)
                                    end                                               cumplimiento,
                                kpi.region,
                                kpi.puesto
                         FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                         WHERE kpi.ejecutado_mes = v_month
                           AND kpi.employee_id = data.employee_id
                         GROUP BY v_month, kpi.employee_id, kpi.employee_full_name, data.kpi_name,
                                  data.kpi_description, data.start_date, data.weight,
                                  data.target, data.unit_type, kpi.region, kpi.puesto
                     ) a;
            ELSEIF data.kpi_name = 'ÓRDENES PLEX' THEN
                --si el KPI es CHURN, buscamos el empleado y sumamos su resultados para este kpi
                v_log = 'IF ORDENES PLEX Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name,
                 employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado,
                 cumplimiento, resultado, resultado_2, region, puesto)
                SELECT a.ejecutado_mes,
                       a.employee_id,
                       a.employee_full_name,
                       a.employee_kpi_name,
                       a.kpi_description,
                       a.start_date,
                       a.weight,
                       a.target,
                       a.unit_type,
                       a.target_amount,
                       a.ejecutado,
                       a.cumplimiento,
                       CASE
                           WHEN (weight * cumplimiento) > weight THEN weight
                           ELSE (weight * cumplimiento) END              resultado,
                       CASE
                           WHEN cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * cumplimiento) > weight THEN weight
                                    ELSE (weight * cumplimiento) END END resultado_2,
                       region,
                       puesto
                FROM (
                         SELECT v_month                        ejecutado_mes,
                                kpi.employee_id,
                                kpi.employee_full_name,
                                data.kpi_name                  employee_kpi_name,
                                data.kpi_description           kpi_description,
                                data.start_date                start_date,
                                data.weight,
                                data.target                    target,
                                data.unit_type,
                                data.target                    target_amount,
                                sum(kpi.cantidad_ordenes_plex) ejecutado,
                                case
                                    when sum(kpi.cantidad_ordenes_plex) < data.target
                                        then 1
                                    else 0 end                 cumplimiento,
                                kpi.region,
                                kpi.puesto
                         FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                         WHERE kpi.ejecutado_mes = v_month
                           AND kpi.employee_id = data.employee_id
                         GROUP BY kpi.employee_id,
                                  kpi.employee_full_name,
                                  kpi.region,
                                  kpi.puesto
                     ) a;
            ELSEIF data.kpi_name = 'FALLAS/PRIMARIOS' THEN

                v_log = 'IF FALLAS/PRIMARIOS Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name,
                 employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado,
                 cumplimiento, resultado, resultado_2, region, puesto)
                SELECT a.ejecutado_mes,
                       a.employee_id,
                       a.employee_full_name,
                       a.employee_kpi_name,
                       a.kpi_description,
                       a.start_date,
                       a.weight,
                       a.target,
                       a.unit_type,
                       a.target_amount,
                       a.ejecutado,
                       a.cumplimiento,
                       CASE
                           WHEN (weight * cumplimiento) > weight THEN weight
                           ELSE (weight * cumplimiento) END              resultado,
                       CASE
                           WHEN cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * cumplimiento) > weight THEN weight
                                    ELSE (weight * cumplimiento) END END resultado_2,
                       region,
                       puesto
                FROM (
                         SELECT v_month                 ejecutado_mes,
                                kpi.employee_id,
                                kpi.employee_full_name,
                                data.kpi_name           employee_kpi_name,
                                data.kpi_description    kpi_description,
                                data.start_date         start_date,
                                data.weight,
                                data.target             target,
                                data.unit_type,
                                data.target             target_amount,
                                round(case
                                          when sum(coalesce(kpi.cantidad_servicios_primarios, 0)) > 0 then
                                              sum(kpi.cantidad_fallas) / sum(kpi.cantidad_servicios_primarios)
                                          else 0
                                          end, 3) * 100 ejecutado,
                                case
                                    when sum(kpi.cantidad_servicios_primarios) > 0 then
                                        (case
                                             when round(((sum(kpi.cantidad_fallas) /
                                                          sum(kpi.cantidad_servicios_primarios)) * 100), 1) <=
                                                  data.target
                                                 then 1
                                             else 0 end)
                                    else 1 end          cumplimiento,
                                kpi.region,
                                kpi.puesto
                         FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                         WHERE kpi.ejecutado_mes = v_month
                           AND kpi.employee_id = data.employee_id
                         GROUP BY kpi.employee_id,
                                  kpi.employee_full_name,
                                  kpi.region,
                                  kpi.puesto
                     ) a;

            ELSEIF data.kpi_name = 'NUMERO ALERTAS' THEN
                --si el KPI es ATENCION ORDENES, buscamos el empleado y sumamos su resultados para este kpi
                v_log = 'IF NUMERO ALERTAS Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name,
                 employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado,
                 cumplimiento, resultado, resultado_2, region, puesto)
                SELECT a.ejecutado_mes,
                       a.employee_id,
                       a.employee_full_name,
                       a.employee_kpi_name,
                       a.kpi_description,
                       a.start_date,
                       a.weight,
                       a.target,
                       a.unit_type,
                       a.target_amount,
                       a.ejecutado,
                       a.cumplimiento,
                       CASE
                           WHEN (weight * cumplimiento) > weight THEN weight
                           ELSE (weight * cumplimiento) END              resultado,
                       CASE
                           WHEN cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * cumplimiento) > weight THEN weight
                                    ELSE (weight * cumplimiento) END END resultado_2,
                       region,
                       puesto
                FROM (
                         SELECT v_month                    ejecutado_mes,
                                kpi.employee_id,
                                kpi.employee_full_name,
                                data.kpi_name              employee_kpi_name,
                                data.kpi_description       kpi_description,
                                data.start_date            start_date,
                                data.weight,
                                data.target                target,
                                data.unit_type,
                                0                          target_amount,
                                sum(kpi.numero_de_alertas) ejecutado,
                                case
                                    when sum(kpi.numero_de_alertas) < data.target
                                        then 1
                                    else 0 end             cumplimiento,
                                region,
                                puesto
                         FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                         WHERE kpi.ejecutado_mes = v_month
                           AND kpi.employee_id = data.employee_id
                         GROUP BY kpi.employee_id,
                                  kpi.employee_full_name,
                                  kpi.region,
                                  kpi.puesto
                     ) a;
            ELSEIF data.kpi_name = 'RECURRENCIA PLEX' THEN
                --si el KPI es ARPU IN > ARPU OUT, buscamos el empleado y sumamos su resultados para este kpi
                v_log = 'IF RECURRENCIA PLEX Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name, employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado, cumplimiento, resultado, resultado_2,
                 region, puesto)
                select a.ejecutado_mes,
                       employee_id,
                       employee_full_name,
                       employee_kpi_name,
                       kpi_description,
                       start_date,
                       weight,
                       target,
                       unit_type,
                       target_amount,
                       ejecutado,
                       cumplimiento,
                       CASE
                           WHEN (weight * cumplimiento) > weight THEN weight
                           ELSE (weight * cumplimiento) END              resultado,
                       CASE
                           WHEN cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * cumplimiento) > weight THEN weight
                                    ELSE (weight * cumplimiento) END END resultado_2,
                       region,
                       puesto
                from (
                         SELECT a.ejecutado_mes,
                                a.employee_id,
                                a.employee_full_name,
                                a.employee_kpi_name,
                                a.kpi_description,
                                a.start_date,
                                a.weight,
                                a.target,
                                a.unit_type,
                                a.target_amount,
                                a.ejecutado,
                                case
                                    when a.ejecutado <= a.target_amount then 1
                                    else 0 end cumplimiento,
                                region,
                                puesto
                         FROM (
                                  SELECT v_month                 ejecutado_mes,
                                         kpi.employee_id,
                                         kpi.employee_full_name,
                                         data.kpi_name           employee_kpi_name,
                                         data.kpi_description    kpi_description,
                                         data.start_date         start_date,
                                         data.weight,
                                         data.target             target,
                                         data.unit_type,
                                         data.target             target_amount,
                                         round(case
                                                   when sum(coalesce(kpi.fallas_ordenes_plex, 0)) > 0 then
                                                       sum(kpi.recurrentes_ordenes_plex) / sum(kpi.fallas_ordenes_plex)
                                                   else 0
                                                   end, 3) * 100 ejecutado,
                                         region,
                                         puesto
                                  FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                                  WHERE kpi.ejecutado_mes = v_month
                                    AND kpi.employee_id = data.employee_id
                                  GROUP BY kpi.employee_id,
                                           kpi.employee_full_name,
                                           kpi.region,
                                           kpi.puesto
                              ) a) a;

            ELSEIF data.kpi_name = 'CHURN POR PLEX' THEN

                v_log = 'IF CHURN Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name, employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado, cumplimiento, resultado, resultado_2, region, puesto)
                select a.ejecutado_mes,
                       employee_id,
                       employee_full_name,
                       employee_kpi_name,
                       kpi_description,
                       start_date,
                       weight,
                       target,
                       unit_type,
                       target_amount,
                       ejecutado,
                       cumplimiento,
                       CASE
                           WHEN (weight * cumplimiento) > weight THEN weight
                           ELSE (weight * cumplimiento) END              resultado,
                       CASE
                           WHEN cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * cumplimiento) > weight THEN weight
                                    ELSE (weight * cumplimiento) END END resultado_2,
                       region,
                       puesto
                from (
                         SELECT a.ejecutado_mes,
                                a.employee_id,
                                a.employee_full_name,
                                a.employee_kpi_name,
                                a.kpi_description,
                                a.start_date,
                                a.weight,
                                a.target,
                                a.unit_type,
                                a.target_amount,
                                a.ejecutado,
                                case
                                    when a.ejecutado <= a.target_amount then 1
                                    else 0 end cumplimiento,
                                region,
                                puesto
                         FROM (
                                  SELECT v_month                 ejecutado_mes,
                                         kpi.employee_id,
                                         kpi.employee_full_name,
                                         data.kpi_name           employee_kpi_name,
                                         data.kpi_description    kpi_description,
                                         data.start_date         start_date,
                                         data.weight,
                                         data.target             target,
                                         data.unit_type,
                                         data.target             target_amount,
                                         round(case
                                                   when sum(coalesce(kpi.cortes, 0)) > 0 then
                                                       sum(kpi.cortes_plex) / sum(kpi.cortes)
                                                   else 0
                                                   end, 3) * 100 ejecutado,
                                         region,
                                         puesto
                                  FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                                  WHERE kpi.ejecutado_mes = v_month
                                    AND kpi.employee_id = data.employee_id
                                  GROUP BY kpi.employee_id,
                                           kpi.employee_full_name,
                                           kpi.region,
                                           kpi.puesto
                              ) a) a;
            ELSEIF data.kpi_name = 'AFECTACIÓN ALERTAS' THEN

                v_log = 'IF AFECTACIÓN Insertando registros para el empleado % y el kpi %';
                RAISE NOTICE '%',v_log;
                INSERT INTO tbl_kpi_plex_cumplimiento_empleado_v2
                (kpi_mes, employee_id, employee_full_name, employee_kpi_name, kpi_description, start_date, weight,
                 target, unit_type, target_amount, ejecutado, cumplimiento, resultado, resultado_2, region, puesto)
                select a.ejecutado_mes,
                       employee_id,
                       employee_full_name,
                       employee_kpi_name,
                       kpi_description,
                       start_date,
                       weight,
                       target,
                       unit_type,
                       target_amount,
                       ejecutado,
                       cumplimiento,
                       CASE
                           WHEN (weight * cumplimiento) > weight THEN weight
                           ELSE (weight * cumplimiento) END              resultado,
                       CASE
                           WHEN cumplimiento < 1 THEN 0
                           ELSE CASE
                                    WHEN (weight * cumplimiento) > weight THEN weight
                                    ELSE (weight * cumplimiento) END END resultado_2,
                       region,
                       puesto
                from (
                         SELECT a.ejecutado_mes,
                                a.employee_id,
                                a.employee_full_name,
                                a.employee_kpi_name,
                                a.kpi_description,
                                a.start_date,
                                a.weight,
                                a.target,
                                a.unit_type,
                                a.target_amount,
                                a.ejecutado,
                                case
                                    when a.ejecutado <= a.target_amount then 1
                                    else 0 end cumplimiento,
                                region,
                                puesto
                         FROM (
                                  SELECT v_month                 ejecutado_mes,
                                         kpi.employee_id,
                                         kpi.employee_full_name,
                                         data.kpi_name           employee_kpi_name,
                                         data.kpi_description    kpi_description,
                                         data.start_date         start_date,
                                         data.weight,
                                         data.target             target,
                                         data.unit_type,
                                         data.target             target_amount,
                                         round(case
                                                   when sum(coalesce(kpi.primarios_hfc, 0)) > 0 then
                                                           sum(kpi.primarios_afectados) /
                                                           sum(kpi.primarios_hfc)
                                                   else 0
                                                   end, 3) * 100 ejecutado,
                                         region,
                                         puesto
                                  FROM tbl_kpi_plex_ejecutado_empleado_v2 kpi
                                  WHERE kpi.ejecutado_mes = v_month
                                    AND kpi.employee_id = data.employee_id
                                  GROUP BY kpi.employee_id,
                                           kpi.employee_full_name,
                                           kpi.region,
                                           kpi.puesto
                              ) a) a;
            END IF;
        END LOOP;


    update tbl_kpi_plex_cumplimiento_empleado_v2
    set id_kpi = b.uuid
    from bicc.app_kpi b
    where employee_kpi_name = b.name
    and kpi_mes = v_month;
END;
$$;

alter procedure kpi_metricas_empleados_mensuales_plex_v2(date) owner to "fernando.lopez";

