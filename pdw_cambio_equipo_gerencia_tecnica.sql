create procedure pdw_cambio_equipo_gerencia_tecnica(p_fecha date DEFAULT (CURRENT_DATE - '1 day'::interval))
    language plpgsql
as
$$
DECLARE
    v_dia_bandera        int;
    v_fecha_mes_anterior date;
BEGIN

    --================================================================================================
    --analisis month over month
    if (p_fecha != public.end_of_month(p_fecha))
    then
        select extract(day from p_fecha) into v_dia_bandera;
    else
        v_dia_bandera := 31;
    end if;

    v_fecha_mes_anterior := date(p_fecha - '1month'::interval);

    raise notice '%,%', v_dia_bandera, p_fecha;
    --================================================================================================
    --analisis para cambios de equipo
    drop table if exists tbl_temp_cambio_equipo_mes_actual_w1;
    create temp table tbl_temp_cambio_equipo_mes_actual_w1 as (
        SELECT case
                   when o.estado = 'CERRADA' and abs(o.minutosreprogramacionvisita) between 0 and 1440
                       then '1. Menos de 1 Día'
                   when o.estado = 'CERRADA' and abs(o.minutosreprogramacionvisita) between 1441 and 4320
                       then '2. De 1 a 3 Días'
                   when o.estado = 'CERRADA' and abs(o.minutosreprogramacionvisita) between 4321 and 8640
                       then '3. De 3 a 6 Días'
                   when o.estado = 'CERRADA' and abs(o.minutosreprogramacionvisita) >= 8641
                       then '4. Más de 6 Días'
                   when o.estado = 'PENDIENTE'
                       then '6. Pendiente'
                   when o.estado = 'ANULADA'
                       then '0. Anulada'
                   else '5. Sin Entrada' end rango_dias_a_visita,
               o.actividad,
               s.ciudad,
               r.region,
               s.pais,
               case
                   when s.tipocliente in ('Residencial', 'Vip', 'PEQUE') then 'Residencial'
                   when s.tipocliente in ('PYME', 'Corporativo') then 'Corporativo'
                   else 'No Definido' end    udn,
               o.estado,
               o.medio,
               o.motivo,
               case
                   when o.actividad in ('CCAJA', 'SOP', 'CMOFFLINE', 'SOPFIBRA', 'SOPCORP', 'CMOFFLINECORP', 'SOPFIBRACORP') then true
                   when (o.actividad = 'CMAC' AND o.motivo IS NOT NULL) then true
                   else false end            es_cambio_equipo
        FROM public.ordenes o
                 left join public.servicios s
                           on s.cliente = o.cliente and s.contratoservicio = o.contratoservicio
                 LEFT JOIN public.vregiones r ON s.ciudad = r.ciudad AND s.pais = r.pais
        WHERE extract(day from o.fechacreacion::DATE) <= v_dia_bandera
          AND to_char(o.fechacreacion, 'yyyymm') = to_char(p_fecha, 'yyyymm'));

    drop table if exists tbl_temp_cambio_equipo_mes_actual_w2;
    create temp table tbl_temp_cambio_equipo_mes_actual_w2 as (
        SELECT a.rango_dias_a_visita,
               a.actividad,
               a.ciudad,
               a.region,
               a.pais,
               a.udn,
               a.estado,
               a.medio,
               a.motivo,
               count(*) contador
        FROM tbl_temp_cambio_equipo_mes_actual_w1 a
        where es_cambio_equipo = true
        group by rango_dias_a_visita, a.actividad, a.ciudad, a.region, a.pais, a.udn, a.estado, a.medio, a.motivo);

    drop table if exists tbl_temp_cambio_equipo_mes_anterior_w1;
    create temp table if not exists tbl_temp_cambio_equipo_mes_anterior_w1 as (
        SELECT CASE
                   WHEN CASE
                            WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o.estado END = 'CERRADA' and abs(o.minutosreprogramacionvisita) between 0 and 1440
                       then '1. Menos de 1 Día'

                   WHEN CASE
                            WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o.estado END = 'CERRADA' and abs(o.minutosreprogramacionvisita) between 1441 and 4320
                       then '2. De 1 a 3 Días'
                   WHEN CASE
                            WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o.estado END = 'CERRADA' and abs(o.minutosreprogramacionvisita) between 4321 and 8640
                       then '3. De 3 a 6 Días'
                   WHEN CASE
                            WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o.estado END = 'CERRADA' and abs(o.minutosreprogramacionvisita) >= 8641
                       then '4. Más de 6 Días'
                   WHEN CASE
                            WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o.estado END = 'ANULADA' then '0. Anulada'
                   WHEN CASE
                            WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o.estado END = 'PENDIENTE' then '6. Pendiente'
                   else '5. Sin Entrada'
                   end AS                 rango_dias_visita_mes_anterior,
               o.actividad,
               s.ciudad,
               r.region,
               s.pais,
               case
                   when s.tipocliente in ('Residencial', 'Vip', 'PEQUE') then 'Residencial'
                   when s.tipocliente in ('PYME', 'Corporativo') then 'Corporativo'
                   else 'No Definido' end udn,
               o.motivo,
               CASE
                   WHEN extract(day from o.fechaliquidacion::DATE) > v_dia_bandera THEN 'PENDIENTE'
                   ELSE o.estado END      estado,
               o.medio,
               case
                   when o.actividad in ('CCAJA', 'SOP', 'CMOFFLINE', 'SOPFIBRA', 'SOPCORP', 'CMOFFLINECORP', 'SOPFIBRACORP') then true
                   when (o.actividad = 'CMAC' AND o.motivo IS NOT NULL) then true
                   else false end         es_cambio_equipo
        FROM public.ordenes o
                 left join public.servicios s
                           on s.cliente = o.cliente and s.contratoservicio = o.contratoservicio
                 LEFT JOIN public.vregiones r ON s.ciudad = r.ciudad AND s.pais = r.pais
        WHERE extract(day from o.fechacreacion::DATE) <= v_dia_bandera
          AND to_char(o.fechacreacion, 'yyyymm') = to_char(v_fecha_mes_anterior, 'yyyymm'));

    drop table if exists tbl_temp_cambio_equipo_mes_anterior_w2;
    create temp table if not exists tbl_temp_cambio_equipo_mes_anterior_w2 as (
        select a.rango_dias_visita_mes_anterior,
               a.actividad,
               a.ciudad,
               a.region,
               a.pais,
               a.udn,
               a.motivo,
               a.estado,
               a.medio,
               count(*) contador
        from tbl_temp_cambio_equipo_mes_anterior_w1 a
        where a.es_cambio_equipo = true
        group by a.rango_dias_visita_mes_anterior, a.actividad, a.ciudad, a.region, a.pais, a.udn, a.motivo, a.estado,
                 a.medio);

    TRUNCATE TABLE tbl_cambio_equipo_gerencia_tecnica;
    INSERT INTO tbl_cambio_equipo_gerencia_tecnica
    (rango_dias_visita, actividad, ciudad, region, pais, udn, motivo, estado, mes_actual, mes_anterior,
     fecha_mes_actual, fecha_mes_anterior, medio)
    SELECT coalesce(a.rango_dias_a_visita, b.rango_dias_visita_mes_anterior) rango_dias_visita,
           coalesce(a.actividad, b.actividad)                                actividad,
           coalesce(a.ciudad, b.ciudad)                                      ciudad,
           coalesce(a.region, b.region)                                      region,
           coalesce(a.pais, b.pais)                                          pais,
           coalesce(a.udn, b.udn)                                            udn,
           coalesce(a.motivo, b.motivo)                                      motivo,
           coalesce(a.estado, b.estado)                                      estado,
           coalesce(sum(a.contador), 0)                                      mes_actual,
           coalesce(sum(b.contador), 0)                                      mes_anterior,
           p_fecha,
           v_fecha_mes_anterior,
           coalesce(a.medio, b.medio)                                        medio
    FROM tbl_temp_cambio_equipo_mes_actual_w2 a
             FULL JOIN tbl_temp_cambio_equipo_mes_anterior_w2 b
                       ON a.rango_dias_a_visita = b.rango_dias_visita_mes_anterior and
                          a.actividad = b.actividad AND
                          a.pais = b.pais AND
                          a.region = b.region AND a.ciudad = b.ciudad AND
                          a.udn = b.udn AND a.motivo = b.motivo AND a.estado = b.estado and
                          a.medio = b.medio
    GROUP BY coalesce(a.rango_dias_a_visita, b.rango_dias_visita_mes_anterior),
             coalesce(a.actividad, b.actividad),
             coalesce(a.ciudad, b.ciudad), coalesce(a.region, b.region), coalesce(a.pais, b.pais),
             coalesce(a.udn, b.udn), coalesce(a.motivo, b.motivo), coalesce(a.estado, b.estado),
             p_fecha, v_fecha_mes_anterior, coalesce(a.medio, b.medio)
    ORDER BY 5, 4, 3, 2;

    update tbl_cambio_equipo_gerencia_tecnica
    set zona = (case
                    when region in ('NORTE', 'LITORAL', 'OCCIDENTE') then 'NOR-OCCIDENTE'
                    when region in ('CENTRO', 'SUR', 'ORIENTE', 'NORORIENTE') then 'CENTRO-SUR'
                    else region end)
    where zona is null;

    --fin del analisis
    --==================================================================================================================

END;
$$;

alter procedure pdw_cambio_equipo_gerencia_tecnica(date) owner to jcastro;

