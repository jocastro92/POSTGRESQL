create procedure pdw_ordenes_medicion_gerencia_tecnicav3(p_fecha date DEFAULT (CURRENT_DATE - '1 day'::interval))
    language plpgsql
as
$$
DECLARE
    v_dia_bandera        int;
    v_fecha_mes_anterior date;
BEGIN
    --hola mundo soy un github
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
    --analisis para ordenes medicion gerencia tecnica
    drop table if exists tbl_temp_medicion_gerencia_mes_actual_w1;
    create temp table tbl_temp_medicion_gerencia_mes_actual_w1 as (
        SELECT o.rango_dias_a_visita rango_dias_visita,
               o."Actividad"         actividad,
               o."Ciudad"            ciudad,
               o."Region"            region,
               o.pais,
               o.unidad_de_negocios  udn,
               o."Estado"            estado,
               o.medio,
               case
                   when o."Actividad" in ('CEQUICORP',
                                          'CMOFFLINECORP',
                                          'INSFIBRACORP',
                                          'INSHFCCORP',
                                          'SOPCORP',
                                          'SOPFIBRACORP',
                                          'SOPRECONCORP',
                                          'STBCORP',
                                          'TRASLADOEXTFIBRACORP',
                                          'TRASLADOEXTHFCCORP',
                                          'TRASLADOINTFIBRACORP',
                                          'TRASLADOINTHFCCORP',
                                          'TVADICIONALCORP',
                                          'PEXTERNO') then true
                   else false end    es_medicion
        FROM public.tbl_diario_lista_ordenes_v2 o
        WHERE extract(day from o."Fecha de Creacion"::DATE) <= v_dia_bandera
          AND to_char(o."Fecha de Creacion", 'yyyymm') = to_char(p_fecha, 'yyyymm'));

    drop table if exists tbl_temp_medicion_gerencia_mes_actual_w2;
    create temp table tbl_temp_medicion_gerencia_mes_actual_w2 as (
        SELECT a.rango_dias_visita,
               a.actividad,
               a.ciudad,
               a.region,
               a.pais,
               a.udn,
               a.estado,
               a.medio,
               count(*) contador
        FROM tbl_temp_medicion_gerencia_mes_actual_w1 a
        where a.es_medicion = true
        group by a.rango_dias_visita, a.actividad, a.ciudad, a.region, a.pais, a.udn, a.estado, a.medio);

    drop table if exists tbl_temp_medicion_gerencia_mes_anterior_w1;
    create temp table if not exists tbl_temp_medicion_gerencia_mes_anterior_w1 as (
        SELECT CASE
                   WHEN CASE
                            WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o."Estado" END = 'CERRADA' AND o.rango_dias_a_visita = '1. Menos de 1 Día'
                       THEN o.rango_dias_a_visita
                   WHEN CASE
                            WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o."Estado" END = 'CERRADA' AND o.rango_dias_a_visita = '2. De 1 a 3 Días'
                       THEN o.rango_dias_a_visita
                   WHEN CASE
                            WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o."Estado" END = 'CERRADA' AND o.rango_dias_a_visita = '3. De 3 a 6 Días'
                       THEN o.rango_dias_a_visita
                   WHEN CASE
                            WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o."Estado" END = 'CERRADA' AND o.rango_dias_a_visita = '4. Más de 6 Días'
                       THEN o.rango_dias_a_visita
                   WHEN CASE
                            WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o."Estado" END = 'ANULADA' then '0. Anulada'
                   WHEN CASE
                            WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                            ELSE o."Estado" END = 'PENDIENTE' then 'PENDIENTE'
                   else '5. Sin Entrada'
                   end AS                 rango_dias_visita_mes_anterior,
               o."Actividad"              actividad,
               o."Ciudad"                 ciudad,
               o."Region"                 region,
               o.pais,
               case
                   when o."Tipo de Cliente" in ('Residencial', 'PEQUE', 'Vip') then 'Residencial'
                   when o."Tipo de Cliente" in ('Corporativo', 'PYME') then 'Corporativo'
                   else 'No Definido' end udn,
               CASE
                   WHEN extract(day from o."Fecha Liquidacion"::DATE) > v_dia_bandera THEN 'PENDIENTE'
                   ELSE o."Estado" END    estado,
               o.medio,
               case
                   when o."Actividad" in ('CEQUI',
                                          'CMOFFLINE',
                                          'CMOFFLINECORP',
                                          'INSFIBRA',
                                          'INSFIBRACORP',
                                          'INSHFC',
                                          'INSHFCCORP',
                                          'SOP',
                                          'SOPCORP',
                                          'SOPFIBRA',
                                          'SOPFIBRACORP',
                                          'SOPRECONCORP',
                                          'SOPRECONHFC',
                                          'STB',
                                          'TRASLADOEXTFIBRA',
                                          'TRASLADOEXTHFC',
                                          'TRASLADOINTERNOFIBRA',
                                          'TRASLADOINTERNOHFC',
                                          'TRASLADOINTFIBRACORP',
                                          'TRASLADOINTHFCCORP',
                                          'TVADICIONAL',
                                          'TVADICIONALCORP',
                                          'PEXTERNO') then true
                   else false end         es_medicion
        FROM public.tbl_diario_lista_ordenes_v2 o
        WHERE extract(day from o."Fecha de Creacion"::DATE) <= v_dia_bandera
          AND to_char(o."Fecha de Creacion", 'yyyymm') = to_char(v_fecha_mes_anterior, 'yyyymm'));

    drop table if exists tbl_temp_medicion_gerencia_mes_anterior_w2;
    create temp table if not exists tbl_temp_medicion_gerencia_mes_anterior_w2 as (
        select a.rango_dias_visita_mes_anterior,
               a.actividad,
               a.ciudad,
               a.region,
               a.pais,
               a.udn,
               a.estado,
               a.medio,
               count(*) contador
        from tbl_temp_medicion_gerencia_mes_anterior_w1 a
        where a.es_medicion = true
        group by a.rango_dias_visita_mes_anterior, a.actividad, a.ciudad, a.region, a.pais, a.udn, a.estado, a.medio);

    TRUNCATE TABLE tbl_ordenes_medicion_gerencia_tecnica_v3;
    INSERT INTO tbl_ordenes_medicion_gerencia_tecnica_v3
    (rango_dias_visita, actividad, ciudad, region, pais, udn, estado, mes_actual, mes_anterior,
     fecha_mes_actual, fecha_mes_anterior, medio)
    SELECT coalesce(a.rango_dias_visita, b.rango_dias_visita_mes_anterior) rango_dias_visita,
           coalesce(a.actividad, b.actividad)                              actividad,
           coalesce(a.ciudad, b.ciudad)                                    ciudad,
           coalesce(a.region, b.region)                                    region,
           coalesce(a.pais, b.pais)                                        pais,
           coalesce(a.udn, b.udn)                                          udn,
           coalesce(a.estado, b.estado)                                    estado,
           coalesce(sum(a.contador), 0)                                    mes_actual,
           coalesce(sum(b.contador), 0)                                    mes_anterior,
           p_fecha,
           v_fecha_mes_anterior,
           coalesce(a.medio, b.medio)                                      medio
    FROM tbl_temp_medicion_gerencia_mes_actual_w2 a
             FULL JOIN tbl_temp_medicion_gerencia_mes_anterior_w2 b
                       ON a.rango_dias_visita = b.rango_dias_visita_mes_anterior AND a.actividad = b.actividad AND
                          a.pais = b.pais AND
                          a.region = b.region AND a.ciudad = b.ciudad AND
                          a.udn = b.udn AND a.estado = b.estado and
                          a.medio = b.medio
    GROUP BY coalesce(a.rango_dias_visita, b.rango_dias_visita_mes_anterior), coalesce(a.actividad, b.actividad),
             coalesce(a.ciudad, b.ciudad), coalesce(a.region, b.region), coalesce(a.pais, b.pais),
             coalesce(a.udn, b.udn), coalesce(a.estado, b.estado),
             p_fecha, v_fecha_mes_anterior, coalesce(a.medio, b.medio)
    ORDER BY 5, 4, 3, 2;

    update tbl_ordenes_medicion_gerencia_tecnica_v3
    set zona = (case
                    when region in ('NORTE', 'LITORAL', 'OCCIDENTE') then 'NOR-OCCIDENTE'
                    when region in ('CENTRO', 'SUR', 'ORIENTE', 'NORORIENTE') then 'CENTRO-SUR'
                    else region end)
    where zona is null;

    call pdw_ordenes_imr_gerencia_tecnica(p_fecha);
    call pdw_cambio_equipo_gerencia_tecnica(p_fecha);
    call pdw_fallas_medicion_gerencia_tecnicav3(p_fecha);

    --github finaL
END;
$$;



