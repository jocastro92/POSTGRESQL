create or replace procedure pdw_sop_instalaciones_recientes_diario(p_fecha date DEFAULT (CURRENT_DATE - '1 day'::interval))
    language plpgsql
as
$$
declare

    v_fecha_dia_anterior date;
    v_fecha_semana       date;
begin

    --hola mundo soy un github

    v_fecha_dia_anterior := p_fecha;
    v_fecha_semana := p_fecha - '6 days'::interval;

    --=============================================================================================
    --analisis para recuperar las fallas del dia anterior
    drop table if exists tbl_temp_fallas;
    create temp table if not exists tbl_temp_fallas as (
        SELECT o.pais                pais_sop,
               o.ciudad              ciudad_sop,
               o.cliente             numero_cliente_sop,
               o.nombrecliente       nombre_cliente_sop,
               o.contratoservicio    contrato_servicio_sop,
               o.numeroorden         numero_orden_sop,
               o.actividad           actividad_sop,
               o.empresa             empresa_sop,
               o.empresaejecutora    empresa_ejecutora_sop,
               o.estado              estado_sop,
               date(o.fechacreacion) fecha_creacion_sop,
               o.motivo              motivo_sop,
               CASE
                   WHEN o.actividad IN ('SOPIN', 'SOPINFIBRA', 'SOPINFIBRAFTTH') AND o.motivo IS NULL THEN FALSE
                   ELSE TRUE
                   END               motivo_valido,
               CASE
                   WHEN o.actividad IN ('SOPIN', 'SOPINFIBRA', 'SOPINFIBRAFTTH') AND o.motivo IS NULL THEN null
                   ELSE o.motivo
                   END               motivo_mant,
               o.nomcolonia          colonia_sop
        FROM public.ordenes o
        WHERE date(o.fechacreacion) = v_fecha_dia_anterior
          AND o.actividad IN ('SOP', 'SOPFIBRA', 'CMOFFLINE', 'SOPIN', 'SOPINFIBRA', 'SOPINFIBRAFTTH',
                              'SOPFIBRACORP', 'SOPCORP', 'CMOFFLINECORP')
          AND o.estado <> 'ANULADA');

    --recuperando las instalaciones de la ultima semana
    drop table if exists tbl_temp_instalaciones;
    create temp table if not exists tbl_temp_instalaciones as (
        select a.pais,
               a.region,
               a.ciudad,
               a.empresa,
               a.contratoservicio,
               a.cliente                                   numero_cliente_ins,
               a.contratoservicio                          contrato_servicio_ins,
               b.actividad                                 actividad_ins,
               b.numeroorden                               numero_orden_ins,
               b.empresaejecutora                          empresa_ejecutora_ins,
               date(b.fechacreacion)                       fecha_creacion_ins,
               b.usuariotecnico                            usuario_tecnico,
               b.usuarioliquida                            usuario_liquida_ins,
               b.fechaliquidacion                          fecha_liquidacion_ins,
               b.nomcolonia                                colonia_ins,
               row_number()
               over (partition by a.cliente, a.contratoservicio
                   order by date(b.fechaliquidacion) desc) r1
        from public.tbl_movimientos_churn_v2 a
                 left join public.ordenes b on a.contratoservicio = b.contratoservicio
            and a.es_activacion = true
        where date(a.fecha_movimiento) >= current_date - '7day'::interval
          and b.actividad IN ('INSHFC', 'INSFIBRA', 'INSCARPA'
            'INSFIBRACORP', 'INSHFCCORP')
          AND b.estado = 'CERRADA');

    --actualizando informacion de las tablas temporales a tabla final mes actual
    truncate table tbl_sop_instalaciones_recientes;
    insert into tbl_sop_instalaciones_recientes
    (pais_sop, region, ciudad_sop, empresa_sop, contrato_servicio_sop, empresa_ejecutora_ins, numero_orden_ins,
     actividad_ins, fecha_liquidacion_ins, usuario_liquida_ins, empresa_ejecutora_sop, numero_orden_sop, actividad_sop,
     fecha_creacion_sop, estado_sop, motivo_sop, numero_cliente_sop, nombre_cliente_sop, cantidad,
     dias_primer_reporte_fallo,
     fecha_creacion_ins, colonia_ins, colonia_sop)
    select a.pais_sop,
           b.region,
           a.ciudad_sop,
           a.empresa_sop,
           a.contrato_servicio_sop,
           b.empresa_ejecutora_ins,
           b.numero_orden_ins,
           b.actividad_ins,
           b.fecha_liquidacion_ins,
           b.usuario_liquida_ins,
           a.empresa_ejecutora_sop,
           a.numero_orden_sop,
           a.actividad_sop,
           a.fecha_creacion_sop,
           a.estado_sop,
           a.motivo_mant,
           a.numero_cliente_sop,
           a.nombre_cliente_sop,
           1,
           extract(day from date(a.fecha_creacion_sop)) - extract(day from date(b.fecha_liquidacion_ins)),
           b.fecha_creacion_ins,
           b.colonia_ins,
           a.colonia_sop
    from tbl_temp_fallas a,
         tbl_temp_instalaciones b
    where a.numero_cliente_sop = b.numero_cliente_ins
      and a.contrato_servicio_sop = b.contrato_servicio_ins
      and a.motivo_valido = true
      and b.r1 = 1
      and extract(day from date(a.fecha_creacion_sop)) - extract(day from date(b.fecha_liquidacion_ins)) >= 0;

    --continuacion de mejora
    --actualizando campo conteo de instalaciones por cliente mes actual
    update tbl_sop_instalaciones_recientes
    set conteo_instalaciones = b.conteo
    from (
             select o.cliente,
                    o.contratoservicio,
                    count(o.numeroorden) conteo
             from public.ordenes o
             where o.actividad IN ('INSHFC', 'INSCARPA', 'INSFIBRA', 'INSTERCERIZADO',
                                   'INSFIBRACORP', 'INSHFCCORP')
               AND date(o.fechaliquidacion) >= v_fecha_semana
               and o.estado <> 'ANULADA'
             group by o.cliente, o.contratoservicio) b
    where tbl_sop_instalaciones_recientes.numero_cliente_sop = b.cliente
      and tbl_sop_instalaciones_recientes.contrato_servicio_sop = b.contratoservicio;

end
$$;

alter procedure pdw_sop_instalaciones_recientes_diario(date) owner to jcastro;

