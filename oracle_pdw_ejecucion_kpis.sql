create PROCEDURE PDW_EJECUCION_KPIS(P_FECHA DATE default sysdate - 1)
AS

    V_ANOMES VARCHAR(6) := TO_CHAR(P_FECHA, 'YYYYMM');


BEGIN

    -- PROCESO ETL


    MERGE INTO TBL_INDICADOR A
    USING TBL_APP_KPI B
    ON (A.ID_OTRA_PLATAFORMA = B.UUID)
    WHEN MATCHED THEN
        UPDATE
        SET A.NOMBRE_KPI = FN_CARACTERES_ESPECIALES(B.NAME),
            A.ESTADO     = (CASE WHEN B.IS_AVAILABLE = 'T' THEN 1 ELSE 0 END)
    WHEN NOT MATCHED THEN
        INSERT
        (A.NOMBRE_KPI, A.ID_OTRA_PLATAFORMA, A.ESTADO, A.UNIDAD_MEDIDA, A.FECHA_CREACION, A.USUARIO_MODIFICA)
        VALUES (B.NAME,
                B.UUID,
                (CASE WHEN B.IS_AVAILABLE = 'T' THEN 1 ELSE 0 END),
                (CASE
                     WHEN NAME IN ('CHURN', 'SERVICIOS PRIMARIOS') THEN 21
                     WHEN B.UNIT_TYPE = '%' THEN 1
                     WHEN B.UNIT_TYPE = 'VOF' THEN 22
                     WHEN B.UNIT_TYPE = 'LPS' THEN 23
                     WHEN B.UNIT_TYPE = 'UND' THEN 24
                     ELSE 25 END),
                SYSDATE,
                'PROCESO');

    COMMIT;


    MERGE INTO TBL_ENTIDAD A
    USING (
        SELECT DISTINCT UUID, FULL_NAME
        FROM TBL_METAS_APP_KPI
        WHERE JOB_ID IN ('b0b64567-816b-465b-ad20-53a996a4966b')
    ) B
    ON (A.ID_EXTERNO = B.UUID)
    WHEN MATCHED THEN
        UPDATE
        SET A.NOMBRE = FN_CARACTERES_ESPECIALES(B.FULL_NAME)
    WHEN NOT MATCHED THEN
        INSERT
        (A.NOMBRE, A.ID_EXTERNO, A.TIPO_ENTIDAD_ID, A.USUARIO_ACTUALIZA, A.FECHA_CREA)
        VALUES (FN_CARACTERES_ESPECIALES(B.FULL_NAME), B.UUID, 21, 'PROCESO', SYSDATE);

    COMMIT;


    -- ACTUALIZACION DE AREAS

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_ETL_AREAS_W1';

    INSERT INTO TBL_ETL_AREAS_W1
        (FULL_NAME, REGION, R1)
    SELECT ID_EMPLOYEE,
           REGION,
           R1
    FROM (
             SELECT A.ID_EMPLOYEE,
                    A.REGION,
                    ROW_NUMBER() OVER (PARTITION BY REGION ORDER BY ID_EMPLOYEE ) R1
             FROM (
                      SELECT DISTINCT ID_EMPLOYEE,
                                      REGION
                      FROM TBL_REGIONES_EMPLEADOS tre
                      WHERE ID_EMPLOYEE IN (SELECT DISTINCT UUID
                                            FROM TBL_METAS_APP_KPI
                                            WHERE JOB_ID = 'c1ef3a42-3310-4948-96b8-5ea20bbfa829')
                  ) A)
    WHERE R1 = 1;


    COMMIT;


    MERGE INTO TBL_ENTIDAD A
    USING TBL_ETL_AREAS_W1 B
    ON (A.NOMBRE = B.REGION
        AND B.R1 = 1
        AND A.ENTIDAD_PADRE = 23
        AND A.TIPO_ENTIDAD_ID = 22
        )
    WHEN MATCHED THEN
        UPDATE
        SET A.ID_EXTERNO = B.FULL_NAME;

    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TMP_EMPLEDOS_OPERACIONES';

    INSERT INTO TBL_TMP_EMPLEDOS_OPERACIONES
        (UUID, FULL_NAME)
    SELECT DISTINCT UUID, FN_CARACTERES_ESPECIALES(FULL_NAME)
    FROM TBL_METAS_APP_KPI
    WHERE JOB_ID IN (
                     '7f77ac1d-4455-4c33-83e1-8f4684fb793f',
                     '222e3a78-3804-4956-af54-a4397d580407',
                     '15992c25-a379-4e22-8e5d-d624524de23f');

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TBL_TMP_EMP_OP_W1';

    INSERT INTO TBL_TMP_EMP_OP_W1
        (UUID, FULL_NAME, REGION)
    SELECT DISTINCT A.UUID, A.FULL_NAME, B.REGION
    FROM TBL_TMP_EMPLEDOS_OPERACIONES A,
         TBL_REGIONES_EMPLEADOS B
    WHERE A.UUID = B.ID_EMPLOYEE;

    COMMIT;


    MERGE INTO TBL_ENTIDAD A
    USING (
        SELECT A.UUID, A.FULL_NAME, A.REGION, B.ID_ENTIDAD
        FROM TBL_TMP_EMP_OP_W1 A,
             TBL_ENTIDAD B
        WHERE A.REGION = B.NOMBRE
          AND B.ENTIDAD_PADRE = 23
    ) B
    ON (A.ID_EXTERNO = B.UUID)
    WHEN MATCHED THEN
        UPDATE
        SET A.NOMBRE = B.FULL_NAME
    WHEN NOT MATCHED THEN
        INSERT
        (A.NOMBRE, A.ID_EXTERNO, A.ENTIDAD_PADRE, A.TIPO_ENTIDAD_ID, A.USUARIO_ACTUALIZA, A.FECHA_CREA)
        VALUES (B.FULL_NAME, B.UUID, B.ID_ENTIDAD, 23, 'PROCESO', SYSDATE);

    COMMIT;

    --eliminar empleados inactivos
    delete
    from TBL_ENTIDAD a
    where not exists(select distinct ID_EMPLOYEE, FULL_NAME
                     from TBL_REGIONES_EMPLEADOS b
                     where a.ID_EXTERNO = b.ID_EMPLOYEE
        )
      and TIPO_ENTIDAD_ID = 23;

    -- FIN DE PROCESO ETL


    DELETE TBL_EJECUCION_KPIS
    WHERE ANOMES = V_ANOMES;

    COMMIT;

    INSERT INTO TBL_EJECUCION_KPIS
        (ANOMES, KPI_ID, ENTIDAD_ID, PESO, USUARIO_ACTUALIZA, FECHA_CREACION)
    SELECT V_ANOMES ANOMES,
           C.ID_KPI,
           B.ID_ENTIDAD,
           A.WEIGHT,
           'PROCESO',
           SYSDATE
    FROM TBL_METAS_APP_KPI A,
         TBL_ENTIDAD B,
         TBL_INDICADOR C
    WHERE A.UUID = B.ID_EXTERNO
      AND A.KPI_ID = C.ID_OTRA_PLATAFORMA;

    COMMIT;

    -- QUERY PARA ACTUALIZACION DE EJECUCION DE LOS DEPARTAMENTOS
    MERGE INTO TBL_EJECUCION_KPIS A
    USING (SELECT B.ID_ENTIDAD,
                  A.EMPLOYEE_ID,
                  A.EMPLOYEE_FULL_NAME,
                  A.EMPLOYEE_KPI_NAME,
                  A.ID_KPI ID_KPI_EXTERNO,
                  C.ID_KPI,
                  A.EJECUTADO_REPORTE,
                  A.CUMPLIMIENTO,
                  A.RESULTADO_2,
                  A.TARGET META,
                  A.RESULTADO
           FROM TBL_CUMPLIMIENTO_EMPLEADOV3 A,
                TBL_ENTIDAD B,
                TBL_INDICADOR C
           WHERE A.ID_KPI = C.ID_OTRA_PLATAFORMA
             AND A.EMPLOYEE_ID = B.ID_EXTERNO
             AND B.TIPO_ENTIDAD_ID = 21
             AND A.KPI_MES = V_ANOMES
    ) B
    ON (A.ANOMES = V_ANOMES
        AND A.KPI_ID = B.ID_KPI
        AND A.ENTIDAD_ID = B.ID_ENTIDAD)
    WHEN MATCHED THEN
        UPDATE
        SET A.EJECUTADO    = round(B.EJECUTADO_REPORTE, 2),
            A.CUMPLIMIENTO = B.CUMPLIMIENTO,
            A.EVALUACION   = B.RESULTADO_2,
            A.META         = B.META,
            A.EVALUACION_2 = B.RESULTADO;

    COMMIT;


    -- QUERY PARA ACTUALIZACION DE EJECUCION DE LAS AREAS DE OPERACIONES
    MERGE INTO TBL_EJECUCION_KPIS A
    USING (SELECT B.ID_ENTIDAD,
                  A.EMPLOYEE_ID,
                  A.EMPLOYEE_FULL_NAME,
                  A.EMPLOYEE_KPI_NAME,
                  A.ID_KPI ID_KPI_EXTERNO,
                  C.ID_KPI,
                  A.EJECUTADO_REPORTE,
                  A.CUMPLIMIENTO,
                  A.RESULTADO_2,
                  A.TARGET META,
                  A.RESULTADO
           FROM TBL_CUMPLIMIENTO_EMPLEADOV3 A,
                TBL_ENTIDAD B,
                TBL_INDICADOR C
           WHERE A.ID_KPI = C.ID_OTRA_PLATAFORMA
             AND A.EMPLOYEE_ID = B.ID_EXTERNO
             AND B.ENTIDAD_PADRE = 23
             AND B.TIPO_ENTIDAD_ID = 22
             AND A.KPI_MES = V_ANOMES
    ) B
    ON (A.ANOMES = V_ANOMES
        AND A.KPI_ID = B.ID_KPI
        AND A.ENTIDAD_ID = B.ID_ENTIDAD)
    WHEN MATCHED THEN
        UPDATE
        SET A.EJECUTADO    = round(B.EJECUTADO_REPORTE, 2),
            A.CUMPLIMIENTO = B.CUMPLIMIENTO,
            A.EVALUACION   = B.RESULTADO_2,
            A.EVALUACION_2 = B.RESULTADO,
            A.META         = B.META
    WHEN NOT MATCHED THEN
        INSERT
        (A.ANOMES,
         A.ENTIDAD_ID,
         A.KPI_ID,
         A.META,
         A.CUMPLIMIENTO,
         A.EJECUTADO,
         A.EVALUACION,
         A.USUARIO_ACTUALIZA,
         A.FECHA_CREACION,
         A.EVALUACION_2)
        VALUES (V_ANOMES,
                B.ID_ENTIDAD,
                B.ID_KPI,
                B.META,
                B.CUMPLIMIENTO,
                round(B.EJECUTADO_REPORTE, 2),
                B.RESULTADO_2,
                'PROCESO',
                SYSDATE,
                B.RESULTADO);

    COMMIT;

    -- QUERY PARA ACTUALIZACION DE EJECUCION DE LOS EMPLEADOS DE OPERACIONES
    MERGE INTO TBL_EJECUCION_KPIS A
    USING (SELECT B.ID_ENTIDAD,
                  A.EMPLOYEE_ID,
                  A.EMPLOYEE_FULL_NAME,
                  A.EMPLOYEE_KPI_NAME,
                  A.ID_KPI ID_KPI_EXTERNO,
                  C.ID_KPI,
                  A.EJECUTADO_REPORTE,
                  A.CUMPLIMIENTO,
                  A.RESULTADO_2,
                  A.TARGET META,
                  A.RESULTADO
           FROM TBL_CUMPLIMIENTO_EMPLEADOV3 A,
                TBL_ENTIDAD B,
                TBL_INDICADOR C
           WHERE A.ID_KPI = C.ID_OTRA_PLATAFORMA
             AND A.EMPLOYEE_ID = B.ID_EXTERNO
             AND B.ENTIDAD_PADRE IN (41,
                                     42,
                                     43,
                                     44,
                                     45,
                                     46,
                                     47)
             AND B.TIPO_ENTIDAD_ID = 23
             AND A.KPI_MES = V_ANOMES
    ) B
    ON (A.ANOMES = V_ANOMES
        AND A.KPI_ID = B.ID_KPI
        AND A.ENTIDAD_ID = B.ID_ENTIDAD)
    WHEN MATCHED THEN
        UPDATE
        SET A.EJECUTADO    = round(B.EJECUTADO_REPORTE, 2),
            A.CUMPLIMIENTO = B.CUMPLIMIENTO,
            A.EVALUACION   = B.RESULTADO_2,
            A.EVALUACION_2 = B.RESULTADO,
            A.META         = B.META
    WHEN NOT MATCHED THEN
        INSERT
        (A.ANOMES,
         A.ENTIDAD_ID,
         A.KPI_ID,
         A.META,
         A.CUMPLIMIENTO,
         A.EJECUTADO,
         A.EVALUACION,
         A.USUARIO_ACTUALIZA,
         A.FECHA_CREACION,
         A.EVALUACION_2)
        VALUES (V_ANOMES,
                B.ID_ENTIDAD,
                B.ID_KPI,
                B.META,
                B.CUMPLIMIENTO,
                round(B.EJECUTADO_REPORTE, 2),
                B.RESULTADO_2,
                'PROCESO',
                SYSDATE,
                B.RESULTADO);

    COMMIT;


END;
/

