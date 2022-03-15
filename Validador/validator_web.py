import arcpy
import os
import sys
import pandas as pd


from importlib.resources import path
from typing import List, Dict, Tuple
from zipfile import ZipFile
from utils.utils import conversion_format, set_workspace, valw_gdb_mensaje
from database.connection import schema

def extract_files(file_path: str, extract_path: str):
    """
    Extract all files from a zip file
    """
    arcpy.AddMessage("2. Extracting files...")
    with ZipFile(file_path, "r") as zip:
        file_name = os.path.basename(zip.filename).split(".")[0]
        file_format = os.path.basename(zip.filename).split(".")[1]
        file_date = file_name.split("_")[1]

        zip.extractall(path=extract_path)

        if file_format != "gdb":
            arcpy.AddWarning("File format is not gdb")    
            list_files = zip.namelist()
            for file in list_files:
                arcpy.AddMessage(F"2.1 File name: {file.split('.')[0]}")
                arcpy.AddMessage(F"2.2 File format: {file.split('.')[1]}")
                arcpy.AddMessage(F"2.3 File date: {file_date}")
            conversion_format(F"{extract_path}\{file}")
        else:
            arcpy.AddMessage(F"2.1 File name: {file_name}")
            arcpy.AddMessage(F"2.2 File format: {file_format}")
            arcpy.AddMessage(F"2.3 File date: {file_date}")
            set_workspace(F"{extract_path}\{file_name}.gdb")


def spatial_matching():
    """
    Spatial matching
    """
    arcpy.AddMessage("4. Spatial matching...")
    arcpy.AddMessage("Verificación coincidencia espacial")
    
    # TODO - Add request WFS
    geojson_servicioANM_1 = {
        "displayFieldName": "",
        "fieldAliases": {
            "OBJECTID": "OBJECTID",
            "SHAPE_Length": "SHAPE_Length",
            "SHAPE_Area": "SHAPE_Area"
        },
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {
            "wkid": 4686,
            "latestWkid": 4686
        },
        "fields": [
            {
                "name": "OBJECTID",
                "type": "esriFieldTypeOID",
                "alias": "OBJECTID"
            },
            {
                "name": "SHAPE_Length",
                "type": "esriFieldTypeDouble",
                "alias": "SHAPE_Length"
            },
            {
                "name": "SHAPE_Area",
                "type": "esriFieldTypeDouble",
                "alias": "SHAPE_Area"
            }
        ],
        "features": [
            {
                "geometry": {
                    "rings": [
                        [
                            [
                                -74.47880612799997,
                                5.42205679500006
                            ],
                            [
                                -74.49103233099999,
                                5.422048681000035
                            ],
                            [
                                -74.49105460199996,
                                5.455144681000036
                            ],
                            [
                                -74.47882773099997,
                                5.455152845000043
                            ],
                            [
                                -74.47880612799997,
                                5.42205679500006
                            ]
                        ]
                    ]
                },
                "attributes": {
                    "CODIGO_EXPEDIENTE": "IDO-08061",
                    "AREA_HA": 495.92897,
                    "FECHA_DE_INSCRIPCION": "8/06/2010",
                    "ESTADO": "Activo",
                    "MODALIDAD": "CONTRATO DE CONCESION (L 685)",
                    "ETAPA": "Explotación",
                    "MINERALES": "ANTRACITA, CARBÓN METALÚRGICO, CARBÓN TÉRMICO",
                    "NOMBRE_DE_TITULAR": "INVERSIONES NUEVA COLONIA S.A.S.",
                    "NUMERO_IDENTIFICACION": "900866306",
                    "TIPO_DE_IDENTIFICACION": "NIT",
                    "IDENTIFICACION_TITULARES": "57086",
                    "PTO_PTI": "PTO",
                    "INSTRUMENTO_AMBIENTAL": "Y",
                    "DEPARTAMENTOS": "Cundinamarca",
                    "MUNICIPIOS": "CAPARRAPÍ",
                    "GRUPO_DE_TRABAJO": "ZONA CENTRO",
                    "FECHA_TERMINACION": "7/06/2040",
                    "OBJECTID": 5178,
                    "SHAPE_Length": 9.06451439618818E-02,
                    "SHAPE_Area": 4.04649952760689E-04
                }
            }
        ]
    }
    geojson_servicioANM_2 = {
        "displayFieldName": "",
        "fieldAliases": {
            "OBJECTID": "OBJECTID",
            "SHAPE_Length": "SHAPE_Length",
            "SHAPE_Area": "SHAPE_Area"
        },
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {
            "wkid": 4686,
            "latestWkid": 4686
        },
        "fields": [
            {
                "name": "OBJECTID",
                "type": "esriFieldTypeOID",
                "alias": "OBJECTID"
            },
            {
                "name": "SHAPE_Length",
                "type": "esriFieldTypeDouble",
                "alias": "SHAPE_Length"
            },
            {
                "name": "SHAPE_Area",
                "type": "esriFieldTypeDouble",
                "alias": "SHAPE_Area"
            }
        ],
        "features": [
            {
                "geometry": {
                    "rings": [
                        [
                            [
                                -74.37196390799994,
                                5.441125020000072
                            ],
                            [
                                -74.37433568799997,
                                5.438257351000061
                            ],
                            [
                                -74.37716077099998,
                                5.439847478000047
                            ],
                            [
                                -74.39108764699995,
                                5.429558979000035
                            ],
                            [
                                -74.39307373099996,
                                5.431465956000068
                            ],
                            [
                                -74.37623310399994,
                                5.443410792000066
                            ],
                            [
                                -74.37196390799994,
                                5.441125020000072
                            ]
                        ]
                    ]
                },
                "attributes": {
                    "CODIGO_EXPEDIENTE": "GBH-141",
                    "AREA_HA": 76.67445,
                    "FECHA_DE_INSCRIPCION": "7/09/2006 3:38:11 a.m.",
                    "ESTADO": "Titulo terminado-en proceso de liquidacion",
                    "MODALIDAD": "CONTRATO DE CONCESION (L 685)",
                    "ETAPA": "Explotación",
                    "MINERALES": "ANHIDRITA, ANTRACITA, ARCILLA COMUN, ARCILLAS, ARCILLAS ESPECIALES, ARCILLAS REFRACTARIAS, ARENAS, ARENAS ARCILLOSAS, ARENAS FELDESPÁTICAS, ARENAS INDUSTRIALES, ARENAS Y GRAVAS SILICEAS, ARENISCAS, ASFALTO NATURAL, AZUFRE, BAUXITA, BENTONITA, CALCITA, CAOLIN, CARBÓN, CARBÓN METALÚRGICO, CARBÓN TÉRMICO, CONCENTRADOS MINERALES DE IRIDIO, CORINDON, CUARZO, DOLOMITA, ESMERALDA, FELDESPATOS, FLUORITA, GRAFITO, GRANATE, GRANITO, GRAVAS, MAGNESITA, MARMOL Y TRAVERTINO, MICA, MINERALES DE ALUMINIO Y SUS CONCENTRADOS, MINERALES DE ANTIMONIO Y SUS CONCENTRADOS, MINERALES DE BARIO, MINERALES DE BORO, MINERALES DE CIRCONIO Y SUS CONCENTRADOS, MINERALES DE COBALTO Y SUS CONCENTRADOS, MINERALES DE COBRE Y SUS CONCENTRADOS, MINERALES DE CROMO Y SUS CONCENTRADOS, MINERALES DE ESTAÑO Y SUS CONCENTRADOS, MINERALES DE HIERRO Y SUS CONCENTRADOS, MINERALES DE LITIO , MINERALES DE MANGANESO Y SUS CONCENTRADOS, MINERALES DE MERCURIO Y SUS CONCENTRADOS, MINERALES DE MOLIBDENO Y SUS CONCENTRADOS, MINERALES DE NIQUEL Y SUS CONCENTRADOS, MINERALES DE ORO Y SUS CONCENTRADOS, MINERALES DE PLATA Y SUS CONCENTRADOS, MINERALES DE PLATINO (INCLUYE PLATINO, PALADIO, RUTENIO, RODIO, OSMIO) Y SUS CONCENTRADOS, MINERALES DE PLOMO Y SUS CONCENTRADOS, MINERALES DE POTASIO, MINERALES DE SODIO, MINERALES DE TANTALIO, MINERALES DE TIERRAS RARAS, MINERALES DE TITANIO Y SUS CONCENTRADOS, MINERALES DE VANADIO Y SUS CONCENTRADOS, MINERALES DE WOLFRAMIO (TUNGSTENO) Y SUS CONCENTRADOS, MINERALES DE ZINC Y SUS CONCENTRADOS, MINERALES Y CONCENTRADOS DE TORIO, MINERALES Y CONCENTRADOS DE URANIO, OTRAS PIEDRAS PRECIOSAS, OTRAS PIEDRAS SEMIPRECIOSAS, OTRAS ROCAS METAMÓRFICAS, OTRAS ROCAS Y MINERALES DE ORIGEN VOLCANICO, OTROS MINERALES DE ALUMINIO Y SUS CONCENTRADOS, PIEDRA POMEZ, PIRITA, PIZARRA, RECEBO, ROCA FOSFATICA, ROCA O PIEDRA CALIZA, ROCA O PIEDRA CORALINA, ROCAS DE CUARCITA, ROCAS DE ORIGEN VOLCÁNICO, PUZOLANA, BASALTO, SAL GEMA, SAL MARINA, SULFATO DE BARIO NATURAL-BARITINA, TALCO, YESO",
                    "NOMBRE_DE_TITULAR": "RAFAEL GARCIA DELGADO, JOSE RAMON GUERRERO SANTAFE, ARMANDO RAMIREZ MARIN, JOSE FAUBRICIO ZAMUDIO ANZOLA, ANDRES BOTERO HERRERA",
                    "NUMERO_IDENTIFICACION": "79047350, 10218624, 19341607, 3079240, 79147444",
                    "TIPO_DE_IDENTIFICACION": "Cédula de Ciudadanía, Cédula de Ciudadanía, Cédula de Ciudadanía, Cédula de Ciudadanía, Cédula de Ciudadanía",
                    "IDENTIFICACION_TITULARES": "24816, 27744, 35140, 39582, 41542",
                    "PTO_PTI": "null",
                    "INSTRUMENTO_AMBIENTAL": "N",
                    "DEPARTAMENTOS": "Cundinamarca",
                    "MUNICIPIOS": "YACOPÍ",
                    "GRUPO_DE_TRABAJO": "ZONA CENTRO",
                    "FECHA_TERMINACION": "8/07/2019",
                    "OBJECTID": 3629,
                    "SHAPE_Length": 5.25209810700847E-02,
                    "SHAPE_Area": 6.25627113648606E-05
                }
            }
        ]
    }

    # IDO-08061

    mining_title = arcpy.AsShape(geojson_servicioANM_1, True)
    delimit_proyect_pg = arcpy.MakeFeatureLayer_management(os.path.join(arcpy.env.workspace, "TOPOGRAFIA_LOCAL", "DELIMIT_PROYEC_PG"))
    select_location = arcpy.SelectLayerByLocation_management(delimit_proyect_pg, "ARE_IDENTICAL_TO", mining_title, None, "NEW_SELECTION", "NOT_INVERT")

    if int(arcpy.GetCount_management(select_location)[0]) > 0:
        arcpy.AddMessage("La delimitación del título minero coincide con el polígono estructurado en AnnA Minería. -> DELIMIT_PROYEC_PG")
        return True
    else:
        arcpy.AddError("Error, La delimitación del título minero no coincide con el polígono estructurado en AnnA Minería. -> DELIMIT_PROYEC_PG")
        return False

def get_feature_datasets():
    """
    Get all feature datasets in a geodatabase
    """
    walk_gds = arcpy.da.Walk(topdown=True, datatype="FeatureDataset")
    datasets = []
    dataset_codes = []
    for dirpath, dirnames, _ in walk_gds:
        for gds in dirnames:
            gds_crs = arcpy.Describe(os.path.join(dirpath, gds)).spatialReference.GCSCode
            datasets.append(gds)
            dataset_codes.append(str(gds_crs))
    return pd.DataFrame({'DATASETS': datasets, 'SRSCODES': dataset_codes})


def get_feature_classes():
    """
    Get all feature classes
    """
    walk_fcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for _, dirnames, fnames in walk_fcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    return pd.DataFrame({'FEATURES': feature_classes})

def get_tables():
    """
    Get all tables
    """
    walk_tables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_tables:
        for fc in fnames:
            if not fc.endswith('__ATTACH'):
                tables.append(fc)
    
    return pd.DataFrame({'TABLAS': tables})
    
def reference_system(connection, engine, id, ds_version: Dict[str, List[str]], ds_validacion: List[str]):
    """
    Reference System

    Args:
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    # TODO eliminar connection y usar solo engine.
    # TODO hacer un setup de configs
    """
    id_validador = _id_validador(connection, validador='SISTEMA DE REFERENCIA')
    version = '1'
    codigo, nombre = _get_srs(connection=connection, version=version)
    # TODO cuidado con esto
    codigo = int(codigo)
    df_comp = ds_version[['DS_NOMBRE', 'SRSCODE']]
    version_vs_gdb = df_comp.merge(ds_validacion, 
        how='left', 
        left_on='DS_NOMBRE', 
        right_on='DATASETS')
    
    # verificar si existen datasets ausentes.

    if version_vs_gdb.isna().sum().sum() > 0 :
        ausentes = version_vs_gdb[(version_vs_gdb['DS_NOMBRE'] != version_vs_gdb['DATASETS'])].copy()
        if len(ausentes) > 0:
            ausentes[valw_gdb_mensaje.mensaje_column] = ausentes['DS_NOMBRE'].apply(lambda x : f'Dataset del MDG faltante -> {x}')
    

    # verificar datasets correctos
    presentes = version_vs_gdb[(version_vs_gdb['DS_NOMBRE'] == version_vs_gdb['DATASETS'])].copy()
    if len(presentes) > 0:
        presentes[valw_gdb_mensaje.mensaje_column] = presentes['DS_NOMBRE'].apply(lambda x : f'Dataset correcto -> {x}')

    # verificar sistema de referencia incorrecto
    srs_incorrecto = version_vs_gdb[
        (version_vs_gdb['SRSCODE'] != version_vs_gdb['SRSCODES']) 
        & 
        (version_vs_gdb['DS_NOMBRE'] == version_vs_gdb['DATASETS'])
        ].copy()
    if len(srs_incorrecto)>0:
        srs_incorrecto[valw_gdb_mensaje.mensaje_column] = srs_incorrecto.apply(lambda x: 
            f'Sistema de referencia, incorrecto el sistema debe ser {x["DS_NOMBRE"]} -> {x["SRSCODE"]}', axis=1)

    # verificar sistemas de referencia correcto
    srs_correcto = version_vs_gdb[
        (version_vs_gdb['SRSCODE'] == version_vs_gdb['SRSCODES']) 
        & 
        (version_vs_gdb['DS_NOMBRE'] == version_vs_gdb['DATASETS'])
        ].copy()
    if len(srs_correcto)>0:
        srs_correcto[valw_gdb_mensaje.mensaje_column] = srs_correcto.apply(lambda x: 
            f'Sistema de referencia correcto {x["DS_NOMBRE"]} -> {x["SRSCODE"]}', axis=1)

    final = pd.concat([srs_incorrecto, srs_correcto])
    final[valw_gdb_mensaje.gdb_id_column] = id
    final[valw_gdb_mensaje.validador_id] = id_validador
    final = final[[
        valw_gdb_mensaje.gdb_id_column, 
        valw_gdb_mensaje.validador_id, 
        valw_gdb_mensaje.mensaje_column]].copy()

    final.to_sql(name='VALW_GDB_MENSAJE',
    con=engine, schema=schema, if_exists='append', index=False, chunksize=1000
    )    

def _get_srs(connection, version: str)-> Tuple[str, str]:
    """
    Obtiene el codigo y el sistema de referencia utilizado por una versión específica.
    Args:
        conn : Una conexión a la base de datos.
        version str: la versión de la validación.
    Returns:
        codigo, nombre Tuple[str, str]: codigo y nombre del sistema de referencia de validación.    
    """
    with connection.cursor() as cur:
        return cur.execute(f"""SELECT SRSCODE, NOMBRE FROM {schema}.VALW_SRS SRS
            INNER JOIN MJEREZ.VALW_VERSION VER ON  VER.ID = SRS.VERSION_ID WHERE VER.VERSION = :version""", version=version).fetchone()


def _insert_mensaje(connection, sql: str, id_gdb: int, mensaje: str, id_validador:int)->None:
    # TODO esto debería arrojar excepciones.
    with connection.cursor() as cur:
        cur.execute(sql, [id_gdb, mensaje, id_validador])

def _id_validador(connection, validador:str)->int:
    """
    Retorna el ID de la tabla VALW_DOM_VALIDADORES.
    Args:
        validador: str , Campo DESCRIPCIÓN de la tabla VALW_DOM_VALIDADORES

    RETURNS:
        int: el ID de la DESCRIPCIÓN.
    """
    with connection.cursor() as cur:
        return cur.execute(f"""SELECT ID FROM {schema}.VALW_DOM_VALIDADORES 
            WHERE DESCRIPCION = :validador""", validador=validador).fetchone()[0]
        


def quantity_dataset(connection, id, gvds, gds) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a datasets.

    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("6. Quantity of datasets...")
    arcpy.AddMessage("Verificación dataset")
    count_errors= 0
    id_validador = _id_validador(connection, validador='DATASETS')
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""
    for key_gvds in gvds.items():
        if key_gvds[0] in gds:
            mensaje = f'Dataset correcto -> {key_gvds[0]}'
            
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddMessage(mensaje)
        else:
            count_errors+= 1
            mensaje = f'Dataset del MDG faltante -> {key_gvds[0]}'
            with connection.cursor() as cur:
                _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    for key_gds, _ in gds.items():
        if not key_gds in gvds:
            count_errors+=1
            mensaje = f"Dataset no incluido en el MDG -> {key_gds}"
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)
    
    arcpy.AddMessage(F"Errores encontrados: {count_errors}")

def quantity_feature_class(connection, id, gvfc, gfc) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a feature classes.

    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    Returns:
        None
    """
    id_validador = _id_validador(connection=connection, validador='FEATURE CLASSES')
    
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""
    arcpy.AddMessage("7. Quantity of feature classes...")
    arcpy.AddMessage("Verificación feature class")
    count_errors= 0

    for vfc in gvfc:
        if vfc in gfc:
            mensaje = f'Feature class correcto -> {vfc}'
            arcpy.AddMessage(F"Feature class correcto -> {vfc}")
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
        else:
            count_errors+= 1
            mensaje = f'Feature class del MDG faltante -> {vfc}'
            arcpy.AddError(F"Feature class del MDG faltante -> {vfc}")
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)


    for fc in gfc:
        if not fc in gvfc:
            count_errors+= 1
            arcpy.AddError(F"Feature class no incluido en el MDG -> {fc}")

    arcpy.AddMessage(F"Errores encontrados: {count_errors}")

def quantity_tables(connection, id, gvtbl, gtbl) -> None:
    """
    Persiste la información de las diferencias o exactitudes de la validación referente a tablas.

    Args:
        connection: Conexión a la base de datos.
        id int: identificador de la gdba en la tabla VALW_GDBS_VALIDAR.
        gvsd str: datasets de la versión.
        gvds Dict[str, List[str]]: Datasets de la versión
        gds Dict[str, List[str]]: Datasets de la gdb a ser comprobada.
    Returns:
        None
    """
    arcpy.AddMessage("8. Quantity of tables...")
    arcpy.AddMessage("Verificación tablas")
    count_errors= 0
    id_validador = _id_validador(connection=connection, validador='TABLAS')
    
    sql = """INSERT INTO MJEREZ.VALW_GDB_MENSAJE(GDB_ID, MENSAJE_VAL, VALIDADOR_ID) VALUES (:id_db, :mensaje, :id_validador)"""
    for vtbl in gvtbl:
        if vtbl in gtbl:
            mensaje = f'Tabla o ficha correcta -> {vtbl}'
            arcpy.AddMessage(F"Tabla o ficha correcta -> {vtbl}")
        else:
            count_errors+= 1
            mensaje = f'Tabla o ficha correcta -> {vtbl}'
            arcpy.AddError(F"Tabla o ficha del MDG faltante -> {vtbl}")
        _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)


    for tbl in gtbl:
        if tbl not in gvtbl:
            count_errors+= 1
            mensaje = f'Tabla o ficha no incluido en el MDG -> {tbl}'
            _insert_mensaje(connection=connection, sql=sql, id_gdb= id, mensaje=mensaje, id_validador=id_validador)
            arcpy.AddError(mensaje)

    arcpy.AddMessage(F"Errores encontrados: {count_errors}")


