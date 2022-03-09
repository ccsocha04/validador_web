from importlib.resources import path
import os
import sys
from typing import List
import arcpy
from zipfile import ZipFile
import pandas as pd

def get_version_datasets(version: str) -> List[str]:
    """
    Get all feature datasets in a geodatabase
    """
    arcpy.AddMessage("1. Getting version ...")
    walk_gvds = arcpy.da.Walk(topdown=True, datatype="FeatureDataset")
    feature_datasets_dict = dict()
    for dirpath, dirnames, fnames in walk_gvds:
        for gvds in dirnames:
            gvds_crs = arcpy.Describe(os.path.join(dirpath, gvds)).spatialReference
            feature_datasets_dict[gvds] = [gvds_crs.GCSCode, gvds_crs.GCSName]

    arcpy.AddMessage(F"1.1 Version_Datasets: {len(feature_datasets_dict)}")

    return feature_datasets_dict

def get_version_feature_classes(version: str) -> List[str]:
    """
    Get all feature classes in a geodatabase
    """
    walk_gvfcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for dirpath, dirnames, fnames in walk_gvfcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    arcpy.AddMessage(F"1.2 Version_Feature_Classes: {len(feature_classes)}")
    
    return feature_classes

def get_version_tables(version: str) -> List[str]:
    """
    Get all tables in a geodatabase
    """
    walk_gvtables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_gvtables:
        for table in fnames:
            tables.append(table)
    
    arcpy.AddMessage(F"1.3 Version_Tables: {len(tables)}")
    
    return tables

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

def conversion_format(path: str):
    """
    Convert all files to geodatabase
    """
    # TODO - Add conversion to geodatabase
    arcpy.AddMessage("2.4 Converting files to geodatabase...")
    arcpy.AddMessage("2.5 Converting feature datasets...")
    arcpy.AddMessage("2.6 Converting feature classes...")
    arcpy.AddMessage("2.7 Converting tables...")

def set_workspace(path: str):
    """
    Set workspace
    """
    arcpy.AddMessage("3. Setting workspace...")
    arcpy.env.workspace = path
    arcpy.env.overwriteOutput = True

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
    feature_datasets_dict = dict()
    for dirpath, dirnames, fnames in walk_gds:
        for gds in dirnames:
            gds_crs = arcpy.Describe(os.path.join(dirpath, gds)).spatialReference.GCSCode
            feature_datasets_dict[gds] = gds_crs
    return feature_datasets_dict

def get_feature_classes():
    """
    Get all feature classes
    """
    walk_fcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for dirpath, dirnames, fnames in walk_fcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    return feature_classes

def get_tables():
    """
    Get all tables
    """
    walk_tables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_tables:
        for fc in fnames:
            tables.append(fc)
    
    return tables
    
def reference_system(gvds, gds):
    """
    Reference Sysetm
    """
    arcpy.AddMessage("5. Reference system...")
    arcpy.AddMessage("Verificación sistema de referencia")
    count_errors= 0
    for key_gds, value_gds in gds.items():
        if key_gds in gvds:
            if value_gds == gvds[key_gds][0]:
                arcpy.AddMessage(F"Sistema de referencia correcto {gvds[key_gds][1]} (EPSG: {gvds[key_gds][0]}) -> {key_gds}")
            else:
                count_errors+= 1
                arcpy.AddError(F"Sistema de referencia, el sistema correcto debe ser {gvds[key_gds][1]} (EPSG: {gvds[key_gds][0]}) -> {key_gds}")
    arcpy.AddMessage(F"Errores encontrados: {count_errors}")


def quantity_dataset(gvds, gds):
    """
    Quantity of datasets
    """
    arcpy.AddMessage("6. Quantity of datasets...")
    arcpy.AddMessage("Verificación dataset")
    count_errors= 0
    for key_gvds in gvds.items():
        if key_gvds[0] in gds:
            arcpy.AddMessage(F"Dataset correcto -> {key_gvds[0]}")
        else:
            count_errors+= 1
            arcpy.AddError(F"Dataset del MDG faltante -> {key_gvds[0]}")
    for key_gds, value_gds in gds.items():
        if not key_gds in gvds:
            count_errors+= 1
            arcpy.AddError(F"Dataset no incluido en el MDG -> {key_gds}")

    arcpy.AddMessage(F"Errores encontrados: {count_errors}")

def quantity_feature_class(gvfc, gfc):
    """
    Quantity of feature classes
    """
    arcpy.AddMessage("7. Quantity of feature classes...")
    arcpy.AddMessage("Verificación feature class")
    count_errors= 0

    for vfc in gvfc:
        if vfc in gfc:
            arcpy.AddMessage(F"Feature class correcto -> {vfc}")
        else:
            count_errors+= 1
            arcpy.AddError(F"Feature class del MDG faltante -> {vfc}")

    for fc in gfc:
        if not fc in gvfc:
            count_errors+= 1
            arcpy.AddError(F"Feature class no incluido en el MDG -> {fc}")

    arcpy.AddMessage(F"Errores encontrados: {count_errors}")

def quantity_tables(gvtbl, gtbl):
    """
    Quantity of tables
    """
    arcpy.AddMessage("8. Quantity of tables...")
    arcpy.AddMessage("Verificación tablas")
    count_errors= 0

    for vtbl in gvtbl:
        if vtbl in gtbl:
            arcpy.AddMessage(F"Tabla o ficha correcta -> {vtbl}")
        else:
            count_errors+= 1
            arcpy.AddError(F"Tabla o ficha del MDG faltante -> {vtbl}")

    for tbl in gtbl:
        if not tbl in gvtbl:
            count_errors+= 1
            arcpy.AddError(F"Tabla o ficha no incluido en el MDG -> {tbl}")

    arcpy.AddMessage(F"Errores encontrados: {count_errors}")


if __name__ == '__main__':

    # Get version GDB
    gdb_structure = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Estructura\SIGM_MINERIA.gdb"
    path = arcpy.env.workspace = gdb_structure
    arcpy.env.overwriteOutput = True
    # Feature Datasets
    version_ds = get_version_datasets(path)
    # Feature Classes
    version_fc = get_version_feature_classes(path)
    # Tables
    version_tbl = get_version_tables(path)
    
    # Get input GDB / GPKG

    # Test - GDB Spatial Matching -> Expediente IDO-08061_20220303 -> False
    # file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\IDO-08061_20220303.gdb.zip"
    # Test - GDB Spatial Matching -> Expediente IDO-08061_20220303 -> True
    # file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\IDO-08061_20220307.gdb.zip"
    # Test - GDB Spatial Matching -> Expediente GBH-141_20220303 -> True
    # file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\GBH-141_20220303.gdb.zip"

    # Test - GDB Spatial Matching - System Reference -> Expediente IDO-08061_20220303 -> False
    file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\IDO-08061_202203071.gdb.zip"
    
    # Test - GDB GPKG -> Expediente IDO-08062_20220303
    # file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\IDO-08062_20220303.zip"
    
    extract_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data"

    # Extract files
    extract_files(file_path, extract_path)

    # Get Datasets
    ds = get_feature_datasets()

    # Get Feature Classes
    fc = get_feature_classes()

    # Get Feature Classes
    tbl = get_tables()
    
    # Validator 1 - Spatial Matching
    validate_1 = spatial_matching()

    # Validator 2 - Reference System
    validate_2 = reference_system(version_ds, ds)

    # Validator 3 - Quantity of datasets
    validate_3 = quantity_dataset(version_ds, ds)

    # Validator 4 - Quantity of feature classes
    validate_4 = quantity_feature_class(version_fc, fc)

    # Validator 5 - Quantity of tables
    validate_5 = quantity_tables(version_tbl, tbl)

    # Validator 6 - Required fields
    # Validator 7 - Attributive characteristics