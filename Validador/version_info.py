import arcpy
import os
import sys
import pandas as pd


from importlib.resources import path
from typing import List, Dict
from zipfile import ZipFile


def get_version_datasets(connection) -> Dict[str, List[str]]:
    """
    Get all feature datasets in a geodatabase

    """
    arcpy.AddMessage("1. Getting version ...")
    feature_datasets_dict = dict()
    with connection.cursor() as cur:
        for row in cur.execute("""SELECT vd.NOMBRE , vs.SRSCODE, vs.NOMBRE FROM MJEREZ.VALW_DATASETS vd
            INNER JOIN MJEREZ.VALW_SRS vs ON vd.SRS_ID = vs.ID"""):
            feature_datasets_dict[row[0]] = [row[1], row[2]]

    arcpy.AddMessage(F"1.1 Version_Datasets: {len(feature_datasets_dict)}")

    return feature_datasets_dict


def get_version_feature_classes(connection) -> List[str]:
    """
    Get all feature classes in a geodatabase

    This function is used to get the base feature classes it would be better to read this from DB
    """
    feature_classes = []
    with connection.cursor() as cur:
        for row in cur.execute("""SELECT DISTINCT NOMBRE_OBJETO FROM VALW_OBJETOS_TOTALES vot"""):
            feature_classes.append(row[0])

    
    arcpy.AddMessage(F"1.2 Version_Feature_Classes: {len(feature_classes)}")
    
    return feature_classes

def get_version_tables(connection) -> List[str]:
    """
    Get all tables in a geodatabase

    This function is used to get the base tables it would be better to read this from DB
    """
    tables = []
    with connection.cursor() as cur:
        for row in cur.execute("""SELECT DISTINCT FICHAX FROM VALW_VALIDAR_TABLAS"""):
            if not row[0].endswith('__ATTACH'):
                tables.append(row[0])
    
    arcpy.AddMessage(F"1.3 Version_Tables: {len(tables)}")
    
    return tables