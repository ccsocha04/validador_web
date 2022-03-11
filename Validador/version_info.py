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
    #w#dsaf alk_gvds = arcpy.da.Walk(topdown=True, datatype="FeatureDataset")
    feature_datasets_dict = dict()
    with connection.cursor() as cur:
        for row in cur.execute("""SELECT vd.NOMBRE , vs.SRSCODE, vs.NOMBRE FROM MJEREZ.VALW_DATASETS vd
            INNER JOIN MJEREZ.VALW_SRS vs ON vd.SRS_ID = vs.ID"""):
            feature_datasets_dict[row[0]] = [row[1], row[2]]

    arcpy.AddMessage(F"1.1 Version_Datasets: {len(feature_datasets_dict)}")

    return feature_datasets_dict


def get_version_feature_classes() -> List[str]:
    """
    Get all feature classes in a geodatabase

    This function is used to get the base feature classes it would be better to read this from DB
    """
    walk_gvfcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for _, _, fnames in walk_gvfcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    arcpy.AddMessage(F"1.2 Version_Feature_Classes: {len(feature_classes)}")
    
    return feature_classes

def get_version_tables() -> List[str]:
    """
    Get all tables in a geodatabase

    This function is used to get the base tables it would be better to read this from DB
    """
    walk_gvtables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_gvtables:
        for table in fnames:
            tables.append(table)
    
    arcpy.AddMessage(F"1.3 Version_Tables: {len(tables)}")
    
    return tables