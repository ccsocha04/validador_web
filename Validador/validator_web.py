from importlib.resources import path
import os
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
    feature_datasets = []
    feature_datasets_crs = []
    for dirpath, dirnames, fnames in walk_gvds:
        for gvds in dirnames:
            feature_datasets.append(gvds)
            gvds_crs = arcpy.Describe(os.path.join(dirpath, gvds)).spatialReference.GCSCode
            feature_datasets_crs.append(gvds_crs)
    
    combined_list = list(zip(feature_datasets, feature_datasets_crs))
    arcpy.AddMessage(F"1.1 Version_Datasets: {len(combined_list)}")

    return combined_list

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
    Convert all files to a geodatabase
    """
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

def get_feature_datasets():
    """
    Get all feature datasets in a geodatabase
    """
    arcpy.AddMessage("Getting feature datasets...")
    walk_fds = arcpy.da.Walk(topdown=True, datatype="FeatureDataset")
    feature_datasets = []
    for dirpath, dirnames, fnames in walk_fds:
        for fd in dirnames:
            feature_datasets.append(fd)
            fds_crs = arcpy.Describe(os.path.join(dirpath, fd)).spatialReference.GCSCode
    
    arcpy.AddMessage(F"Feature_DataSets: {len(feature_datasets)}")
    arcpy.AddMessage(feature_datasets)
    
    get_feature_classes()
    
    return feature_datasets


def get_feature_classes():
    """
    Get all feature classes in a feature dataset
    """
    arcpy.AddMessage("Getting feature classes...")
    walk_fcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for dirpath, dirnames, fnames in walk_fcs:
        for fc in fnames:
            feature_classes.append(fc)
            fc_crs = arcpy.Describe(os.path.join(dirpath, fc)).spatialReference.GCSCode
    
    arcpy.AddMessage(F"Feature_Classes: {len(feature_classes)}")
    arcpy.AddMessage(feature_classes)

    get_tables()

    return feature_classes


def get_tables():
    """
    Get all tables in a geodatabase
    """
    arcpy.AddMessage("Getting tables...")
    walk_tables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_tables:
        for table in fnames:
            tables.append(table)
    
    arcpy.AddMessage(F"Tables: {len(tables)}")
    arcpy.AddMessage(tables)
    
    return tables


if __name__ == '__main__':

    # Get version GDB
    gdb_structure = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Estructura\SIGM_MINERIA.gdb"
    path = arcpy.env.workspace = gdb_structure
    # Feature Datasets
    version_ds = get_version_datasets(path)
    # Feature Classes
    version_fc = get_version_feature_classes(path)
    # Tables
    version_tables = get_version_tables(path)
    
    # Get input GDB / GPKG    
    file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\0-178_202200301.gdb.zip"
    # file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\0-179_202200301.zip"
    extract_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data"

    # Extract files
    extract_files(file_path, extract_path)

    # Validate 1
    #fds = get_feature_datasets()

    # Validate 2
    #fds = get_feature_datasets()

    # Validate 3
    #fds = get_feature_datasets()

    # Validate 4
    #fds = get_feature_datasets()

    # Validate 5
    #fds = get_feature_datasets()