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
    arcpy.AddMessage("Getting version datasets...")
    walk_gvds = arcpy.da.Walk(topdown=True, datatype="FeatureDataset")
    feature_datasets = []
    for dirpath, dirnames, fnames in walk_gvds:
        for gvds in dirnames:
            feature_datasets.append(gvds)
    
    print("Version_Datasets: ",len(feature_datasets))
    
    return feature_datasets

def get_version_feature_classes(version: str) -> List[str]:
    """
    Get all feature classes in a geodatabase
    """
    arcpy.AddMessage("Getting version feature classes...")
    walk_gvfcs = arcpy.da.Walk(topdown=True, datatype="FeatureClass")
    feature_classes = []
    for dirpath, dirnames, fnames in walk_gvfcs:
        for fc in fnames:
            feature_classes.append(fc)
    
    print("Version_Feature_Classes: ",len(feature_classes))
    
    return feature_classes

def get_version_tables(version: str) -> List[str]:
    """
    Get all tables in a geodatabase
    """
    arcpy.AddMessage("Getting version tables...")
    walk_gvtables = arcpy.da.Walk(topdown=True, datatype="Table")
    tables = []
    for dirpath, dirnames, fnames in walk_gvtables:
        for table in fnames:
            tables.append(table)
    
    print("Version_Tables: ",len(tables))
    
    return tables

def extract_files(file_path, extract_path):
    """
    Extract all files from a zip file
    """
    arcpy.AddMessage("Extracting files...")
    
    with ZipFile(file_path, "r") as zip:
        
        file_name = os.path.basename(zip.filename).split(".")[0]
        file_format = os.path.basename(zip.filename).split(".")[1]
        file_date = file_name.split("_")[1]

        arcpy.AddMessage("File name: {}".format(file_name))
        arcpy.AddMessage("File format: {}".format(file_format))
        arcpy.AddMessage("File date: {}".format(file_date))

        list_files = zip.namelist()

        for file in list_files:
            print("Extracting file: {}".format(file))

        # zip.extractall(path=extract_path)

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
    
    print("Feature_DataSets: ",len(feature_datasets))
    print(feature_datasets)
    
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
    
    print("Feature_Classes: ",len(feature_classes))
    print(feature_classes)

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
    
    print("Tables: ",len(tables))
    print(tables)
    
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
    
    # Inputs
    file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\0-178_202200301.gdb.zip"
    # file_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\0-179_202200301.zip"
    extract_path = r"C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data"

    # Set the workspace
    """
    in_workspace = os.path.dirname(file_path)
    arcpy.AddMessage("Set the workspace...")
    db_name = (in_workspace.split("\\")[-1]).split(".")[0]
    arcpy.env.workspace = in_workspace
    """

    # Extract files
    # extract_files(file_path, extract_path)
    #fds = get_feature_datasets()