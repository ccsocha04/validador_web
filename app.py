from runpy import run_path

from attr import attributes
from Validador.validator_web import (extract_files, get_feature_classes, get_feature_datasets, get_tables,  
                                    quantity_dataset, quantity_feature_class, 
                                    quantity_tables, reference_system, spatial_matching, 
                                    quantity_required, feature_attributes)
from Validador.version_info import (get_version_datasets, get_version_feature_classes, get_version_tables, 
                                    get_version_required, get_version_attributes)
from utils.utils import set_workspace
from pathlib import Path
from database.connection import con
from database.gdb_path_to_validate import por_validar, gdb_para_validar, update_estado, borrar_registros_mensajes


if __name__ == '__main__':
    # cantidad = por_validar(con)

    #if cantidad > 0:
    path = r'C:\UTGI\SoftwareEstrategico\ANNA\Python\Validador_Web\Data\IDO-08061_202203071.gdb'
    
    expediente = 'IDO-08061'
    documento_tecnico = 'Formato Básico Minero - FBM'
    # etapa = 'Construcción y montaje'
    etapa = 'Exploración'

    id_bd_gdb, ruta_gdb = gdb_para_validar(con, gdb=path)
    
    update_estado(con, id=id_bd_gdb, estado='En proceso')
    borrar_registros_mensajes(con, id=id_bd_gdb)

    # Get version GDB
    
    # Feature Datasets
    version_ds = get_version_datasets(con)
    # Feature Classes
    version_fc = get_version_feature_classes(con)
    # Tables
    version_tbl = get_version_tables(con)
    # Required objects
    version_req = get_version_required(con, documento_tecnico, etapa)
    # Features attributes
    # version_att = get_version_attributes(con)

    # print(version_att)
    
    # file_path = str(current_path.parent.absolute().joinpath(data_folder).joinpath(zip_file))
    # TODO esto debe ser leido de la base de datos
    # extract_path = str(current_path.parent.absolute().joinpath(data_folder))

    # Extract files
    # TODO verificar que path no termine en zip. si es así se debe desempaquetar y actualizar la ruta
    #  enla base de datos
    # if ruta_gdb.endswith('.zip'):
    #    extract_files(file_path, extract_path)
    set_workspace(ruta_gdb)

    # Get Datasets
    ds = get_feature_datasets()

    # Get Feature Classes
    fc = get_feature_classes()

    # Get Feature Classes
    tbl = get_tables()
    
    # Validator 1 - Spatial Matching
    spatial_matching(con, id_bd_gdb, expediente)

    # Validator 2 - Reference System
    reference_system(con, id_bd_gdb, version_ds, ds)

    # Validator 3 - Quantity of datasets
    quantity_dataset(con, id_bd_gdb, version_ds, ds)

    # Validator 4 - Quantity of feature classes
    quantity_feature_class(con, id_bd_gdb, version_fc, fc)

    # Validator 5 - Quantity of tables
    quantity_tables(con, id_bd_gdb, version_tbl, tbl)

    # Validator 6 - Required
    quantity_required(con, id_bd_gdb, version_req, fc)

    # Validator 7 - Attributive
    # feature_attributes(con, id_bd_gdb, version_fc, fc)

    # TODO actualizar estado de la gdb

    update_estado(con, id=id_bd_gdb, estado='Finalizado')