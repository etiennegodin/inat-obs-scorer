from .functions import CreateSchema, DataBaseConnection
from ..pipeline import Module, SubModule
# CREATE ALL SCHEMAS FOR FUTURE TABLE AND VIEWS

create_raw_schema = CreateSchema('create_raw_schema', schema= 'raw')
create_clean_schema = CreateSchema("create_clean_schema", schema = "clean" )
create_preprocessed_schema = CreateSchema("create_preprocessed_schema", schema = "preprocessed" )
create_features_schema = CreateSchema("create_features_schema", schema = "features" )
create_observers_schema = CreateSchema("create_observers_schema", schema = "observers")
create_labeled_schema = CreateSchema("create_labeled_schema", schema = "labeled")
create_score_schema = CreateSchema("create_labeled_schema", schema = "score")
create_encoded_schema = CreateSchema("create_encoded_schema", schema = "encoded")
create_transformed_schema = CreateSchema("create_transformed_schema", schema = "transformed")


create_all_schemas = SubModule('create_all_schemas',[create_raw_schema,
                                                create_clean_schema,
                                                create_preprocessed_schema,
                                                create_features_schema,
                                                create_observers_schema,
                                                create_labeled_schema,
                                                create_score_schema,
                                                create_encoded_schema,
                                                create_transformed_schema])


#Create init module with db connection d
db_connection = DataBaseConnection()

db_init = Module('init', [db_connection, create_all_schemas], always_run= True)