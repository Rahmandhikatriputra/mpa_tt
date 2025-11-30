from modules.create_database import SchemaCreation
from modules.insert_to_database import DataInsert

def main():
# define the credential of the database
    database_name = "mpa_tt"
    user_database = "postgres"
    password_database = "postgres"
    host_database = "postgres" 
    port_database = "5432"

    # data source file directory
    data_source_dir = "/app/data"

    # begin to create the schemas and tables
    create_schema = SchemaCreation(database_name, user_database, password_database, host_database, port_database)
    create_schema.create_schema()

    # begin to insert the data
    insert_data = DataInsert(data_source_dir, create_schema)
    insert_data.insert_process()    
    insert_data.normalizing_schema()
    insert_data.test_db()

if __name__ == "__main__":
    main()


