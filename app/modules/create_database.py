import psycopg2

#create schema creation class to create 3 schemas: staging, raw and normalized:
class SchemaCreation:
    def __init__(self, database_name, user_database, password_database, host_database, port_database):
        self.database_name = database_name
        self.user_database = user_database
        self.password_database = password_database
        self.host_database = host_database
        self.port_database = port_database
        
    def create_schema(self):
        with psycopg2.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
            with conn.cursor() as cur:
                cur.execute("""CREATE SCHEMA IF NOT EXISTS raw;
                            CREATE SCHEMA IF NOT EXISTS staging;
                            CREATE SCHEMA IF NOT EXISTS processed;

                            CREATE TABLE IF NOT EXISTS raw.interchange(
                            part_number text,
                            alt_part_number text,
                            notes text
                            );
                            
                            CREATE TABLE IF NOT EXISTS raw.inventory(
                            branch text,
                            part_number text,
                            part_name text,
                            category text,
                            stock_on_hand text,
                            min_stock text,
                            uom text
                            );

                            CREATE TABLE IF NOT EXISTS raw.sales(
                            sale_id text,
                            branch text,
                            customer text,
                            part_number text,
                            part_name text,
                            category text,
                            qty text,
                            unit_price text,
                            currency text,
                            sale_date text
                            );

                            CREATE TABLE IF NOT EXISTS raw.transfers(
                            transfer_id text,
                            from_branch text,
                            to_branch text,
                            part_number text,
                            qty text,
                            transfer_date text
                            );

                            CREATE TABLE IF NOT EXISTS staging.interchange(
                            part_number text PRIMARY KEY,
                            alt_part_number text,
                            notes text
                            );
                            
                            CREATE TABLE IF NOT EXISTS staging.inventory(
                            branch text,
                            part_number text,
                            part_name text,
                            category text,
                            stock_on_hand integer,
                            min_stock integer,
                            uom text
                            );

                            CREATE TABLE IF NOT EXISTS staging.sales(
                            sale_id integer PRIMARY KEY,
                            branch text,
                            customer text,
                            part_number text,
                            part_name text,
                            category text,
                            qty integer,
                            unit_price float,
                            currency text,
                            sale_date date
                            );

                            CREATE TABLE IF NOT EXISTS staging.transfers(
                            transfer_id integer PRIMARY KEY,
                            from_branch text,
                            to_branch text,
                            part_number text,
                            qty integer,
                            transfer_date date
                            );

                            CREATE TABLE IF NOT EXISTS processed.top_movers(
                            branch text,
                            part_number text,
                            part_name text,
                            qty_been_sold integer
                            );

                            CREATE TABLE IF NOT EXISTS processed.restocking_needs(
                            branch text,
                            part_number text,
                            part_name text,
                            stock_on_hand_in_pcs integer,
                            min_stock_in_pcs integer
                            );

                            CREATE TABLE IF NOT EXISTS processed.supply_chain_optimization(
                            "branch-part_number" text,
                            part_name text,
                            amount_of_excessed_stock integer,
                            qty_been_sold integer
                            );

                            CREATE TABLE IF NOT EXISTS processed.lost_sales_analysis(
                            branch text,
                            part_number text,
                            finished_part text,
                            alternative_part text
                            )

                            """)