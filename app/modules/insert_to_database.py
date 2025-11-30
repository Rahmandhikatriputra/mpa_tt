import psycopg2
import pandas as pd
import os

#Create a class Staging

class DataInsert:
    def __init__(self, data_source_dir, db_creator):
        self.data_source_dir = data_source_dir
        self.database_name = db_creator.database_name
        self.user_database = db_creator.user_database
        self.password_database = db_creator.password_database
        self.host_database = db_creator.host_database
        self.port_database = db_creator.port_database
    
    #create data ingestion program to raw table
    def insert_process(self):
        # raw_data = os.listdir(self.data_source_dir)

        #connect to db
        with psycopg2.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
            with conn.cursor() as cur:
                #insert csv data into database
                
                #raw.interchange
                interchange_dataset = pd.read_csv(os.path.join(self.data_source_dir, "interchange.csv"))
                cur.execute("TRUNCATE raw.interchange")
                insert_query = """
                                INSERT INTO raw.interchange (
                                part_number,
                                alt_part_number,
                                notes
                                ) VALUES (%s, %s, %s)
                                """
                cur.executemany(insert_query, interchange_dataset.values.tolist())

                #raw.inventory
                inventory_dataset = pd.read_csv(os.path.join(self.data_source_dir, "inventory.csv"))
                cur.execute("TRUNCATE raw.inventory")
                insert_query = """
                                INSERT INTO raw.inventory (
                                branch,
                                part_number,
                                part_name,
                                category,
                                stock_on_hand,
                                min_stock,
                                uom
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """
                cur.executemany(insert_query, inventory_dataset.values.tolist())
                
                # raw.sales
                sales_dataset = pd.read_csv(os.path.join(self.data_source_dir, "sales.csv"))
                sales_dataset["sale_date"] = sales_dataset["sale_date"].apply(lambda x: f"{int(x.split("/")[0]):02d}" + "/" + f"{int(x.split("/")[1]):02d}" + "/" + x.split("/")[2])
                cur.execute("TRUNCATE raw.sales")
                insert_query = """
                                INSERT INTO raw.sales (
                                sale_id,
                                branch,
                                customer,
                                part_number,
                                part_name,
                                category,
                                qty,
                                unit_price,
                                currency,
                                sale_date
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)
                                """
                cur.executemany(insert_query, sales_dataset.values.tolist())

                # raw.transfers
                transfers_dataset = pd.read_csv(os.path.join(self.data_source_dir, "transfers.csv"))
                transfers_dataset["transfer_date"] = transfers_dataset["transfer_date"].apply(lambda x: f"{int(x.split("/")[0]):02d}" + "/" + f"{int(x.split("/")[1]):02d}" + "/" + x.split("/")[2])
                cur.execute("TRUNCATE raw.transfers")
                insert_query = """
                                INSERT INTO raw.transfers (
                                transfer_id,
                                from_branch,
                                to_branch,
                                part_number,
                                qty,
                                transfer_date
                                ) VALUES (%s, %s, %s, %s, %s, %s)
                                """
                cur.executemany(insert_query, transfers_dataset.values.tolist())

    # process the dataset       
    def normalizing_schema(self):
        with psycopg2.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
             with conn.cursor() as cur:
                # standard the data into staging table
                 cur.execute("""
                            INSERT INTO staging.interchange (
                                                            part_number,
                                                            alt_part_number,
                                                            notes
                                                            )
                            SELECT part_number, alt_part_number, notes
                            FROM raw.interchange
                            ON CONFLICT (part_number) DO UPDATE
                            SET 
                                alt_part_number = EXCLUDED.alt_part_number,
                                notes = EXCLUDED.notes;

                            TRUNCATE staging.inventory;

                            INSERT INTO staging.inventory (
                                                            branch,
                                                            part_number,
                                                            part_name,
                                                            category,
                                                            stock_on_hand,
                                                            min_stock,
                                                            uom
                                                            )
                            SELECT 
                                    branch,
                                    part_number,
                                    part_name,
                                    category,
                                    CAST(stock_on_hand AS INTEGER) AS stock_on_hand,
                                    CAST(min_stock AS INTEGER) AS min_stock,
                                    uom
                            FROM raw.inventory;
                            
                            INSERT INTO staging.sales (
                                                            sale_id,
                                                            branch,
                                                            customer,
                                                            part_number,
                                                            part_name,
                                                            category,
                                                            qty,
                                                            unit_price,
                                                            currency,
                                                            sale_date
                                                            )
                            SELECT 
                                    CAST(sale_id AS INTEGER) AS sale_id,
                                    branch,
                                    customer,
                                    part_number,
                                    part_name,
                                    category,
                                    CAST(qty AS INTEGER) AS qty,
                                    CAST(unit_price AS REAL) AS unit_price,
                                    currency,
                                    TO_DATE(sale_date, 'MM/DD/YYYY') AS sale_date
                            FROM raw.sales
                            ON CONFLICT (sale_id) DO NOTHING;
                             
                            INSERT INTO staging.transfers (
                                                            transfer_id,
                                                            from_branch,
                                                            to_branch,
                                                            part_number,
                                                            qty,
                                                            transfer_date
                                                            )
                            SELECT 
                                CAST(transfer_id AS INTEGER) AS transfer_id,
                                from_branch,
                                to_branch,
                                part_number,
                                CAST(qty AS INTEGER) AS qty,
                                TO_DATE(transfer_date, 'MM/DD/YYYY') AS transfer_date
                            FROM raw.transfers
                            ON CONFLICT (transfer_id) DO NOTHING;
                            
                            TRUNCATE processed.top_movers;
                            TRUNCATE processed.restocking_needs;
                            TRUNCATE processed.lost_sales_analysis;
                            TRUNCATE processed.supply_chain_optimization;

                           INSERT INTO processed.top_movers (
                                                            branch,
                                                            part_number,
                                                            part_name,
                                                            qty_been_sold
                            )
                            SELECT
                                    branch, 
                                    part_number, 
                                    part_name, 
                                    SUM(qty) AS qty
                            FROM staging.sales
                            GROUP BY part_name, part_number, branch
                            ORDER BY qty DESC
                            LIMIT 10;

                            INSERT INTO processed.restocking_needs (
                            branch,
                            part_number,
                            part_name,
                            stock_on_hand_in_pcs,
                            min_stock_in_pcs
                            )
                            SELECT
                                    branch, 
                                    part_number,
                                    part_name, 
                                    stock_on_hand,
                                    min_stock
                            FROM staging.inventory
                            WHERE min_stock > stock_on_hand;
                             
                            INSERT INTO processed.lost_sales_analysis(
                            branch,
                            part_number,
                            finished_part,
                            alternative_part
                            )
                            SELECT 
                                    processed.restocking_needs.branch,
                                    processed.restocking_needs.part_number,
                                    processed.restocking_needs.part_name,
                                    staging.interchange.alt_part_number
                            FROM processed.restocking_needs
                            INNER JOIN staging.interchange ON processed.restocking_needs.part_number = staging.interchange.part_number;
                            
                            INSERT INTO processed.supply_chain_optimization(
                            "branch-part_number",
                            part_name,
                            amount_of_excessed_stock,
                            qty_been_sold
                            )
                            WITH
                                branch_excess_stock AS (
                                SELECT
                                    CONCAT(branch, '-', part_number) AS "branch-part_number",
                                    part_name, 
                                    stock_on_hand,
                                    min_stock
                                FROM staging.inventory
                                WHERE min_stock < stock_on_hand
                                ),
                                highest_demand_part AS (
                                SELECT
                                        CONCAT(branch, '-', part_number) AS "branch-part_number",
                                        branch, 
                                        part_number, 
                                        part_name, 
                                        SUM(qty) AS qty
                                FROM staging.sales
                                GROUP BY part_name, part_number, branch
                                ORDER BY qty DESC
                                )
                                SELECT branch_excess_stock."branch-part_number",
                                branch_excess_stock.part_name,
                                branch_excess_stock.stock_on_hand - branch_excess_stock.min_stock AS excess_stock_amount,
                                highest_demand_part.qty 
                                FROM branch_excess_stock
                                INNER JOIN highest_demand_part ON branch_excess_stock."branch-part_number" = highest_demand_part."branch-part_number"
                                ORDER BY highest_demand_part.qty DESC;
                             """)
    
    #create a method to test the db
    def test_db(self):
        with psycopg2.connect(f"dbname={self.database_name} user={self.user_database} password={self.password_database} host={self.host_database} port={self.port_database}") as conn:
             with conn.cursor() as cur:
                 cur.execute("SELECT * FROM processed.restocking_needs")

                 #print query result
                 results = cur.fetchall()
                 for row in results:
                     print(row)
        

