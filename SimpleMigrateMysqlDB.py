import mysql.connector
import re
from tqdm import tqdm

class SimpleMigrateMysqlDB:
    def __init__(self, source_conn, dest_conn):
        """
        Initialize a SimpleMigrateMysqlDB instance.

        Parameters:
        source_conn (dict): The source db config.
        dest_conn (dict): The destination db config.
        """
        self.source_conn = source_conn
        self.dest_conn = dest_conn

    def get_create_table_ddl(self, connection, table_name, otype):
        """
        Retrieve the CREATE TABLE or VIEW statement for the specified table or view.

        This function executes a SHOW CREATE TABLE or SHOW CREATE VIEW query to retrieve
        the SQL statement that creates the specified table or view. It then processes
        the statement to remove unnecessary options, such as ENGINE, DEFAULT CHARSET,
        COLLATE, and ALGORITHM.

        Parameters:
        connection (mysql.connector.Connection): A connection to the MySQL database.
        table_name (str): The name of the table or view to retrieve the statement for.
        otype (str): The type of object to retrieve the statement for ("TABLE" or "VIEW").

        Returns:
        str: The processed CREATE TABLE or VIEW statement for the specified object.
        """
        cursor = connection.cursor()
        cursor.execute(f"SHOW CREATE {otype} {table_name}")
        create_table_sql = cursor.fetchone()[1]

        create_table_sql = create_table_sql.replace(' ENGINE=InnoDB', '')
        create_table_sql = create_table_sql.replace(' DEFAULT CHARSET=utf8mb4', '')
        create_table_sql = create_table_sql.replace(' COLLATE=utf8mb4_0900_ai_ci', '')
        create_table_sql = create_table_sql.replace(' ALGORITHM=UNDEFINED DEFINER=`admin`@`%` SQL SECURITY DEFINER', '')

        return create_table_sql

    def get_tables_and_views(self, source_conn):
        """
        Generate DDL statements for tables and views from the source database.

        This function connects to the source database and retrieves a list of table and view names
        along with their types (TABLE or VIEW) using the information_schema. For each table or view,
        it generates the corresponding DDL statement using the get_create_table_ddl function and
        categorizes them into lists based on their type.

        Parameters:
        source_conn (dict): A dictionary containing the connection details to the source database.

        Returns:
        tuple: A tuple containing:
            - A dictionary with table/view names as keys and their types as values.
            - A list of view names.
            - A list of table names.
            - A list of DDL statements for tables.
            - A list of DDL statements for views.
        """
        connection = mysql.connector.connect(**self.source_conn)
        cursor = connection.cursor()
        cursor.execute(f"SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '{self.source_conn['database']}'")
        tables_and_views = {row[0]: row[1] for row in cursor}

        views, tabs, table_ddl, views_ddl = [], [], [], []

        for name, type in tqdm(tables_and_views.items(), desc="Generating DDL"):
            if type == "VIEW":
                views.append(name)
                res = self.get_create_table_ddl(connection, name, "VIEW")
                views_ddl.append(res)
            elif type == "BASE TABLE":
                tabs.append(name)
                res = self.get_create_table_ddl(connection, name, "TABLE")
                table_ddl.append(res)

        # Close the cursor
        cursor.close()
        connection.close()
        return tables_and_views, views, tabs, table_ddl, views_ddl
    
    def execute_create_statements(self, create_statements):
        """
        Execute DDL statements to create tables in the target database.

        This function connects to the target database and executes a list of DDL statements
        to create tables. It iterates through the provided list of statements and handles
        the case when a table already exists (MySQL error 1050).

        Parameters:
        create_statements (list): A list of DDL statements for creating tables.

        Returns:
        None
        """
        try:
            connection = mysql.connector.connect(**self.dest_conn)
            cursor = connection.cursor()

            for create_statement in tqdm(create_statements, desc="Create DDL statements from source DB"):
                try:
                    # Use regular expression to extract the table or view name
                    table_or_view_name_match = re.search(r"(CREATE TABLE|CREATE VIEW) `(.+?)`", create_statement)
                    if table_or_view_name_match:
                        object_type = table_or_view_name_match.group(1)
                        object_name = table_or_view_name_match.group(2)
                        print("Operation:", object_type)
                        print("Object Name:", object_name)
                    else:
                        object_name = None
                        object_type = None
                        print("Table or view name not found in the DDL statement.")
                    
                    cursor.execute(create_statement)
                    print(f"SUMMARY: Created successfully")
                except mysql.connector.Error as err:
                    if err.errno == 1050:  # Table already exists error
                        print(f"SUMMARY: Already exists")
                        pass

            connection.commit()
            print("All create statements executed successfully!")

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                
    def migrate_table(self, tabs):
        """
        Migrate data from the source to the destination database, handling duplicates using REPLACE INTO.

        This function connects to both the source and destination databases and migrates data from
        specified tables. It iterates through the tables, copying rows in batches and using REPLACE INTO
        to handle duplicates. After successful migration, the function closes the cursors.

        Parameters:
        tabs (list): A list of table names to migrate.

        Returns:
        None
        """
        try:
            source_connection = mysql.connector.connect(**self.source_conn)
            source_cursor = source_connection.cursor(buffered=True)
            
            dest_connection = mysql.connector.connect(**self.dest_conn)
            dest_cursor = dest_connection.cursor(buffered=True)

            # Batch size for insertion
            batch_size = 5000

            # Copy tables
            for table_name in tqdm(tabs, desc="Migrate data from source to destination"):
                try:
                    print(f"Working on table {table_name} ------")
                    source_cursor.execute(f"SELECT * FROM {table_name}")

                    total_rows = 0  # Counter for total rows copied

                    while True:
                        rows = source_cursor.fetchmany(batch_size)
                        if not rows:
                            break

                        # Modify insert query to use REPLACE INTO to handle duplicates
                        insert_query = f"REPLACE INTO {table_name} VALUES ({', '.join(['%s'] * len(rows[0]))})"
                        dest_cursor.executemany(insert_query, rows)
                        dest_connection.commit()

                        total_rows += len(rows)  # Increment total rows counter

                    print(f"Total rows copied for {table_name}: {total_rows}")

                except Exception as e:
                    print(e)
                    pass

            # Close cursors
            source_cursor.close()
            dest_cursor.close()
                                   
            # Close connections
            source_connection.close()
            dest_connection.close()

        except Exception as e:
            print(e)

if __name__ == "__main__":
    source_conn = {
        "host": "source_host",
        "user": "source_user",
        "password": "source_password",
        "database": "source_database"
    }
    
    dest_conn = {
        "host": "destination_host",
        "user": "destination_user",
        "password": "destination_password",
        "database": "destination_database"
    }

    cls = SimpleMigrateMysqlDB(source_conn, dest_conn)

    tables_and_views, views, tabs, table_ddl, views_ddl = cls.get_tables_and_views(source_conn)
    print(tables_and_views)

    cls.execute_create_statements(table_ddl)
    cls.execute_create_statements(views_ddl)

    cls.migrate_table(tabs)
