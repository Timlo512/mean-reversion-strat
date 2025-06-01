import psycopg2
import pandas as pd
from typing import Union, List, Tuple

# Backlog PostgresRepo to distinguish different database connections

_dtype_dict = {
    "float64": "float",
    "int64": "int",
    "object": "varchar",
    "datetime64[ns]": "timestamp"
}

class PostgresConnector:
    def __init__(self, dbname, user, password, host='localhost', port=5432, **kwargs):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()

    def __del__(self):
        self.close()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def _get_current_user(self):
        self.cursor.execute("SELECT current_user;")
        return self.cursor.fetchone()[0]
    
    def create_table(self, table_name, columns, with_id=True, unique_cols=None, enforce_cols={}):
        try:
            ddl = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            if with_id:
                ddl += "id SERIAL PRIMARY KEY, "
            for column, data_type in columns.items():
                data_type = enforce_cols.get(column, data_type)
                ddl += f"{column} {data_type}, "

            if unique_cols:
                unique_cols = ', '.join(unique_cols)
                ddl += f"UNIQUE ({unique_cols}), "
            ddl = ddl.rstrip(", ") + ");"
            print(ddl)

            self.cursor.execute(ddl)
            self.connection.commit()
        except Exception as e:
            print(f"Error creating table: {e}")
            self.connection.rollback()

    # def create_table_by_df(self, df, table_name):
    def create_table_by_df(self, df, table_name, with_id = True, unique_cols = None, enforce_cols = {}):
        # Parse the col names
        col_dict = {key: _dtype_dict[str(val)] for key, val in df.dtypes.to_dict().items()}

        # Create the table statement
        self.create_table(table_name, col_dict, with_id, unique_cols, enforce_cols)
        return True
        
    def execute_query(self, query: str) -> List[Tuple]:
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def read_sql_by_pd(self, query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, self.connection)

    def write_data(self, data: Union[list[str], pd.DataFrame, dict], table_name, if_exists='append', conflict_columns = None, **kwargs) -> bool:
        """
        Write data to the database.
        Args:
            df (DataFrame): The DataFrame to write.
            table_name (str): The name of the table to write to.
            if_exists (str): What to do if the table already exists. Allowed values
                ['append', 'upsert']. Default is 'append'.
        """
        if isinstance(data, pd.DataFrame):
            df = data

        elif isinstance(data, dict):
            print("data is a dict, converting to DataFrame")
            df = pd.DataFrame([data])
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            raise ValueError("Not supported data type")

        auto_create = kwargs.get("auto_create", True)
    
        try:
            # Create table if it doesn't exist
            if auto_create:
                enforce_cols = kwargs.get("enforce_cols", {})
                check = self.create_table_by_df(df, table_name, unique_cols=conflict_columns, enforce_cols=enforce_cols)

            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.to_numpy()]

            # Create insert query
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))

            if if_exists == 'upsert':
                if not conflict_columns:
                    raise ValueError("conflict_columns must be specified for upsert operations.")

                # Build the ON CONFLICT clause
                conflict_clause = ', '.join(conflict_columns)
                update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns])

                insert_query = f"""
                    INSERT INTO {table_name} ({columns}) 
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_clause})
                    DO UPDATE SET {update_clause};
                """
            else:  # Default to 'append'
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # Execute insert query
            self.cursor.executemany(insert_query, data)
            self.connection.commit()
            return True
        except Exception as e:
            print(f"Error writing data: {e}")
            self.connection.rollback()
            return False

    def delete_data(self, table_name, condition):
        """
        Delete data from the database.
        Args:
            table_name (str): The name of the table to delete from.
            condition (str): The condition for deletion.
        """
        try:
            delete_query = f"DELETE FROM {table_name} WHERE {condition};"
            self.cursor.execute(delete_query)
            self.connection.commit()
        except Exception as e:
            print(f"Error deleting data: {e}")
            self.connection.rollback()

    def drop_table(self, table_name):
        """
        Drop a table from the database.
        Args:
            table_name (str): The name of the table to drop.
        """
        try:
            drop_query = f"DROP TABLE IF EXISTS {table_name};"
            self.cursor.execute(drop_query)
            self.connection.commit()
        except Exception as e:
            print(f"Error dropping table: {e}")
            self.connection.rollback()