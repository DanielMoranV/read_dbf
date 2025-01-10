import mysql.connector
import csv
import time


def migrate_to_mysql(csv_path, table_name, mysql_config):
    start_time = time.time()
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        with open(csv_path, 'r', encoding='latin-1') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)

            # Crear la tabla
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            create_table_query = f"CREATE TABLE {
                table_name} ({', '.join([f'{header} TEXT' for header in headers])})"
            cursor.execute(create_table_query)

            # Desactivar índices y claves foráneas
            cursor.execute("SET foreign_key_checks = 0")
            cursor.execute(f"ALTER TABLE {table_name} DISABLE KEYS")

            # Insertar datos en lotes
            insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({
                ', '.join(['%s'] * len(headers))})"
            batch_size = 1000
            batch = []

            for row in reader:
                if len(row) < len(headers):
                    row.extend([None] * (len(headers) - len(row)))
                batch.append([None if value == '' else value for value in row])

                if len(batch) == batch_size:
                    cursor.executemany(insert_query, batch)
                    batch = []

            if batch:
                cursor.executemany(insert_query, batch)

            # Reactivar índices y claves foráneas
            cursor.execute(f"ALTER TABLE {table_name} ENABLE KEYS")
            cursor.execute("SET foreign_key_checks = 1")

            cursor.close()
            conn.close()

            end_time = time.time()
            duration = end_time - start_time
            print(f"Tiempo total de migración: {duration:.2f} segundos")
            return f"Table {table_name} migrated successfully."
    except mysql.connector.Error as e:
        raise Exception(f"Error migrating table {table_name}: {e}")
