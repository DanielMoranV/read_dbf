from dbfread import DBF
import csv
import os
import mysql.connector
import time

pathDataBases = 'C:/sisclin/DATA'
outputFolder = 'C:/sisclin/TablasVFP'

# Asegurarse de que la carpeta de salida exista
os.makedirs(outputFolder, exist_ok=True)


def dbf_to_csv(dbf_path, csv_path):
    dbf_table = DBF(dbf_path, load=True, encoding='latin1', ignorecase=True)
    dbf_record_count = len(dbf_table)  # Contar registros en DBF
    print(f"Registros en {dbf_path}: {dbf_record_count}")

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(dbf_table.field_names)  # Escribir encabezados
        for record in dbf_table:
            writer.writerow(list(record.values()))

    # Contar registros en el CSV
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        csv_record_count = sum(1 for _ in csv.reader(
            csvfile)) - 1  # -1 para excluir encabezados
    print(f"Registros en {csv_path}: {csv_record_count}")

    return dbf_record_count, csv_record_count


def create_table_and_insert_data(cursor, table_name, csv_path):
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

        # Crear la tabla
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_table_query = f"CREATE TABLE {
            table_name} ({', '.join([f'{header} TEXT' for header in headers])})"
        cursor.execute(create_table_query)

        # Insertar datos
        insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({
            ', '.join(['%s'] * len(headers))})"
        for row in reader:
            cursor.execute(insert_query, row)

        # Contar registros en la base de datos
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        db_record_count = cursor.fetchone()[0]
        print(f"Registros en la tabla MySQL {table_name}: {db_record_count}")

        return db_record_count


# Conectar a la base de datos MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='db_sisclin'
)
cursor = conn.cursor()

# Lista de tablas a migrar
tables = ['SC0011', 'SC0006', 'SC0003', 'SC0004',
          'SC0033', 'SC0017', 'SC0002', 'SC0012', 'SC0022']

# Medir el tiempo total
start_time_total = time.time()

for table in tables:
    start_time = time.time()
    csv_path = f'{outputFolder}/{table}.csv'

    dbf_count, csv_count = dbf_to_csv(f'{pathDataBases}/{table}.DBF', csv_path)
    db_count = create_table_and_insert_data(cursor, table, csv_path)

    # Comparar conteos
    print(f"Comparación de registros para {table}:")
    print(f"  Registros DBF: {dbf_count}")
    print(f"  Registros CSV: {csv_count}")
    print(f"  Registros MySQL: {db_count}")

    end_time = time.time()
    print(f"Tiempo para procesar {table}: {
          end_time - start_time:.2f} segundos")

# Confirmar los cambios y cerrar la conexión
conn.commit()
cursor.close()
conn.close()

end_time_total = time.time()
print(f"Tiempo total: {end_time_total - start_time_total:.2f} segundos")
