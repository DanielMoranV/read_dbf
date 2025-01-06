import subprocess
import os
import csv
import mysql.connector
import time

pathDataBases = 'C:/sisclin/DATA'
outputFolder = 'C:/sisclin/TablasVFP'

# Asegurarse de que la carpeta de salida exista
os.makedirs(outputFolder, exist_ok=True)


def dbf_to_csv_vfp(exe_path, dbf_path, csv_path):
    """
    Convierte una tabla DBF a CSV usando el ejecutable de Visual FoxPro.
    """
    try:
        # Llamar al ejecutable con los argumentos necesarios
        subprocess.run([exe_path, dbf_path, csv_path], check=True)
        print(f"Conversión completada: {dbf_path} -> {csv_path}")

        # Contar registros en el archivo CSV
        with open(csv_path, 'r', encoding='latin-1') as csvfile:
            csv_record_count = sum(1 for _ in csv.reader(
                csvfile)) - 1  # Excluir encabezados
        print(f"Registros en {csv_path}: {csv_record_count}")
        return csv_record_count
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar el archivo .exe: {e}")
        return 0


def create_table_and_insert_data(cursor, table_name, csv_path):
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
            batch.append(row)

            if len(batch) == batch_size:
                cursor.executemany(insert_query, batch)
                batch = []

        if batch:
            cursor.executemany(insert_query, batch)

        # Reactivar índices y claves foráneas
        cursor.execute(f"ALTER TABLE {table_name} ENABLE KEYS")
        cursor.execute("SET foreign_key_checks = 1")

        # Contar registros en la base de datos
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        db_record_count = cursor.fetchone()[0]
        print(f"Registros en la tabla MySQL {table_name}: {db_record_count}")

        return db_record_count


# Ruta al ejecutable de Visual FoxPro
vfp_exe_path = "ConvertirDBF.exe"

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
    dbf_path = f'{pathDataBases}/{table}.DBF'
    csv_path = f'{outputFolder}/{table}.csv'

    # Convertir DBF a CSV usando el ejecutable de VFP
    csv_count = dbf_to_csv_vfp(vfp_exe_path, dbf_path, csv_path)

    # Iniciar transacción
    conn.start_transaction()

    # Insertar datos en la base de datos
    db_count = create_table_and_insert_data(cursor, table, csv_path)

    # Confirmar transacción
    conn.commit()

    # Comparar conteos
    print(f"Comparación de registros para {table}:")
    print(f"  Registros CSV: {csv_count}")
    print(f"  Registros MySQL: {db_count}")

    end_time = time.time()
    print(f"Tiempo para procesar {table}: {
          end_time - start_time:.2f} segundos")

# Confirmar los cambios y cerrar la conexión
cursor.close()
conn.close()

end_time_total = time.time()
print(f"Tiempo total: {end_time_total - start_time_total:.2f} segundos")
