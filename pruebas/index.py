import os
import subprocess
import csv
import mysql.connector
from dotenv import load_dotenv
import threading
from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from tkinter import Tk, Label, Entry, Text, Button, StringVar, Scrollbar, Frame, END, messagebox

# FastAPI App
app = FastAPI()
server_thread = None
is_server_running = False
dotenv_path = ".env"


def start_server():
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

# Database Migration Function


def migrate_to_mysql(csv_path, table_name, mysql_config):
    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        with open(csv_path, 'r', encoding='latin-1') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)

            # Drop table if it exists and create a new one
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            create_table_query = f"CREATE TABLE {
                table_name} ({', '.join([f'{header} TEXT' for header in headers])})"
            cursor.execute(create_table_query)

            # Insert data
            insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({
                ', '.join(['%s'] * len(headers))})"
            cursor.executemany(insert_query, reader)

        conn.commit()
        cursor.close()
        conn.close()
        return f"Table {table_name} migrated successfully."

    except mysql.connector.Error as e:
        return f"Error migrating table {table_name}: {e}"

# GUI Setup


class VFPApp:
    def __init__(self, root):
        self.root = root
        self.root.title("VFP to CSV & MySQL Manager")

        # Environment variables
        self.path_databases_var = StringVar(value=os.getenv(
            'PATH_DATABASES', 'Z:/SoporteTi/sisclin//DATA'))
        self.output_folder_var = StringVar(
            value=os.getenv('OUTPUT_FOLDER', 'C:/sisclin/TablasVFP'))
        self.tables_var = StringVar(
            value='SC0011,SC0006,SC0003,SC0004,SC0033,SC0017,SC0002,SC0012,SC0022')

        # GUI Elements
        Label(root, text="Path Databases:").grid(row=0, column=0, sticky="w")
        self.path_databases_entry = Entry(
            root, textvariable=self.path_databases_var, width=50)
        self.path_databases_entry.grid(row=0, column=1)

        Label(root, text="Output Folder:").grid(row=1, column=0, sticky="w")
        self.output_folder_entry = Entry(
            root, textvariable=self.output_folder_var, width=50)
        self.output_folder_entry.grid(row=1, column=1)

        Label(root, text="Tables (comma-separated):").grid(row=2,
                                                           column=0, sticky="w")
        self.tables_entry = Entry(root, textvariable=self.tables_var, width=50)
        self.tables_entry.grid(row=2, column=1)

        # Console Output
        self.console_frame = Frame(root)
        self.console_frame.grid(row=4, column=0, columnspan=2, pady=5)
        self.console_text = Text(self.console_frame, height=15, width=80)
        self.console_text.pack(side="left", fill="both", expand=True)
        self.console_scrollbar = Scrollbar(
            self.console_frame, command=self.console_text.yview)
        self.console_scrollbar.pack(side="right", fill="y")
        self.console_text.config(yscrollcommand=self.console_scrollbar.set)

        # Buttons
        self.toggle_server_button = Button(
            root, text="Start Server", command=self.toggle_server)
        self.toggle_server_button.grid(row=3, column=0, pady=10)

        self.run_button = Button(
            root, text="Run Conversion & Migration", command=self.run_migration)
        self.run_button.grid(row=3, column=1, pady=10)

    def log(self, message):
        self.console_text.insert(END, message + "\n")
        self.console_text.see(END)

    def toggle_server(self):
        global is_server_running, server_thread
        if not is_server_running:
            self.log("Starting FastAPI server...")
            server_thread = threading.Thread(target=start_server, daemon=True)
            server_thread.start()
            is_server_running = True
            self.toggle_server_button.config(text="Stop Server")
        else:
            self.log("Stopping FastAPI server...")
            # Shutting down a FastAPI server programmatically requires implementing custom logic (not included here).
            is_server_running = False
            self.toggle_server_button.config(text="Start Server")

    def run_migration(self):
        self.log("Starting DBF to CSV and MySQL Migration...")
        path_databases = self.path_databases_var.get()
        output_folder = self.output_folder_var.get()
        tables = self.tables_var.get().split(',')

        os.makedirs(output_folder, exist_ok=True)

        for table in tables:
            dbf_path = os.path.join(path_databases, f"{table}.DBF")
            csv_path = os.path.join(output_folder, f"{table}.csv")

            self.log(f"Converting {dbf_path} to {csv_path}")
            try:
                subprocess.run(
                    ["ConvertirDBF.exe", dbf_path, csv_path], check=True)
                self.log(f"Successfully converted {dbf_path} to {csv_path}")

                # Migrate CSV to MySQL
                mysql_config = {
                    'host': os.getenv('MYSQL_HOST', 'localhost'),
                    'user': os.getenv('MYSQL_USER', 'root'),
                    'password': os.getenv('MYSQL_PASS', ''),
                    'database': os.getenv('MYSQL_DB', 'db_sisclin'),
                }
                print(mysql_config)
                migration_message = migrate_to_mysql(
                    csv_path, table, mysql_config)
                self.log(migration_message)

            except subprocess.CalledProcessError as e:
                self.log(f"Error converting {dbf_path}: {e}")
            except Exception as e:
                self.log(f"Error migrating {table}: {e}")

# FastAPI Endpoints


class ConversionRequest(BaseModel):
    path_databases: str
    output_folder: str
    tables: list[str]


@app.post("/convert")
def convert_tables(request: ConversionRequest):
    output_messages = []
    path_databases = request.path_databases
    output_folder = request.output_folder
    tables = request.tables

    os.makedirs(output_folder, exist_ok=True)

    for table in tables:
        dbf_path = os.path.join(path_databases, f"{table}.DBF")
        csv_path = os.path.join(output_folder, f"{table}.csv")

        try:
            subprocess.run(
                ["ConvertirDBF.exe", dbf_path, csv_path], check=True)
            output_messages.append(f"Successfully converted {
                                   dbf_path} to {csv_path}")

            # Migrate CSV to MySQL
            mysql_config = {
                'host': os.getenv('MYSQL_HOST', 'localhost'),
                'user': os.getenv('MYSQL_USER', 'root'),
                'password': os.getenv('MYSQL_PASS', ''),
                'database': os.getenv('MYSQL_DB', 'db_sisclin'),
            }
            migration_message = migrate_to_mysql(csv_path, table, mysql_config)
            output_messages.append(migration_message)

        except subprocess.CalledProcessError as e:
            output_messages.append(f"Error converting {dbf_path}: {e}")
        except Exception as e:
            output_messages.append(f"Error migrating {table}: {e}")

    return JSONResponse(content={"messages": output_messages})


@app.get("/run_migration")
def run_migration_endpoint():
    output_messages = []
    path_databases = os.getenv('PATH_DATABASES', 'Z:/SoporteTi/sisclin//DATA')
    output_folder = os.getenv('OUTPUT_FOLDER', 'C:/sisclin/TablasVFP')
    tables = os.getenv(
        'TABLES', 'SC0011,SC0006,SC0003,SC0004,SC0033,SC0017,SC0002,SC0012,SC0022').split(',')

    os.makedirs(output_folder, exist_ok=True)

    for table in tables:
        dbf_path = os.path.join(path_databases, f"{table}.DBF")
        csv_path = os.path.join(output_folder, f"{table}.csv")

        try:
            subprocess.run(
                ["ConvertirDBF.exe", dbf_path, csv_path], check=True)
            output_messages.append(f"Successfully converted {
                                   dbf_path} to {csv_path}")

            # Migrate CSV to MySQL
            mysql_config = {
                'host': os.getenv('MYSQL_HOST', 'localhost'),
                'user': os.getenv('MYSQL_USER', 'root'),
                'password': os.getenv('MYSQL_PASS', ''),
                'database': os.getenv('MYSQL_DB', 'db_sisclin'),
            }
            migration_message = migrate_to_mysql(csv_path, table, mysql_config)
            output_messages.append(migration_message)

        except subprocess.CalledProcessError as e:
            output_messages.append(f"Error converting {dbf_path}: {e}")
        except Exception as e:
            output_messages.append(f"Error migrating {table}: {e}")

    return JSONResponse(content={"messages": output_messages})


if __name__ == "__main__":
    # Start the Tkinter GUI
    load_dotenv(dotenv_path)
    root = Tk()
    app = VFPApp(root)
    root.mainloop()
