import os
import schedule
import threading
from tkinter import Tk, Label, Entry, Text, Button, StringVar, Scrollbar, Frame, END, messagebox
from dotenv import load_dotenv
import uvicorn
import time
import sys

from convert_dbf_csv import convert_dbf_to_csv
from import_csv_mysql import migrate_to_mysql


class TextRedirector:
    def __init__(self, widget, tag="stdout"):
        self.widget = widget
        self.tag = tag

    def write(self, message):
        self.widget.insert(END, message + "\n", (self.tag,))
        self.widget.see(END)

    def flush(self):
        pass

    def isatty(self):
        return False


def start_server():
    uvicorn.run('api:app', host="0.0.0.0", port=8080)


class VFPApp:
    def __init__(self, root):
        self.root = root
        self.root.title("VFP to CSV & MySQL Manager")
        self._load_environment_variables()

        # GUI Elements
        self._initialize_gui()

        # Redirect stdout and stderr to the console_text widget
        sys.stdout = TextRedirector(self.console_text, "stdout")
        sys.stderr = TextRedirector(self.console_text, "stderr")

        # Server state
        self.server_thread = None
        self.is_server_running = False

    def _load_environment_variables(self):
        """Carga las variables de entorno."""
        self.path_databases_var = StringVar(value=os.getenv(
            'PATH_DATABASES', 'Z:/SoporteTi/sisclin/DATA'))
        self.output_folder_var = StringVar(
            value=os.getenv('OUTPUT_FOLDER', 'C:/sisclin/TablasVFP'))
        self.tables_var = StringVar(
            value='SC0011,SC0006,SC0003,SC0004,SC0033,SC0017,SC0002,SC0012,SC0022')

    def _initialize_gui(self):
        """Configura los elementos de la interfaz gráfica."""
        Label(self.root, text="Path Databases:").grid(
            row=0, column=0, sticky="w")
        self.path_databases_entry = Entry(
            self.root, textvariable=self.path_databases_var, width=50)
        self.path_databases_entry.grid(row=0, column=1)

        Label(self.root, text="Output Folder:").grid(
            row=1, column=0, sticky="w")
        self.output_folder_entry = Entry(
            self.root, textvariable=self.output_folder_var, width=50)
        self.output_folder_entry.grid(row=1, column=1)

        Label(self.root, text="Tables (comma-separated):").grid(row=2,
                                                                column=0, sticky="w")
        self.tables_entry = Entry(
            self.root, textvariable=self.tables_var, width=50)
        self.tables_entry.grid(row=2, column=1)

        # Console Output
        self.console_frame = Frame(self.root)
        self.console_frame.grid(row=4, column=0, columnspan=2, pady=5)
        self.console_text = Text(self.console_frame, height=15, width=80)
        self.console_text.pack(side="left", fill="both", expand=True)
        self.console_scrollbar = Scrollbar(
            self.console_frame, command=self.console_text.yview)
        self.console_scrollbar.pack(side="right", fill="y")
        self.console_text.config(yscrollcommand=self.console_scrollbar.set)

        # Buttons Frame
        self.buttons_frame = Frame(self.root)
        self.buttons_frame.grid(row=3, column=0, columnspan=2, pady=10)

        # Buttons
        self.toggle_server_button = Button(
            self.buttons_frame, text="Start Server", command=self.toggle_server)
        self.toggle_server_button.grid(row=0, column=0, padx=5)

        self.run_button = Button(
            self.buttons_frame, text="Run Conversion & Migration", command=self.run_migration)
        self.run_button.grid(row=0, column=1, padx=5)

        self.upload_mysql_button = Button(
            self.buttons_frame, text="Subir MySQL", command=self.upload_to_mysql)
        self.upload_mysql_button.grid(row=0, column=2, padx=5)

    def _schedule_task(self):
        """Ejecuta las tareas programadas en segundo plano."""
        while True:
            schedule.run_pending()
            time.sleep(1)

    def schedule_migration(self):
        """Programa la migración diaria a las 00:00."""
        schedule.every().day.at("00:00").do(self.run_migration)
        # schedule.every(10).seconds.do(self.run_migration)
        threading.Thread(target=self._schedule_task, daemon=True).start()
        messagebox.showinfo(
            "Programación", "Migración programada diariamente a las 00:00.")

    def log(self, message):
        """Escribe un mensaje en la consola de la aplicación."""
        self.console_text.insert(END, message + "\n")
        self.console_text.see(END)

    def toggle_server(self):
        """Inicia o detiene el servidor FastAPI."""
        if not self.is_server_running:
            self.log("Starting FastAPI server...")
            self.server_thread = threading.Thread(
                target=start_server, daemon=True)
            self.server_thread.start()
            self.is_server_running = True
            self.toggle_server_button.config(text="Stop Server")
        else:
            self.log("Stopping FastAPI server...")
            self.is_server_running = False
            self.toggle_server_button.config(text="Start Server")

    def _convert_tables(self, path_databases, output_folder, tables):
        """Convierte las tablas DBF a CSV."""
        os.makedirs(output_folder, exist_ok=True)
        for table in tables:
            dbf_path = os.path.join(path_databases, f"{table}.DBF")
            csv_path = os.path.join(output_folder, f"{table}.csv")
            try:
                convert_dbf_to_csv(dbf_path, csv_path)
                self.log(f"Successfully converted {table}.DBF to {table}.csv")
            except Exception as e:
                self.log(f"Error converting {table}: {e}")

    def _migrate_tables_to_mysql(self, output_folder, tables):
        """Migra las tablas CSV a MySQL."""
        mysql_config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASS', ''),
            'database': os.getenv('MYSQL_DB', 'db_sisclin'),
            'allow_local_infile': True,
            'charset': 'utf8mb4'
        }

        for table in tables:
            csv_path = os.path.join(output_folder, f"{table}.csv")
            try:
                migration_message = migrate_to_mysql(
                    csv_path, table, mysql_config)
                self.log(migration_message)
            except Exception as e:
                self.log(f"Error migrating {table}: {e}")

    def upload_to_mysql(self):
        start_time = time.time()
        self.log("Starting DBF to CSV and MySQL Migration...")
        output_folder = self.output_folder_var.get()
        tables = self.tables_var.get().split(',')
        self._migrate_tables_to_mysql(output_folder, tables)

        elapsed_time = time.time() - start_time
        self.log(f"Migration completed in {elapsed_time:.2f} seconds.")

    def run_migration(self):
        """Ejecuta la migración completa: conversión y carga a MySQL."""
        start_time = time.time()
        self.log("Starting DBF to CSV and MySQL Migration...")

        path_databases = self.path_databases_var.get()
        output_folder = self.output_folder_var.get()
        tables = self.tables_var.get().split(',')

        self._convert_tables(path_databases, output_folder, tables)
        self._migrate_tables_to_mysql(output_folder, tables)

        elapsed_time = time.time() - start_time
        self.log(f"Migration completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    load_dotenv(".env")
    print("Variables de entorno cargadas:")
    with open(".env") as f:
        for line in f:
            if line.strip() and not line.startswith("#"):
                key = line.split('=', 1)[0]
                value = os.getenv(key)
                print(f"{key}: {value}")
    root = Tk()
    app = VFPApp(root)
    app.schedule_migration()
    root.mainloop()
