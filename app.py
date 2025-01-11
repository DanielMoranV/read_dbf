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


server_thread = None
is_server_running = False


class TextRedirector:
    def __init__(self, widget, tag="stdout"):
        self.widget = widget
        self.tag = tag

    def write(self, message):
        self.widget.insert(END, message, (self.tag,))
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

        # Redirect stdout and stderr to the console_text widget
        sys.stdout = TextRedirector(self.console_text, "stdout")
        sys.stderr = TextRedirector(self.console_text, "stderr")

        # Buttons
        self.toggle_server_button = Button(
            root, text="Start Server", command=self.toggle_server)
        self.toggle_server_button.grid(row=3, column=0, pady=10)

        self.run_button = Button(
            root, text="Run Conversion & Migration", command=self.run_migration)
        self.run_button.grid(row=3, column=1, pady=10)

    def schedule_migration(self):
        """Programar la migración diaria a las 00:00."""
        schedule.every().day.at("00:00").do(self.run_migration)

        def run_schedule():
            while True:
                schedule.run_pending()
                time.sleep(1)

        threading.Thread(target=run_schedule, daemon=True).start()
        messagebox.showinfo(
            "Programación", "Migración programada diariamente a las 00:00.")

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
            is_server_running = False
            self.toggle_server_button.config(text="Start Server")

    def run_migration(self):
        start_time = time.time()
        self.log("Starting DBF to CSV and MySQL Migration...")
        path_databases = self.path_databases_var.get()
        output_folder = self.output_folder_var.get()
        tables = self.tables_var.get().split(',')

        os.makedirs(output_folder, exist_ok=True)

        for table in tables:
            dbf_path = os.path.join(path_databases, f"{table}.DBF")
            csv_path = os.path.join(output_folder, f"{table}.csv")

            try:
                convert_dbf_to_csv(dbf_path, csv_path)
                self.log(f"Successfully converted {dbf_path} to {csv_path}")

                mysql_config = {
                    'host': os.getenv('MYSQL_HOST', 'localhost'),
                    'user': os.getenv('MYSQL_USER', 'root'),
                    'password': os.getenv('MYSQL_PASS', ''),
                    'database': os.getenv('MYSQL_DB', 'db_sisclin'),
                    'allow_local_infile': True,
                    'charset': 'utf8mb4'
                }
                migration_message = migrate_to_mysql(
                    csv_path, table, mysql_config)
                self.log(migration_message)
            except Exception as e:
                self.log(f"Error: {e}")
        end_time = time.time()
        elapsed_time = end_time - start_time
        self.log(f"Tiempo de ejecución de convert_dbf_to_csv: {
                 elapsed_time:.2f} segundos")


if __name__ == "__main__":
    load_dotenv(".env")

    # Imprimir variables de entorno para verificar
    print("PATH_DATABASES:", os.getenv('PATH_DATABASES'))
    print("OUTPUT_FOLDER:", os.getenv('OUTPUT_FOLDER'))
    print("VFP_EXE_PATH:", os.getenv('VFP_EXE_PATH'))
    print("MYSQL_HOST:", os.getenv('MYSQL_HOST'))
    print("MYSQL_USER:", os.getenv('MYSQL_USER'))
    print("MYSQL_PASS:", os.getenv('MYSQL_PASS'))
    print("MYSQL_DB:", os.getenv('MYSQL_DB'))

    root = Tk()
    app = VFPApp(root)
    app.schedule_migration()
    root.mainloop()
