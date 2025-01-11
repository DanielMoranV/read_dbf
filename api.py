# api.py
from fastapi import FastAPI, HTTPException
import os
from functions import run_migration_logic
from dotenv import load_dotenv
from pydantic import BaseModel
import subprocess
import json

# Load environment variables
load_dotenv(override=True)

data_source = os.getenv('PATH_DATABASES', 'Z:/SoporteTi/sisclin//DATA')
app = FastAPI()


class QueryModel(BaseModel):
    query: str


@app.get("/")
async def root():
    return {"message": "FastAPI está funcionando"}


@app.get("/run-migration")
async def run_migration():
    try:
        # Lee valores predeterminados desde variables de entorno o define directamente
        path_databases = os.getenv(
            'PATH_DATABASES', 'Z:/SoporteTi/sisclin//DATA')
        output_folder = os.getenv('OUTPUT_FOLDER', 'C:/sisclin/TablasVFP')
        tables = [
            "SC0011", "SC0006", "SC0003", "SC0004", "SC0033",
            "SC0017", "SC0002", "SC0012"
        ]

        # Ejecuta la lógica
        messages = run_migration_logic(path_databases, output_folder, tables)

        return {
            "status": "success",
            "messages": messages
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/execute_query")
def execute_query(data: QueryModel):
    try:
        # Validar que la consulta sea del tipo SELECT
        if not data.query.strip().lower().startswith("select"):
            raise HTTPException(
                status_code=400, detail="Solo se permiten consultas del tipo SELECT.")

        # Define el path del ejecutable dbf_query.exe
        # Asegúrate de poner la ruta correcta
        executable_path = "dbf_query.exe"
        query = data.query  # Toma la consulta del request

        # Usa subprocess para ejecutar el .exe y pasar la consulta como parámetro
        result = subprocess.run(
            [executable_path, data_source, query], capture_output=True, text=True)

        # Verifica si hubo error en la ejecución
        if result.returncode != 0:
            raise HTTPException(
                status_code=500, detail=f"Error en dbf_query.exe: {result.stderr}")

        # Devuelve el resultado procesado
        try:
            output_json = json.loads(result.stdout)  # Si la salida es JSON
            return output_json
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=500, detail="Error al procesar los resultados JSON.")
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error en execute_query: {str(e)}")
