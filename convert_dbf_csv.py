import subprocess
import csv
import os


def convert_dbf_to_csv(dbf_path, csv_path):
    try:
        subprocess.run(["ConvertirDBF.exe", dbf_path, csv_path], check=True)
        process_csv(csv_path)
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error converting {dbf_path}: {e}")


def process_csv(csv_path):
    temp_csv_path = csv_path + ".tmp"
    with open(csv_path, 'r', encoding='latin-1') as infile, open(temp_csv_path, 'w', newline='', encoding='latin-1') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        headers = next(reader)
        writer.writerow(headers)
        for row in reader:
            if any(row):
                writer.writerow(row)
    os.replace(temp_csv_path, csv_path)
