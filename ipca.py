import sys
import pandas as pd
import requests
import boto3
import csv
from datetime import datetime
from io import StringIO
from requests.adapters import HTTPAdapter, Retry
from awsglue.utils import getResolvedOptions

BASE_API_ADDRESS = "https://apisidra.ibge.gov.br/values/"
CSV_HEADER = {
    "Número-índice (base: dezembro de 1993 = 100)": "NUMERO INDICE(DEZ 93 = 100)",
    "Variação mensal": "NO MES",
    "Variação acumulada em 3 meses": "3 MESES",
    "Variação acumulada em 6 meses": "6 MESES",
    "Variação acumulada no ano": "NO ANO",
    "Variação acumulada em 12 meses": "12 MESES"
}
MONTHS = {
    "01": "JAN",
    "02": "FEV",
    "03": "MAR",
    "04": "ABR",
    "05": "MAI",
    "06": "JUN",
    "07": "JUL",
    "08": "AGO",
    "09": "SET",
    "10": "OUT",
    "11": "NOV",
    "12": "DEZ"
}

URLS = {
    "INPC": f"{BASE_API_ADDRESS}t/1736/n1/all/h/n/P/all?formato=json",
    "IPCA": f"{BASE_API_ADDRESS}t/1737/n1/all/h/n/P/all?formato=json",
    "IPCA15": f"{BASE_API_ADDRESS}t/3065/n1/all/h/n/P/all?formato=json"
}

def make_csv(data_to_save:list):
    buffer = StringIO()
    writer = csv.writer(buffer, delimiter=';')
    writer.writerows(data_to_save)

    return buffer

def make_header(column:str):
    reference = [column_splited.strip() for column_splited in column.split(" - ")][-1]

    if reference in CSV_HEADER:
        column = CSV_HEADER[reference]
    
    return column

def mount_df(response_data:dict):
    data_df = []

    df = pd.DataFrame.from_dict(response_data)

    pivot_df = df.pivot_table(index=["D2C"], columns="D3N", values="V", aggfunc="first").reset_index()

    new_columns = {
        column: make_header(column)
        for column in pivot_df.columns
    }

    pivot_df.rename(columns=new_columns, inplace=True)

    pivot_df.replace({'...': "0.0"}, inplace=True)

    pivot_df["MES"] = pivot_df["D2C"].apply(lambda col: MONTHS[col[-2:]])
    pivot_df["MES_NO"] = pivot_df["D2C"].apply(lambda col: int(col[-2:]))
    pivot_df["ANO"] = pivot_df["D2C"].apply(lambda col: int(col[:-2]))
    pivot_df["NUMERO INDICE(DEZ 93 = 100)"] = pivot_df["NUMERO INDICE(DEZ 93 = 100)"].apply(lambda col: col[0:col.find(".")+3])

    pivot_df.sort_values(by=["ANO", "MES_NO"], inplace=True)

    pivot_df = pivot_df[pivot_df["ANO"] >= 1994]

    final_df = pivot_df[[
        "ANO", 
        "MES", 
        "NUMERO INDICE(DEZ 93 = 100)", 
        "NO MES", 
        "3 MESES", 
        "6 MESES", 
        "NO ANO", 
        "12 MESES"
    ]]

    df_dict = final_df.to_dict("list")

    data_df.append([k for k in df_dict.keys()])

    for ano, mes, no_indice, no_mes, tres_mes, seis_mes, no_ano, doze_mes in zip(
        df_dict["ANO"],
        df_dict["MES"],
        df_dict["NUMERO INDICE(DEZ 93 = 100)"],
        df_dict["NO MES"],
        df_dict["3 MESES"],
        df_dict["6 MESES"],
        df_dict["NO ANO"],
        df_dict["12 MESES"],
    ):
        data_df.append([ano, mes, no_indice, no_mes, tres_mes, seis_mes, no_ano, doze_mes])

    return data_df

def generate_raw_name(type_name:str, path:str):
    execution_time = datetime.now().strftime('%Y%m%d%H%M%S')
    return path + "/IPCA/" + type_name + "_Ref" + execution_time[0:6] + "_" + execution_time + ".csv"

def save_file_in_bucket(csv_file:StringIO, file_path_name:str, buckets:list):
    client = boto3.client("s3")

    for bucket in buckets:
        client.put_object(Body=csv_file.getvalue(), Bucket=bucket, Key=file_path_name)
        print(f'[*] Saved data in bucket: {bucket}/{file_path_name}')

def save_bucket(df:pd.DataFrame, type_name:str, raw_buckets:list, work_area_buckets:list, path:str):
    csv_file = make_csv(df)
    file_path_name = None

    if raw_buckets:
        file_path_name = generate_raw_name(type_name, path)
        save_file_in_bucket(csv_file, file_path_name, raw_buckets)

    if work_area_buckets:
        file_path_name = f"{path}/{type_name}.csv"
        save_file_in_bucket(csv_file, file_path_name, work_area_buckets)

def mount_retry():
    s = requests.Session()
    s.mount(BASE_API_ADDRESS, HTTPAdapter(max_retries=5))
    return s

def main():
    args = getResolvedOptions(sys.argv, ['raw_buckets', 'work_area_buckets', 'path', 'debug'])

    raw_buckets = args['raw_buckets']
    work_area_buckets = args['work_area_buckets']
    path = args['path'] or "ECONOMIC"

    print(f'[*] Start process')

    raw_buckets = raw_buckets.replace(" ", "").split(",") or ["br-bliss-data-providers", "br-bliss-data-stage"]
    work_area_buckets = work_area_buckets.replace(" ", "").split(",") or ["br-bliss-work-area"]

    s = mount_retry()

    for type_name, url in URLS.items():
        response_data = s.get(url)

        if response_data.status_code == 200:
            df = mount_df(response_data.json())

            save_bucket(df, type_name, raw_buckets, work_area_buckets, path)

if __name__ == "__main__":
    main()