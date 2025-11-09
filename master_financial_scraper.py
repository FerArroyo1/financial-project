#
# ----- 1. BLOQUE COMPLETO DE IMPORTS -----
#
import pandas as pd
import numpy as np
import yfinance as yf
import requests
import json
import calendar
import re
import flask
from flask import Flask, request, jsonify
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import timedelta

#
# ----- 2. VARIABLES GLOBALES Y MAPAS -----
#
# ¡IMPORTANTE! Reemplaza esto con tu email
headers = {"User-Agent": "tu-email-real@gmail.com"}

# TU MAPA COMPLETO DE ESTADOS FINANCIEROS
statement_keys_map = {
    "balance_sheet": [
        "balance sheet", "balance sheets", "statement of financial position",
        "consolidated balance sheets", "consolidated balance sheet",
        "consolidated financial position", "consolidated balance sheets - southern",
        "consolidated statements of financial position", "consolidated statement of financial position",
        "consolidated statements of financial condition", "combined and consolidated balance sheet",
        "condensed consolidated balance sheets", "consolidated balance sheets, as of december 31",
        "dow consolidated balance sheets", "consolidated balance sheets (unaudited)",
        "consolidated and combined statement of financial position",
        "consolidated condensed balance sheets", "consolidated balance sheets statement"
    ],
    "income_statement": [
        "income statement", "income statements", "statement of earnings (loss)",
        "consolidated statements of operations", "consolidated statement of operations",
        "consolidated statements of earnings", "consolidated statement of earnings",
        "consolidated statements of income", "consolidated statement of income",
        "consolidated income statements", "consolidated income statement",
        "condensed consolidated statements of earnings", "condensed consolidated statements of operations",
        "consolidated results of operations", "consolidated statements of income (loss)",
        "consolidated statements of income - southern",
        "consolidated statements of operations and comprehensive income",
        "consolidated and combined statement of income (loss)",
        "consolidated and combined statement of income (loss) (unaudited)",
        "CONSOLIDATED STATEMENTS OF INCOME", "combined statement of income (loss) (unaudited)",
        "consolidated condensed statements of income",
        "consolidated statements of comprehensive income",
        "consolidated statements of comprehensive loss",
         'consolidated statements of operations and comprehensive income (loss)',
        "consolidated statements of operations statement",
        "consolidated statements of comprehensive income (loss)"
    ],
    "cash_flow_statement": [
        "cash flows statement", "cash flows statements", "statement of cash flows",
        "statements of consolidated cash flows", "consolidated statements of cash flows",
        "consolidated statement of cash flows", "consolidated statement of cash flow",
        "consolidated cash flows statements", "consolidated cash flow statements",
        "condensed consolidated statements of cash flows",
        "consolidated statements of cash flows (unaudited)",
        "consolidated statements of cash flows - southern",
        "consolidated and combined statement of cash flows",
        "consolidated and combined statement of cash flows (unaudited)",
        "combined statement of cash flows (unaudited)",
        "consolidated condensed statements of cash flows",
        "consolidated statements of cash flows statement"
    ],
}


#
# ----- 3. TODAS TUS FUNCIONES AUXILIARES (COMPLETAS) -----
#

def cik_matching_ticker(ticker, headers=headers):
    ticker = ticker.upper().replace(".", "-")
    ticker_json = requests.get(
        "https://www.sec.gov/files/company_tickers.json", headers=headers
    ).json()

    for company in ticker_json.values():
        if company["ticker"] == ticker:
            cik = str(company["cik_str"]).zfill(10)
            return cik
    raise ValueError(f"Ticker {ticker} not found in SEC database")

def _get_file_name(report):
    html_file_name_tag = report.find("HtmlFileName")
    xml_file_name_tag = report.find("XmlFileName")
    if html_file_name_tag:
        return html_file_name_tag.text
    elif xml_file_name_tag:
        return xml_file_name_tag.text
    else:
        return ""

def _is_statement_file(short_name_tag, long_name_tag, file_name):
    return (
        short_name_tag is not None
        and long_name_tag is not None
        and file_name
        and "Statement" in long_name_tag.text
    )

def get_statement_file_names_in_filing_summary(ticker, accession_number, headers=None):
    try:
        cik = cik_matching_ticker(ticker, headers) 
        session = requests.Session()
        base_link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"
        filing_summary_link = f"{base_link}/FilingSummary.xml"
        filing_summary_response = session.get(
            filing_summary_link, headers=headers
        ).content.decode("utf-8")

        filing_summary_soup = BeautifulSoup(filing_summary_response, "lxml-xml")
        statement_file_names_dict = {}
        for report in filing_summary_soup.find_all("Report"):
            file_name = _get_file_name(report)
            short_name, long_name = report.find("ShortName"), report.find("LongName")
            if _is_statement_file(short_name, long_name, file_name):
                statement_file_names_dict[short_name.text.lower()] = file_name
        return statement_file_names_dict
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return {}
    except ValueError as e: 
        print(f"Error getting CIK for {ticker}: {e}")
        return {}


def _get_disclosure_name(disclosure):
    html_file_name_tag = disclosure.find("HtmlFileName")
    xml_file_name_tag = disclosure.find("XmlFileName")
    if html_file_name_tag:
        return html_file_name_tag.text
    elif xml_file_name_tag:
        return xml_file_name_tag.text
    else:
        return ""

def _is_disclosure_file(short_name_tag, long_name_tag, file_name):
    return (
        short_name_tag is not None
        and long_name_tag is not None
        and file_name
        and ("Disclosure" in long_name_tag.text or "Note" in long_name_tag.text)
    )

def get_disclosure_file_names(ticker, accession_number, headers=None):
    try:
        session = requests.Session()
        cik = cik_matching_ticker(ticker, headers)
        base_link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"
        filing_summary_link = f"{base_link}/FilingSummary.xml"
        filing_summary_response = session.get(
            filing_summary_link, headers=headers
        ).content.decode("utf-8")

        filing_summary_soup = BeautifulSoup(filing_summary_response, "lxml-xml")
        disclosure_file_names_dict = {}
        for disclosure in filing_summary_soup.find_all("Report"):
            disclosure_name = _get_disclosure_name(disclosure)
            short_name, long_name = disclosure.find("ShortName"), disclosure.find("LongName")
            if _is_disclosure_file(short_name, long_name, disclosure_name):
                disclosure_file_names_dict[short_name.text.lower()] = disclosure_name
        return disclosure_file_names_dict
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return {}
    except ValueError as e: 
        print(f"Error getting CIK for {ticker}: {e}")
        return {}

def get_facts(ticker, headers=None):
    cik = cik_matching_ticker(ticker, headers)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    company_facts = requests.get(url, headers=headers).json()
    return company_facts

def standardize_date(date: str) -> str:
    for abbr, full in zip(calendar.month_abbr[1:], calendar.month_name[1:]):
        date = date.replace(abbr, full)
    return date

def deduplicate_list(items):
    new_items = []
    counts = {}
    for item in items:
        if item in counts:
            counts[item] += 1
            new_items.append(f"{item}_{counts[item]}")
        else:
            counts[item] = 0
            new_items.append(item)
    return new_items

def get_datetime_index_dates_from_statement(soup: BeautifulSoup) -> pd.Index:
    table_headers = soup.find_all("th", {"class": "th"})
    dates = [str(th.div.string) for th in table_headers if th.div and th.div.string]
    dates = [standardize_date(date).replace(".", "") for date in dates]
    
    unique_dates_str = deduplicate_list(dates)
    return pd.Index(unique_dates_str)

def parse_table_from_file(cik, accession_number, file_name, headers, label_dict=None):
    session = requests.Session()
    base_link = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"
    table_link = f"{base_link}/{file_name}"

    try:
        response = session.get(table_link, headers=headers)
        response.raise_for_status()
        if table_link.endswith(".xml"):
            soup = BeautifulSoup(response.content, "lxml-xml", from_encoding="utf-8")
        else:
            soup = BeautifulSoup(response.content, "lxml")
    except requests.RequestException as e:
        print(f"Error fetching table {file_name}: {e}")
        return pd.DataFrame() 

    columns = []
    values_set = []
    
    date_time_index = get_datetime_index_dates_from_statement(soup)
    if date_time_index.empty:
         return pd.DataFrame() 

    for table in soup.find_all("table"):
        for row in table.select("tr"):
            onclick_elements = row.select("td.pl a, td.pl.custom a")
            if not onclick_elements:
                continue

            onclick_attr = onclick_elements[0]["onclick"]
            column_title = onclick_attr.split("defref_")[-1].split("',")[0]
            columns.append(column_title)
            values = [np.nan] * len(date_time_index)

            for i, cell in enumerate(row.select("td.text, td.nump, td.num")):
                if "text" in cell.get("class"):
                    continue
                if i >= len(values): 
                    break
                value_text = cell.text.strip()
                value_text = value_text.replace(",", "")
                if ("($ " in value_text or "$ (" in value_text) and ")" in value_text:
                    value_text = "-" + value_text.replace("$", "").replace("(", "").replace(")", "").strip()
                elif ("(¥ " in value_text or "¥ (" in value_text) and ")" in value_text:
                    value_text = "-" + value_text.replace("¥", "").replace("(", "").replace(")", "").strip()
                elif "(" in value_text and ")" in value_text:
                    value_text = "-" + value_text.replace("(", "").replace(")", "")
                else:
                    value_text = value_text.replace("$", "").replace("¥", "")
                try:
                    if value_text:
                        values[i] = float(value_text)
                    else:
                        values[i] = np.nan
                except ValueError:
                    values[i] = np.nan
            values_set.append(values)

    if not values_set: 
        return pd.DataFrame()
        
    unique_columns = deduplicate_list(columns)
    
    transposed_values_set = list(zip(*values_set))
    
    if len(transposed_values_set) != len(date_time_index):
        print(f"Data length mismatch in {file_name}. Index: {len(date_time_index)}, Data: {len(transposed_values_set)}")
        min_len = min(len(transposed_values_set), len(date_time_index))
        transposed_values_set = transposed_values_set[:min_len]
        date_time_index = date_time_index[:min_len]

    df = pd.DataFrame(transposed_values_set, columns=unique_columns, index=date_time_index)
    
    df.columns = df.columns.str.replace("us-gaap_", "", regex=False)
    df.columns = df.columns.str.replace("ifrs-full_", "", regex=False)
    
    if label_dict:
        df.columns = df.columns.map(lambda x: label_dict.get(x.split("_", 1)[-1], x))
        df.columns = deduplicate_list(df.columns)

    return df

#
# ----- 4. LÓGICA PRINCIPAL (MODIFICADA: SÓLO 10-K) -----
#
def get_data_for_ticker(ticker, headers, statement_keys_map, label_dict):
    """
    Obtiene todos los datos ANUALES (10-K/20-F) para UN solo ticker.
    (Versión simplificada SIN 10-Q).
    """
    print(f"--- Empezando análisis para: {ticker} ---")
    cik = cik_matching_ticker(ticker, headers)

    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    company_json = requests.get(url, headers=headers).json()
    filings = pd.DataFrame(company_json['filings']['recent'])
    
    # --- Lógica Anual (10-K / 20-F) ---
    accn_list_annual = filings[filings["form"].isin(["10-K", "20-F"])]
    accn_list_annual = accn_list_annual.set_index("reportDate")
    accn_list_annual["accessionNumber"] = accn_list_annual["accessionNumber"].str.replace("-", "", regex=False)
    accn_list_annual = accn_list_annual[["accessionNumber", "form"]]

    # --- LÓGICA TRIMESTRAL (10-Q) ELIMINADA ---

    # --- Procesar Anuales ---
    all_annual_statements = {"income_statement": [], "balance_sheet": [], "cash_flow": []}
    all_annual_disclosures = {} 
    print(f"Fetching {len(accn_list_annual)} annual filings for {ticker}...")
    for report_date, row in accn_list_annual.iterrows():
        accn = row["accessionNumber"]
        try:
            stmt_files = get_statement_file_names_in_filing_summary(ticker, accn, headers=headers)
            for key, stmts in statement_keys_map.items():
                for stmt_name in stmts:
                    file_name = stmt_files.get(stmt_name.lower())
                    if file_name:
                        df = parse_table_from_file(cik, accn, file_name, headers, label_dict)
                        if key == "cash_flow_statement": key = "cash_flow" 
                        all_annual_statements[key].append(df)
                        break
        except Exception as e:
            print(f"Error getting annual statements for {ticker} ({accn}): {e}")
        
        try:
            disclosure_files = get_disclosure_file_names(ticker, accn, headers=headers)
            yearly_disclosures = {}
            for name, file_name in disclosure_files.items():
                disc_df = parse_table_from_file(cik, accn, file_name, headers, label_dict)
                if not disc_df.empty:
                    yearly_disclosures[name] = json.loads(disc_df.to_json(orient="index", date_format="iso"))
            if yearly_disclosures:
                all_annual_disclosures[report_date] = yearly_disclosures
        except Exception as e:
            print(f"Error getting annual disclosures for {accn}: {e}")

    # --- PROCESAMIENTO TRIMESTRAL ELIMINADO ---

    # --- Preparar JSON de Salida ---
    IS_final = pd.DataFrame()
    BS_final = pd.DataFrame()
    CF_final = pd.DataFrame()
    if all_annual_statements["income_statement"]:
        IS_final = pd.concat(all_annual_statements["income_statement"]).sort_index(ascending=False)
        IS_final = IS_final[~IS_final.index.duplicated(keep='first')]
    if all_annual_statements["balance_sheet"]:
        BS_final = pd.concat(all_annual_statements["balance_sheet"]).sort_index(ascending=False)
        BS_final = BS_final[~BS_final.index.duplicated(keep='first')]
    if all_annual_statements["cash_flow"]:
        CF_final = pd.concat(all_annual_statements["cash_flow"]).sort_index(ascending=False)
        CF_final = CF_final[~CF_final.index.duplicated(keep='first')]

    is_json = json.loads(IS_final.to_json(orient="index", date_format="iso"))
    bs_json = json.loads(BS_final.to_json(orient="index", date_format="iso"))
    cf_json = json.loads(CF_final.to_json(orient="index", date_format="iso"))
    
    ticker_output = {
        "ticker": ticker,
        "cik": cik,
        "annual_filings": {
            "income_statement": is_json,
            "balance_sheet": bs_json,
            "cash_flow": cf_json,
            "disclosures": all_annual_disclosures
        }
        # "quarterly_filings" HA SIDO ELIMINADO
    }
    
    print(f"--- Análisis para {ticker} completado. ---")
    return ticker_output


def run_financial_analysis(request_data):
    """
    Función principal que recibe una LISTA de tickers
    y devuelve los datos de todos ellos.
    """
    
    tickers_list = request_data.get('tickers') 
    if not tickers_list or not isinstance(tickers_list, list):
        raise ValueError("Missing 'tickers' list in JSON payload.") 

    try:
        facts = get_facts(tickers_list[0], headers) 
        gaap_data = facts.get("facts", {}).get("us-gaap", {})
        if not gaap_data: 
            gaap_data = facts.get("facts", {}).get("ifrs-full", {})
        label_dict = {fact: details["label"] for fact, details in gaap_data.items()}
    except Exception as e:
        print(f"Could not get label dictionary: {e}")
        label_dict = {}

    final_results = {}
    
    for ticker in tickers_list:
        try:
            ticker_data = get_data_for_ticker(ticker, headers, statement_keys_map, label_dict)
            final_results[ticker] = ticker_data
        except Exception as e:
            print(f"Error CRÍTICO procesando {ticker}: {e}")
            final_results[ticker] = {"error": str(e)}

    return final_results

#
# ----- 5. ESTE ES EL ENVOLTORIO DE FLASK (EL SERVIDOR WEB) -----
#
app = Flask(__name__)

@app.route('/run', methods=['POST'])
def handler():
    try:
        request_data = request.json
        result = run_financial_analysis(request_data)
        
        # Chequeo de error simple
        if isinstance(result, dict) and "error" in result:
             return jsonify(result), 500
            
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": f"Flask wrapper error: {str(e)}"}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
