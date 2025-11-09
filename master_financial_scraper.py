#
# ----- ESTA ES TU NUEVA LÓGICA (REEMPLAZA LA ANTERIOR) -----
#

def get_data_for_ticker(ticker, headers, statement_keys_map, label_dict):
    """
    Obtiene todos los datos anuales y trimestrales para UN solo ticker.
    """
    print(f"--- Empezando análisis para: {ticker} ---")
    cik = cik_matching_ticker(ticker, headers)

    # Obtener todos los filings
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    company_json = requests.get(url, headers=headers).json()
    filings = pd.DataFrame(company_json['filings']['recent'])
    
    # --- Lógica Anual (10-K / 20-F) ---
    accn_list_annual = filings[filings["form"].isin(["10-K", "20-F"])]
    accn_list_annual = accn_list_annual.set_index("reportDate")
    accn_list_annual["accessionNumber"] = accn_list_annual["accessionNumber"].str.replace("-", "", regex=False)
    accn_list_annual = accn_list_annual[["accessionNumber", "form"]]

    # --- Lógica Trimestral (10-Q) ---
    accn_list_quarterly = filings[filings["form"].isin(["10-Q"])]
    accn_list_quarterly = accn_list_quarterly.set_index("reportDate")
    accn_list_quarterly["accessionNumber"] = accn_list_quarterly["accessionNumber"].str.replace("-", "", regex=False)
    accn_list_quarterly = accn_list_quarterly[["accessionNumber", "form"]].head(3) 

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
        
        # Lógica de Disclosures Anuales
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

    # --- Procesar Trimestrales ---
    all_quarterly_data = {} 
    print(f"Fetching {len(accn_list_quarterly)} quarterly filings for {ticker}...")
    for report_date, row in accn_list_quarterly.iterrows():
        accn = row["accessionNumber"]
        quarterly_report = {} 
        try:
            stmt_files = get_statement_file_names_in_filing_summary(ticker, accn, headers=headers)
            for key, stmts in statement_keys_map.items():
                for stmt_name in stmts:
                    file_name = stmt_files.get(stmt_name.lower())
                    if file_name:
                        df_q = parse_table_from_file(cik, accn, file_name, headers, label_dict)
                        json_key = key.replace("_statement", "") 
                        quarterly_report[json_key] = json.loads(df_q.to_json(orient="index", date_format="iso"))
                        break
        except Exception as e:
            print(f"Error getting quarterly statements for {ticker} ({accn}): {e}")
        
        # Lógica de Disclosures Trimestrales
        try:
            disclosure_files = get_disclosure_file_names(ticker, accn, headers=headers)
            quarterly_disclosures = {}
            for name, file_name in disclosure_files.items():
                disc_df = parse_table_from_file(cik, accn, file_name, headers, label_dict)
                if not disc_df.empty:
                    quarterly_disclosures[name] = json.loads(disc_df.to_json(orient="index", date_format="iso"))
            quarterly_report["disclosures"] = quarterly_disclosures
        except Exception as e:
            print(f"Error getting quarterly disclosures for {accn}: {e}")

        all_quarterly_data[report_date] = quarterly_report

    # --- Preparar JSON de Salida para ESTE ticker ---
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
        },
        "quarterly_filings": all_quarterly_data
    }
    
    print(f"--- Análisis para {ticker} completado. ---")
    return ticker_output

#
# ----- ESTA ES TU FUNCIÓN PRINCIPAL (LA QUE ESPERA "TICKERS") -----
#
def run_financial_analysis(request_data):
    """
    Función principal que recibe una LISTA de tickers
    y devuelve los datos de todos ellos.
    """
    
    tickers_list = request_data.get('tickers') # Espera una lista: ["AAPL", "MSFT"]
    if not tickers_list or not isinstance(tickers_list, list):
        # ¡ESTE ES EL ERROR QUE DEBERÍAS VER SI N8N FALLA!
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

# Esta es la ruta que n8n llamará (ej. ...onrender.com/run)
@app.route('/run', methods=['POST'])
def handler():
    try:
        # 1. Obtiene el JSON que n8n envía
        request_data = request.json
        
        # 2. Llama a tu función principal de lógica
        result = run_financial_analysis(request_data)
        
        # 3. Comprueba si la función devolvió un error
        if isinstance(result, dict) and result.get("status_code") == 500:
            return jsonify(result), 500
            
        # 4. Devuelve el resultado como un JSON exitoso
        return jsonify(result), 200

    except Exception as e:
        # Captura cualquier error inesperado
        return jsonify({"error": f"Flask wrapper error: {str(e)}"}), 500

# Esta línea es necesaria para que Render inicie el servidor
if __name__ == "__main__":
    # Render usa Gunicorn, así que esto es solo para pruebas locales
    app.run(host='0.0.0.0', port=5000)
