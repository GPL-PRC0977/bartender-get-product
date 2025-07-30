from flask import Flask, request, jsonify
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from google.oauth2 import service_account
from google.cloud import secretmanager
import json

app = Flask(__name__)

load_dotenv()

current_directory = os.path.dirname(os.path.abspath(__file__))

# FOR BQ ACCESS
ep_key_secrets = os.getenv("EP_CRED_KEYS")
ep_project_id = os.getenv("EP_PROJECT_ID")

# FOR API ENDPOINT ACCESS
ep_key_table_id = os.getenv("ep_key_table_id")
ep_key_project_id = os.getenv("ep_key_project_id")
ep_key_secret_id = os.getenv("ep_key_secret_id")


def get_credentials_from_secret(project_id: str, secret_id: str):
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = secret_client.access_secret_version(
        request={"name": secret_name})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info)
    return credentials


# FOR BQ ACCESS
ep_key_credentials_bq = get_credentials_from_secret(
    ep_project_id, ep_key_secrets)

client = bigquery.Client(credentials=ep_key_credentials_bq,
                         project=ep_key_credentials_bq.project_id)

# FOR API ENDPOINT ACCESS
ep_key_credentials_api = get_credentials_from_secret(
    ep_key_project_id, ep_key_secret_id)


def is_valid_api_key(api_key):
    api_client = bigquery.Client(
        credentials=ep_key_credentials_api, project=ep_key_project_id)

    query = f"""
        SELECT 1 FROM `{ep_key_table_id}`
        WHERE api_key = @api_key
        AND active = 1
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("api_key", "STRING", api_key)
        ]
    )
    results = api_client.query(query, job_config=job_config).result()
    return any(results)


@app.route('/bartender/items', methods=['GET'])
def get_items():
    if is_valid_api_key(request.headers.get('X-API-KEY')) is False:
        return jsonify({"error": "Unauthorized. Invalid API key."}), 401

    param = request.args.get('param', default=None, type=str)

    query = """SELECT * FROM `pgc-one-primer-dw.ds_data_bartender.item_master` 
                WHERE reference_1 = @param or cas_no = @param"""

    query_parameters = []
    if param:
        query_parameters.append(bigquery.ScalarQueryParameter(
            "param", "STRING", f"{param}"))

    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=query_parameters))
        results = query_job.result()

        rows = [dict(row) for row in results]

        if not rows:
            return jsonify({"message": "No items found"}), 404
        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/bartender/product', methods=['GET'])
def get_product():
    if is_valid_api_key(request.headers.get('X-API-KEY')) is False:
        return jsonify({"error": "Unauthorized. Invalid API key."}), 401

    barcode = request.args.get('barcode', default=None, type=str)
    mall = request.args.get('mall', default=None, type=str)

    print(barcode, mall)

    query = """select * from `pgc-one-primer-dw.ds_data_bartender.products`
            where barcode = @barcode
            and mall_group_name = @mall"""

    query_parameters = []
    if barcode:
        query_parameters.append(bigquery.ScalarQueryParameter(
            "barcode", "STRING", f"{barcode}"))
    if mall:
        query_parameters.append(bigquery.ScalarQueryParameter(
            "mall", "STRING", f"{mall}"))

    print(query_parameters)

    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=query_parameters, use_legacy_sql=False))
        results = query_job.result()

        rows = [dict(row) for row in results]

        if not rows:
            return jsonify({"message": "Product not found"}), 404
        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/bartender/product_primer', methods=['GET'])
def get_product_primer():
    if is_valid_api_key(request.headers.get('X-API-KEY')) is False:
        return jsonify({"error": "Unauthorized. Invalid API key."}), 401

    barcode = request.args.get('barcode', default=None, type=str)

    query = """select * from `pgc-one-primer-dw.ds_data_bartender.products`
            where barcode = @barcode"""

    query_parameters = []
    if barcode:
        query_parameters.append(bigquery.ScalarQueryParameter(
            "barcode", "STRING", f"{barcode}"))

    print(query_parameters)

    try:
        query_job = client.query(query, job_config=bigquery.QueryJobConfig(
            query_parameters=query_parameters, use_legacy_sql=False))
        results = query_job.result()

        rows = [dict(row) for row in results]

        if not rows:
            return jsonify({"message": "Product not found"}), 404
        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# if __name__ == '__main__':
#     app_port = os.getenv("PORT")
#     app.run(debug=False, host="0.0.0.0", port=app_port)

if __name__ == '__main__':
    app.run(debug=False)
