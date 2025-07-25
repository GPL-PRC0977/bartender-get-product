from flask import Flask, request, jsonify
from google.cloud import bigquery
from dotenv import load_dotenv
import os
from google.oauth2 import service_account
from google.cloud import secretmanager
import json

app = Flask(__name__)

load_dotenv()
api_key_secrets = os.getenv("API_CRED_KEYS")
api_project_id = os.getenv("API_PROJECT_ID")

# api_key_table_id = os.getenv("api_key_table_id")
# api_key_project_id = os.getenv("api_key_project_id")
# api_key_secret_id = os.getenv("api_key_secret_id")

current_directory = os.path.dirname(os.path.abspath(__file__))


def get_credentials_from_secret(project_id: str, secret_id: str):
    # Initialize Secret Manager client
    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/1"

    # Access the secret payload
    response = secret_client.access_secret_version(
        request={"name": secret_name})
    service_account_info = json.loads(response.payload.data.decode("UTF-8"))

    # Create credentials from JSON
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info)
    return credentials


api_key_credentials = get_credentials_from_secret(
    api_project_id, api_key_secrets)

key = os.path.join(current_directory, 'pgc-dma-dev-sandbox.json')
credentials = service_account.Credentials.from_service_account_file(
    key, scopes=["https://www.googleapis.com/auth/cloud-platform",
                 "https://spreadsheets.google.com/feeds",
                 "https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive.file",
                 "https://www.googleapis.com/auth/drive"],
)

client = bigquery.Client(credentials=api_key_credentials,
                         project=api_key_credentials.project_id)

API_KEY = os.getenv("API_KEY")


def require_api_key(f):
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('X-API-KEY')
        if api_key != API_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function


# def is_valid_api_key(api_key):
#     api_key_credentials = get_credentials_from_secret(
#         api_key_project_id, api_key_secret_id)
#     api_client = bigquery.Client(
#         credentials=api_key_credentials, project=api_key_project_id)

#     query = f"""
#         SELECT 1 FROM `{api_key_table_id}`
#         WHERE api_key = @api_key
#         AND active = 1
#         LIMIT 1
#     """
#     job_config = bigquery.QueryJobConfig(
#         query_parameters=[
#             bigquery.ScalarQueryParameter("api_key", "STRING", api_key)
#         ]
#     )
#     results = api_client.query(query, job_config=job_config).result()
#     return any(results)


@app.route('/items', methods=['GET'])
@require_api_key
def get_items():
    # if is_valid_api_key(request.headers.get('X-API-KEY')) is False:
    #     return jsonify({"error": "Unauthorized. Invalid API key."}), 401
    # Get the 'param' query parameter, default to None if not provided

    param = request.args.get('param', default=None, type=str)

    query = """SELECT * FROM `pgc-dma-dev-sandbox.Bartender.vw_bartender_item_master` 
                WHERE reference_1 LIKE @param"""

    query_parameters = []
    if param:
        query_parameters.append(bigquery.ScalarQueryParameter(
            "param", "STRING", f"%{param}%"))

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


if __name__ == "__main__":
    app.run(debug=True)
