import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from openai import OpenAI
from google.cloud import bigquery
import time
import json
from dotenv import load_dotenv
load_dotenv()


app = FastAPI()

# Get the environment variables
os.environ["OPENAI_API_KEY"]= os.getenv('OPENAI_API_KEY')
google_application_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
bigquery_project_id = os.getenv('BIGQUERY_PROJECT_ID')
schema_file_path = os.getenv('SCHEMA_FILE_PATH')
# Set up clients
openai_client = OpenAI()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials
bigquery_client = bigquery.Client(project=bigquery_project_id)

with open(schema_file_path,'r') as f:
    schema = json.load(f)

def dataframe_from_query(sql_query):
    query_job = bigquery_client.query(sql_query)
    df = query_job.result().to_dataframe()
    return df

def generate_sql(natural_language_query, schema, error):
    prompt = f"""
    Database Schema:
    {schema}\n
    User Query:
    {natural_language_query}\n
    Any Error:
    {error}\n
    SQL Query:
    """
    response = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": f"You are a helpful assistant that translates natural language into SQL queries based on the {prompt}. Do not output anything other than the SQL query, no text nothing.You should recognize names or any other strings of clients and brand even though some part of string is missing or mis aligned, for example: actual string: House of RARE RABBIT user: rare rabbit, rare rabit, house of rare.Use LIKE operators or any other functions to complete this task without any errors."},
            {"role": "user", "content": prompt}
        ],
    )
    return response.choices[0].message.content[7:][:-3]

def recheck_sql(user_message, schema, error=None, retries=0, max_retries=5):
    try:
        sql_query = generate_sql(user_message, schema, error)
        df = dataframe_from_query(sql_query)
        return sql_query
    except Exception as e:
        error = str(e)
        retries += 1
        if retries < max_retries:
            return recheck_sql(user_message, schema, error, retries, max_retries)
        else:
            raise Exception(f"Failed after {max_retries} attempts. Last error: {error}")
error = None

class QueryRequest(BaseModel):
    query: str

def chat_with_user():
    while True:
        user_message = input("Enter your query (or type 'exit' to stop): ")
        print(user_message)
        if user_message.lower() in ["exit", "bye", "quit"]:
            print("Goodbye!")
            break
        try:
            # Initial call to recheck_sql with retries and max_retries parameters
            final_sql_query = recheck_sql(user_message, schema, retries=0, max_retries=5)
            df = dataframe_from_query(final_sql_query)
            print(df)
        except Exception as e:
            print(f"Error processing query: {e}")


@app.post("/query")
def handle_query(request: QueryRequest):
    user_message = request.query
    if not user_message:
        raise HTTPException(status_code=400, detail="No query provided")

    # try:
    #     final_sql_query = recheck_sql(user_message, schema, error)
    #     df = dataframe_from_query(final_sql_query)
    #     return {"result": df.to_dict(orient='records')}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))
    try:
        final_sql_query = recheck_sql(user_message, schema, retries=0, max_retries=5)
        df = dataframe_from_query(final_sql_query)
        return {"result": df.to_dict(orient='records')}
    except Exception as e:
        return f"Error processing query: {e}"

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=80, log_level="info")
