import pandas as pd
from sqlalchemy import create_engine, text
import os
import psycopg2
from psycopg2 import sql
from openai import OpenAI

# Database connection parameters
db_host = "SERVER_NAME"
db_port = "5432"
db_name = "tradeapollo"
db_user = "tradeapollo"
db_password = "PASSWORD"

# GPT-4 model and OpenAI API key
GPT_MODEL = "gpt-4-0125-preview"
OPENAI_API_KEY = "sk-KEY"

# Initialize OpenAI client
client = OpenAI(api_key=OPENAI_API_KEY)

# Use sqlalchemy to create a connection to the database
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Establish a connection using psycopg2
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    database=db_name,
    user=db_user,
    password=db_password
)

# Return the table names in the database
def get_table_names():
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            cursor.execute(query)
            tables = cursor.fetchall()
            return tables
    except psycopg2.Error as e:
        print("Error:", e)

# Get 5 random rows from a table and store them in a dataframe
def get_random_rows(conn, table, n=5):
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("SELECT * FROM {} ORDER BY RANDOM() LIMIT {}").format(sql.Identifier(table), sql.Literal(n))
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            return df
    except psycopg2.Error as e:
        print("Error:", e)

# Generate SQL query using GPT
def generate_sql_query(prompt: str, table_definitions: str, gpt_model: str, api_key: str):
    system = "You are an SQL generator that only returns TSQL sequel statements and no natural language. Given the table names, definitions and a prompt."
    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": table_definitions + prompt}
    ]

    # Call OpenAI API to generate SQL query
    response = client.chat.completions.create(
        model=gpt_model,
        messages=messages,
        temperature=0
    )

    response_message = response.choices[0].message.content
    # Replace square brackets with double quotes
    response_message = response_message.replace('[', '"').replace(']', '"')

    return response_message

# Execute SQL query and return the result
def execute_sql_query(sql_query, engine):
    with engine.connect() as conn:
        result = pd.read_sql_query(sql_query, conn).to_markdown()
    return result

# Run the entire process
def run_query(prompt: str, return_natural_language: bool = False):
    # Get table definitions
    table_definitions = ""
    for table in get_table_names():
        table_definitions += f'### {table[0]}\n'
        table_definitions += get_random_rows(conn, table[0]).to_markdown()
        table_definitions += '\n'

    # Generate SQL query using GPT
    sql_query = generate_sql_query(prompt, table_definitions, GPT_MODEL, OPENAI_API_KEY)
    sql_query = sql_query.split("```sql")[1].split("```")[0]
    try:
        # Execute SQL query
        result = execute_sql_query(sql_query, engine)

        # Return the result in natural language if specified
        if return_natural_language:
            result = parse_result_in_natural_language(prompt, result)

        return result
    except:
        return sql_query

# Function to parse the SQL result into natural language (customize as needed)
def parse_result_in_natural_language(prompt, result):
    # Add your logic here to convert SQL result to natural language
    # Example: "The result of the query 'prompt' is:\n{result}"
    return f"The result of the query '{prompt}' is:\n{result}"


import gradio as gr

def run_query_interface(prompt):
    result = run_query(prompt, return_natural_language=True)
    return result

inputs = gr.inputs.Textbox(lines=3, placeholder="Enter SQL query prompt...", label=None)
output = gr.outputs.Textbox(label=None)

gr.Interface(
    fn=run_query_interface,
    inputs=inputs,
    outputs=output,
    title="SQL Query Generator",
    description="Enter your prompt to generate and execute an SQL query.",
    allow_flagging=False,
    theme="compact",
    layout="vertical",
    css="""body {margin: 0; height: 100vh; display: flex; justify-content: center; align-items: center;}
           .gradio {max-width: 400px;}
           .gr-textarea {width: 300px; height: 100px;}
           .output_text {width: 300px;}"""
).launch()
