# LLM SQL Query Generator and Executor README

## Overview
This Python script gets table information from PostgreSQL and sends it to GPT-4 to generate a SQL query to get the data.

## Features
- **Database Connection**: Establishes connection to a PostgreSQL database using SQLAlchemy and psycopg2.
- **Table Exploration**: Retrieves and displays names of tables within the database.
- **Random Row Sampling**: Fetches a specified number of random rows from a chosen table to assist in forming query prompts.
- **SQL Query Generation**: Leverages the OpenAI GPT-4 model to generate SQL queries based on user-provided prompts, integrating table definitions and other contextual information.
- **Query Execution**: Executes the generated SQL queries and presents the results.
- **Gradio Interface**: Provides a web interface for easy interaction with the SQL query generation and execution functionality.

## Dependencies
- pandas
- SQLAlchemy
- psycopg2
- openai
- gradio

## Usage
1. **Set Up Environment**: Ensure all dependencies are installed in your Python environment.
2. **Database Credentials**: Modify the `db_host`, `db_port`, `db_name`, `db_user`, and `db_password` variables to match your PostgreSQL database credentials.
3. **OpenAI API Key**: Update the `OPENAI_API_KEY` variable with your OpenAI API key.
4. **Running the Script**: Execute the script to start the Gradio web interface.
5. **Interacting with the Interface**: Enter your SQL query prompts in the Gradio interface textbox and submit to generate and execute the SQL query. The interface will display the query result or the generated SQL query if execution fails.

## Functions
- **run_query(prompt, return_natural_language=False)**: Write a text and get the SQL query generated, and get data back if it works.

## Security Note
The provided script includes hardcoded database credentials and an OpenAI API key for demonstration purposes. In a real-world scenario, it is crucial to securely manage sensitive information such as database credentials and API keys, preferably using environment variables or secure vaults. 

## Disclaimer
This script is intended for educational and demonstration purposes. Users should exercise caution and respect privacy and security guidelines when connecting to databases and using third-party APIs.
