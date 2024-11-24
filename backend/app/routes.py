from flask import Flask, Blueprint, jsonify, request
import csv
import json
from io import StringIO
import spacy
from .mysql_connect import get_mysql_connection
from .firebase_connect import get_firebase_connection

routes = Blueprint('routes', __name__)
app = Flask(__name__)

# Initialize Firebase
firebase_db = get_firebase_connection()

# Load the Spacy language model
nlp = spacy.load("en_core_web_sm")

# Dictionaries
verb_dict = {
    "find": "SELECT",
    "show": "SELECT",
    "list": "SELECT",
    "count": "COUNT",
    "add": "INSERT",
    "insert": "INSERT",
    "delete": "DELETE",
    "remove": "DELETE",
    "update": "UPDATE",
    "create": "CREATE",
    "drop": "DROP",
    "alter": "ALTER",
    "join": "JOIN",
    "merge": "JOIN"
}

prep_dict = {
    "by": "GROUP BY",
    "in": "WHERE",
    "where": "WHERE",
    "of": "FROM",
    "from": "FROM",
    "on": "ON",
    "into": "INTO",
    "as": "AS",
    "equals": "=",
    "is": "=",
    "are": "=",
    "not": "!=",
    "greater": ">",
    "less": "<",
    "equal": "=",
    "and": "AND",
    "or": "OR",
    "between": "BETWEEN",
    "like": "LIKE",
    "total": "SUM",
    "average": "AVG",
    "minimum": "MIN",
    "maximum": "MAX",
    "count": "COUNT",
    "sum": "SUM",
    "avg": "AVG",
    "distinct": "DISTINCT",
    "unique": "DISTINCT",
    "percent": "PERCENT",
    "ratio": "RATIO"
}

def parse_csv(file):
    """
    Parse CSV file and return rows as a list of dictionaries.
    """
    csv_data = StringIO(file.stream.read().decode('utf-8'))
    csv_reader = csv.DictReader(csv_data)
    return list(csv_reader)

def parse_json(file):
    """
    Parse JSON file and return data as a list of dictionaries.
    """
    json_data = json.load(file.stream)
    if isinstance(json_data, list):
        return json_data
    elif isinstance(json_data, dict):
        return [json_data]
    else:
        raise ValueError("Invalid JSON format: must be a list or object.")

@routes.route('/analyze', methods=['POST'])
def analyze_text():
    """
    Analyze text and generate SQL queries based on parts of speech and dictionaries.
    """
    try:
        # Get input text from the request
        data = request.json
        text = data.get('text', '')

        if not text:
            return jsonify({"error": "No text provided"}), 400

        # Process the text with spaCy
        doc = nlp(text)

        # Initialize query components
        query = []
        where_clause = []
        last_metric = None
        in_where = False

        for token in doc:
            word = token.text.lower()

            # Check if the token is a verb and in verb_dict
            if token.pos_ == "VERB" and word in verb_dict:
                query.append(verb_dict[word])

            # Check if the token is in prep_dict
            elif word in prep_dict:
                if word == "where":
                    in_where = True
                    query.append(prep_dict[word])
                elif word in ["total", "sum", "average", "minimum", "maximum", "count"]:
                    last_metric = prep_dict[word]
                elif in_where:
                    where_clause.append(prep_dict[word])
                else:
                    query.append(prep_dict[word])

            # Handle nouns and proper nouns
            elif token.pos_ in ["NOUN", "PROPN"]:
                if last_metric:
                    query.append(f"{last_metric}({token.text.upper()})")
                    last_metric = None
                elif in_where:
                    where_clause.append(token.text.upper())
                else:
                    query.append(token.text.upper())

            # Handle numbers
            elif token.pos_ == "NUM":
                if in_where:
                    where_clause.append(token.text)
                else:
                    query.append(token.text)

        if where_clause:
            query.append(" ".join(where_clause))

        final_query = " ".join(query)
        return jsonify({"query": final_query}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@routes.route('/mysql/test', methods=['GET'])
def mysql_test():
    """
    Test MySQL connection and return some data.
    """
    connection = get_mysql_connection()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT DATABASE();")  # Example query
                result = cursor.fetchone()
            connection.close()
            return jsonify({'database': result[0]})
        except Exception as e:
            return jsonify({'error': str(e)})
    else:
        return jsonify({'error': 'Failed to connect to MySQL'})


@routes.route('/firebase/test', methods=['GET'])
def firebase_test():
    """
    Test Firebase connection and retrieve data from a sample node.
    """
    if firebase_db:
        try:
            ref = firebase_db.reference('hospital_db')
            data = ref.get()
            return jsonify(data)
        except Exception as e:
            return jsonify({'error': str(e)})
    else:
        return jsonify({'error': 'Failed to connect to Firebase'})


@routes.route('/sql/create', methods=['POST'])
def sql_create_from_file():
    """
    Create or modify a MySQL table dynamically from a CSV or JSON file.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Extract the table name from the file name (without extension)
        table_name = file.filename.rsplit('.', 1)[0]

        # Determine file type (CSV or JSON) based on the extension
        if file.filename.endswith('.csv'):
            data = parse_csv(file)
        elif file.filename.endswith('.json'):
            data = parse_json(file)
        else:
            return jsonify({"error": "Unsupported file type. Please upload a CSV or JSON file."}), 400

        if not data:
            return jsonify({"error": "Empty file or invalid data."}), 400

        column_names = data[0].keys()
        mysql_conn = get_mysql_connection()

        # Dynamically create or alter table to match the file structure
        create_or_alter_table(mysql_conn, column_names, table_name)

        # Insert rows into MySQL
        results = []
        cursor = mysql_conn.cursor()
        for row in data:
            columns_placeholder = ', '.join(column_names)
            values_placeholder = ', '.join(['%s'] * len(column_names))
            query = f"INSERT INTO {table_name} ({columns_placeholder}) VALUES ({values_placeholder})"
            cursor.execute(query, tuple(row[col] for col in column_names))
            mysql_conn.commit()

            results.append({"mysql_id": cursor.lastrowid})

        return jsonify({"message": f"Data inserted successfully into table '{table_name}'!", "results": results}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/firebase/create', methods=['POST'])
def firebase_create_from_file():
    """
    Upload data from CSV or JSON file to Firebase Realtime Database, 
    storing it under a path derived from the file name.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Extract the file name without extension to use as the Firebase path
        file_name = file.filename.rsplit('.', 1)[0]

        # Determine file type (CSV or JSON) based on the extension
        if file.filename.endswith('.csv'):
            data = parse_csv(file)
        elif file.filename.endswith('.json'):
            data = parse_json(file)
        else:
            return jsonify({"error": "Unsupported file type. Please upload a CSV or JSON file."}), 400

        if not data:
            return jsonify({"error": "Empty file or invalid data."}), 400

        # Get Firebase connection
        firebase_conn = get_firebase_connection()
        if firebase_conn is None:
            return jsonify({"error": "Could not connect to Firebase"}), 500

        # Specify the path in Realtime Database using the file name
        ref = firebase_conn.reference(file_name)

        # Push rows to the database
        for row in data:
            ref.push(row)

        return jsonify({"message": f"File data successfully uploaded to Firebase under '{file_name}'"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/sql/read', methods=['POST'])
def sql_read():
    """
    Execute a specific query on MySQL and return results.
    """
    try:
        # Get the query from the request body
        data = request.json
        query = data.get('query', '')

        if not query:
            return jsonify({"error": "No query provided"}), 400

        mysql_conn = get_mysql_connection()
        cursor = mysql_conn.cursor()

        # Execute the query
        cursor.execute(query)
        rows = cursor.fetchall()

        # Extract column names from the cursor description
        columns = [desc[0] for desc in cursor.description]

        # Convert rows to a list of dictionaries
        results = [dict(zip(columns, row)) for row in rows]
        return jsonify({"mysql_data": results}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/firebase/read', methods=['POST'])
def firebase_read():
    """
    Read all data from a specific Firebase path based on the provided file name.
    """
    try:
        # Get Firebase connection
        firebase_conn = get_firebase_connection()
        if firebase_conn is None:
            return jsonify({"error": "Could not connect to Firebase"}), 500

        # Get the file name from the request body
        data = request.json
        file_name = data.get('file_name', '')

        if not file_name:
            return jsonify({"error": "No file name provided"}), 400

        # Reference the Firebase path using the file name
        ref = firebase_conn.reference(file_name)

        # Get all data from the specified node
        data = ref.get()

        if not data:
            return jsonify({"message": f"No data found under '{file_name}'"}), 200

        # Return the data
        return jsonify({"firebase_data": data}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def create_or_alter_table(mysql_conn, column_names, table_name):
    """
    Create or alter a MySQL table to match the file structure.
    """
    cursor = mysql_conn.cursor()

    # Check if the table exists
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{table_name}'
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Create table
        columns_definition = ', '.join([f"`{col}` TEXT" for col in column_names])
        query = f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {columns_definition}
            );
        """
        cursor.execute(query)
    else:
        # Alter table to add missing columns
        existing_columns = set()
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """)
        for row in cursor.fetchall():
            existing_columns.add(row[0])

        for col in column_names:
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN `{col}` TEXT;")

    mysql_conn.commit()


app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(debug=True)
