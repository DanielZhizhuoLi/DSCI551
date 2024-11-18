from flask import Blueprint, request, jsonify
import csv
from io import StringIO
from app.mysql_connect import get_mysql_connection
from app.firebase_connect import get_firebase_connection

routes = Blueprint('routes', __name__)

### --- CREATE --- ###

@routes.route('/sql/create', methods=['POST'])
def sql_create_from_csv():
    """
    Create or modify a MySQL table dynamically from a CSV file.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Read CSV
        csv_data = StringIO(file.stream.read().decode('utf-8'))
        csv_reader = csv.DictReader(csv_data)

        column_names = csv_reader.fieldnames
        if not column_names:
            return jsonify({"error": "Invalid CSV file"}), 400

        mysql_conn = get_mysql_connection()

        # Dynamically create or alter table to match CSV structure
        create_or_alter_table(mysql_conn, column_names)

        # Insert rows into MySQL
        results = []
        cursor = mysql_conn.cursor()
        for row in csv_reader:
            columns_placeholder = ', '.join(column_names)
            values_placeholder = ', '.join(['%s'] * len(column_names))
            query = f"INSERT INTO dynamic_table ({columns_placeholder}) VALUES ({values_placeholder})"
            cursor.execute(query, tuple(row[col] for col in column_names))
            mysql_conn.commit()

            results.append({"mysql_id": cursor.lastrowid})

        return jsonify({"message": "Data inserted successfully!", "results": results}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route('/firebase/create', methods=['POST'])
def firebase_create_from_csv():
    """
    Create records in Firebase from a CSV file.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Read CSV
        csv_data = StringIO(file.stream.read().decode('utf-8'))
        csv_reader = csv.DictReader(csv_data)

        column_names = csv_reader.fieldnames
        if not column_names:
            return jsonify({"error": "Invalid CSV file"}), 400

        firebase_conn = get_firebase_connection()

        # Insert rows into Firebase
        results = []
        for row in csv_reader:
            firebase_data = {col: row[col] for col in column_names}
            doc_ref = firebase_conn.collection('dynamic_table').add(firebase_data)
            results.append({"firebase_doc_id": doc_ref[1].id})

        return jsonify({"message": "Data inserted successfully!", "results": results}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


### --- READ --- ###

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
    Read all documents from Firebase.
    """
    try:
        firebase_conn = get_firebase_connection()
        docs = firebase_conn.collection('dynamic_table').stream()

        # Convert Firestore documents to a list of dictionaries
        firebase_data = [{doc.id: doc.to_dict()} for doc in docs]
        return jsonify({"firebase_data": firebase_data}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


### --- UTILITY FUNCTION --- ###

def create_or_alter_table(mysql_conn, column_names):
    """
    Create or alter a MySQL table to match the CSV structure.
    """
    cursor = mysql_conn.cursor()

    # Check if the table exists
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'dynamic_table'
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Create table
        columns_definition = ', '.join([f"`{col}` TEXT" for col in column_names])
        query = f"""
            CREATE TABLE dynamic_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {columns_definition}
            );
        """
        cursor.execute(query)
    else:
        # Alter table to add missing columns
        for col in column_names:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = 'dynamic_table' AND column_name = '{col}';
            """)
            column_exists = cursor.fetchone()[0]
            if not column_exists:
                cursor.execute(f"ALTER TABLE dynamic_table ADD COLUMN `{col}` TEXT;")

    mysql_conn.commit()
