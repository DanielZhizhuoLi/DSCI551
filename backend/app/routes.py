from flask import Flask, Blueprint, jsonify, request
import csv
from io import StringIO
from mysql_connect import get_mysql_connection
from mongodb_connect import get_mongodb_connection
import re

routes = Blueprint('routes', __name__)
app = Flask(__name__)

# Initialize MongoDB
mongo_client = get_mongodb_connection()
db_name = "chatdb" 
collection_name = "sample_mfix"

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
    

@routes.route('/mongodb/test', methods=['GET'])
def mongodb_test():
    """
    Test MongoDB connection and retrieve data from a sample collection.
    """
    if mongo_client:
        try:
            # Access the database and collection
            db = mongo_client[db_name]
            collection = db[collection_name]
            
            # Retrieve data from the collection
            data = list(collection.find({}, {"_id": 0}))  # Exclude `_id` field for simplicity
            
            return jsonify(data)
        except Exception as e:
            return jsonify({'error': str(e)})
    else:
        return jsonify({'error': 'Failed to connect to MongoDB'})

    
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


@routes.route('/mongodb/create', methods=['POST'])
def mongodb_create_from_csv():
    """
    Upload CSV data to MongoDB collection.
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

        # Get MongoDB connection
        mongo_client = get_mongodb_connection()
        if mongo_client is None:
            return jsonify({"error": "Could not connect to MongoDB"}), 500

        # Specify the database and collection where the data will be stored
        db_name = "chatdb"  # Replace with your MongoDB database name
        collection_name = "patients"  # Replace with your MongoDB collection name
        db = mongo_client[db_name]
        collection = db[collection_name]

        # Insert rows into the MongoDB collection
        data_to_insert = []
        for row in csv_reader:
            data_to_insert.append(row)
        
        # Insert data into MongoDB
        if data_to_insert:
            collection.insert_many(data_to_insert)

        return jsonify({"message": "CSV data successfully uploaded to MongoDB"}), 200

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

#-----------------------------------------------------------------------------------------------------------------------------------------------
def mongo_from_sql(mysql_query):
    """
    Translates a MySQL query to a MongoDB aggregation pipeline.
    Supports SELECT, WHERE, JOIN, ORDER BY, and LIMIT.
    """
    import re

    # Regex pattern to parse MySQL queries
    pattern = re.compile(
        r"SELECT\s+(?P<select>\*|[\w\.,\s]+)\s+FROM\s+(?P<table1>\w+)"
        r"(\s+AS\s+(?P<alias1>\w+))?"
        r"(\s+JOIN\s+(?P<table2>\w+)\s+ON\s+(?P<join_condition>[^\s]+(?:\s+=\s+[^\s]+)))?"
        r"(\s+WHERE\s+(?P<where>[^ORDERBYLIMIT]+))?"
        r"(\s+ORDER\s+BY\s+(?P<order_by>[^LIMIT]+))?"
        r"(\s+LIMIT\s+(?P<limit>\d+))?",
        re.IGNORECASE | re.DOTALL
    )

    match = pattern.match(mysql_query.strip())
    if not match:
        raise ValueError("Invalid or unsupported MySQL query format.")

    # Extract query parts
    query_parts = match.groupdict()

    # Debugging: Print extracted query parts
    print("\n--- Query Parts ---")
    for key, value in query_parts.items():
        print(f"{key}: {value}")

    table1 = query_parts["table1"]
    table2 = query_parts.get("table2")
    join_condition = query_parts.get("join_condition")
    select_fields = query_parts["select"]
    where_clause = query_parts.get("where")
    order_by_clause = query_parts.get("order_by")
    limit = query_parts.get("limit")

    pipeline = []

    # Handle JOIN
    if table2 and join_condition:
        print("JOIN Condition (raw):", join_condition)  # Debugging print
        join_parts = join_condition.split("=")
        if len(join_parts) != 2:
            raise ValueError("JOIN condition is too complex or improperly formatted.")
        left_field, right_field = [part.strip() for part in join_parts]
        left_field = left_field.split(".")[-1]
        right_field = right_field.split(".")[-1]
        pipeline.append({
            "$lookup": {
                "from": table2,
                "localField": left_field,
                "foreignField": right_field,
                "as": f"{table2}_joined"
            }
        })

    # Handle SELECT fields
    if select_fields.strip() == "*":
        # MongoDB's default behavior includes all fields, so no $project stage is needed
        pass
    else:
        fields = [field.strip() for field in select_fields.split(",")]
        mongo_projection = {field.split(".")[-1]: 1 for field in fields}
        pipeline.append({"$project": mongo_projection})

    # Handle WHERE clause
    if where_clause:
        mongo_query = mongo_where_clause(where_clause.strip())
        pipeline.append({"$match": mongo_query})

    # Handle ORDER BY clause
    if order_by_clause:
        mongo_sort = []
        for clause in order_by_clause.strip().split(","):
            field, order = clause.strip().split()
            mongo_sort.append((field.split(".")[-1], 1 if order.upper() == "ASC" else -1))
        pipeline.append({"$sort": dict(mongo_sort)})

    # Handle LIMIT clause
    if limit:
        mongo_limit = int(limit)
        pipeline.append({"$limit": mongo_limit})

    return {
        "collection": table1,
        "pipeline": pipeline
    }

def mongo_where_clause(where_clause):
    """
    Translates a MySQL WHERE clause to a MongoDB query object.
    """
    operators = {
        "=": "$eq",
        "!=": "$ne",
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
    }
    
    logical_operators = {
        "AND": "$and",
        "OR": "$or",
    }

    # Tokenize the WHERE clause
    tokens = re.split(r"\s+(AND|OR)\s+", where_clause, flags=re.IGNORECASE)

    conditions = []
    current_operator = None

    for token in tokens:
        token = token.strip()
        if token.upper() in logical_operators:
            current_operator = logical_operators[token.upper()]
        else:
            # Parse individual condition
            for operator, mongo_op in operators.items():
                if operator in token:
                    field, value = token.split(operator, 1)
                    field = field.strip()
                    value = value.strip().strip("'")
                    value = int(value) if value.isdigit() else value
                    conditions.append({field: {mongo_op: value}})
                    break
            else:
                raise ValueError(f"Unsupported condition: {token}")

    if current_operator and len(conditions) > 1:
        return {current_operator: conditions}
    elif len(conditions) == 1:
        return conditions[0]
    else:
        raise ValueError("Invalid WHERE clause")

@routes.route('/mongodb/read', methods=['POST'])
def mongodb_read():
    """
    Execute a query on MongoDB. Supports raw MongoDB queries and MySQL-like queries.
    """
    try:
        # Get the query details from the request body
        data = request.json
        mysql_query = data.get('mysql_query')
        collection_name = data.get('collection')
        mongo_query = data.get('query', {})
        projection = data.get('projection', {})
        limit = data.get('limit', None)
        sort = data.get('sort', None)

        if mysql_query:
            # If MySQL-like query is provided, translate it
            translated_query = mongo_from_sql(mysql_query)
            collection_name = translated_query["collection"]
            pipeline = translated_query["pipeline"]
        elif collection_name and mongo_query:
            # If raw MongoDB query is provided
            pipeline = []
            if mongo_query:
                pipeline.append({"$match": mongo_query})
            if projection:
                pipeline.append({"$project": projection})
            if sort:
                pipeline.append({"$sort": dict(sort)})
            if limit:
                pipeline.append({"$limit": limit})
        else:
            return jsonify({"error": "No valid query provided"}), 400

        # Connect to MongoDB
        mongodb_client = get_mongodb_connection()
        if mongodb_client is None:
            return jsonify({"error": "Could not connect to MongoDB"}), 500

        # Specify the database and collection
        db = mongodb_client['chatdb']  # Replace with your MongoDB database name
        collection = db[collection_name]

        # Execute the MongoDB aggregation pipeline
        data = list(collection.aggregate(pipeline))

        # Convert ObjectId to string for JSON serialization
        for doc in data:
            doc['_id'] = str(doc['_id'])

        if not data:
            return jsonify({"message": "No data found"}), 200

        # Return the data
        return jsonify({"mongodb_data": data}), 200

    except ValueError as e:
        return jsonify({"error": f"Invalid MySQL query: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    
    #----------------------------------------------------------------------------------------------------------------------------------------
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


app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(debug=True)



