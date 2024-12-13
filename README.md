# Database Query System

A Flask-based app   provides a unified interface for working with multiple databases (MySQL, MongoDB) and natural language query processing.

## Features

- Natural Language to SQL Query conversion
- Multi-database support:
  - MySQL
  - MongoDB
  - Firebase Realtime Database (Only for upload)
- File import support (CSV/JSON) for all databases
- Database table information retrieval
- Query translation between different database systems

## Backend Deployment（under backend/）

1. Install dependencies from requirements.txt:
- `pip install -r requirements.txt`

2. Start the Flask server:
- `python run.py`

3. Download spaCy language model:
- `python -m spacy download en_core_web_sm`



## front Deployment（under react_frontend/frontend）
1. Navigate to the Frontend Directory
- `cd react_frontend/frontend`

2. Install Dependencies
- `npm install`

3. Run the Development Server
- `npm start`

## API Endpoints

### Analysis
- `POST /analyze` - Convert natural language to SQL queries

### MySQL Operations
- `GET /mysql/test` - Test MySQL connection
- `POST /sql/create` - Create/modify tables from CSV/JSON files
- `POST /sql/read` - Execute SQL queries

### MongoDB Operations
- `POST /mongodb/create` - Upload data from CSV/JSON files
- `POST /mongodb/read` - Execute MongoDB queries
- Supports both raw MongoDB queries and MySQL-like queries

### Firebase Operations
- `GET /firebase/test` - Test Firebase connection
- `POST /firebase/create` - Upload data from CSV/JSON files
- `POST /firebase/read` - Read data from specific Firebase paths

### Database Information
- `GET /tables-info` - Retrieve comprehensive information about tables and collections from all connected databases

