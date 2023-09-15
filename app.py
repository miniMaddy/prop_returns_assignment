from flask import Flask, request, jsonify
import psycopg2
import os

db_password = os.getenv("PROP_RETURNS_USER_PASS")

app = Flask(__name__)

conn = psycopg2.connect(
            host="localhost",
            database="db",
            user="prop_returns",
            password=db_password
        )

@app.route("/")
def index():
    return "Welcome to the Property Details API!"

@app.route("/property_details/search/doc_no", methods=['POST'])
def property_details_from_doc_no():
    # Connect to the database
    if request.method == 'POST':
        req = request.json
        document_no = req['document_no']

        # Create a cursor object
        cur = conn.cursor()

        # Execute a query
        cur.execute(f"SELECT * FROM property_details WHERE \"Doc_No\" = {document_no}")

        # Retrieve query results
        records = cur.fetchall()

        # Close the cursor and connection
        cur.close()

        # Return the results as JSON
        return jsonify(records)
    
    else:
        return "The request method must be POST"

@app.route("/property_details/search/year", methods=['POST'])
def property_details_from_year():
    # Connect to the database
    if request.method == 'POST':
        req = request.json
        year = req['year']

        # Create a cursor object
        cur = conn.cursor()

        # Execute a query
        cur.execute(f"SELECT * FROM property_details WHERE EXTRACT(YEAR FROM \"Date\") = {year}")

        # Retrieve query results
        records = cur.fetchall()

        # Close the cursor and connection
        cur.close()

        # Return the results as JSON
        return jsonify(records)
    else:
        return "The request method must be POST"
    
@app.route("/property_details/search/name", methods=['POST'])
def property_details_from_name():
    # Connect to the database
    if request.method == 'POST':
        req = request.json
        name = req['name']

        # Create a cursor object
        cur = conn.cursor()

        # Execute a query
        cur.execute(f"SELECT * FROM property_details WHERE \"Buyer_name\" ILIKE '%{name}%' or \"Seller_name\" ILIKE '%{name}%'")

        # Retrieve query results
        records = cur.fetchall()

        # Close the cursor and connection
        cur.close()

        # Return the results as JSON
        return jsonify(records)
    else:
        return "The request method must be POST"


