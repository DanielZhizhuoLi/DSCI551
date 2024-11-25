from pymongo import MongoClient
from pymongo.server_api import ServerApi

def get_mongodb_connection(uri):
    try:
        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        #PING TO CONFIRM CONNECTION
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        
        return client
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

# Example usage
uri = "mongodb+srv://abreufue:DSCI551_CHATDB@chatdb.crdpa.mongodb.net/?retryWrites=true&w=majority&appName=ChatDB"
client = get_mongodb_connection(uri)

if client:
    # Use the client to interact with the database
    db = client["ChatDB"]
