from flask import Flask
from flask_cors import CORS


def create_app():
    app = Flask(__name__)

    #Cross-Origin Resource Sharing
    CORS(app)

    # Register routes
    from app.routes import main
    app.register_blueprint(main)

    return app
