from flask import Blueprint, jsonify

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return "test dsci551"

@main.route('/api/data', methods=['GET'])
def get_data():
    return jsonify({"test": "dsci551!"})
