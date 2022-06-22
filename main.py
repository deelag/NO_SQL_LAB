from flask import request, Flask
from controllers.data_processor import DataProcessor, ProcessingError

app = Flask(__name__)

@app.route("/load-json", methods=['POST'])
def load_file():
    request_data = request.get_json()
    
    strategy = None
    json_endpoint = None

    if "strategy" in request_data:
        strategy = request_data["strategy"]
    else:
        return "No strategy provided, choose between \"kafka\" or \"console\"", 400
    
    if "json_endpoint" in request_data:
        json_endpoint = request_data["json_endpoint"]
    else:
        return "No json endpoint provided", 400

    try:
        data_processor = DataProcessor(strategy, json_endpoint)
        result = data_processor.process()
    except ProcessingError as e:
        return e.msg, 500

    return result

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5000")
