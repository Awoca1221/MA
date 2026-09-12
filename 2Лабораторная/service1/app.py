from flask import Flask, jsonify

app = Flask(__name__)


@app.get("/service1/info")
def info():
    return jsonify(service="service1")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
