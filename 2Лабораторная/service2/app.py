from flask import Flask, jsonify

app = Flask(__name__)


@app.get("/service2/info")
def info():
    return jsonify(service="service2")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
