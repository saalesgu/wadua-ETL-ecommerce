from flask import Flask, render_template
import requests
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import os 

API_BASE = os.getenv("API_BASE")

app = Flask(__name__)

def get_data(endpoint):
    url = f"{API_BASE}/{endpoint}"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    return []

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/ventas")
def ventas():
    data = get_data("ventas_por_periodo")
    return render_template("ventas.html", data=data)

@app.route("/productos")
def productos():
    data = get_data("top_productos")
    return render_template("productos.html", data=data)

@app.route("/pagos")
def pagos():
    data = get_data("metodos_pago")
    return render_template("pagos.html", data=data)

if __name__ == "__main__":
    app.run(debug=True)
