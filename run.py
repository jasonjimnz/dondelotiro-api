from flask import Flask
from flask import jsonify
from flask import request
from model import GraphModel


app = Flask(__name__)


g_model = GraphModel()


@app.route('/', methods=['POST'])
def get_distances():
    lat = request.form.get('lat', None)
    lon = request.form.get('lon', None)

    if lat and lon:
        distances = [
            {
                'lat': r['latitude'],
                'lon': r['longitude'],
                'distance': r['point_distance'],
                'container_type': r['container_type'],
                'trash_types': r['trash_types']
            } for r in g_model.get_distances(float(lat), float(lon)).records()
        ]

        return jsonify({'dist': distances}), 200
    else:
        return jsonify({'msj': "You must provide lat and lon params"}), 400


if __name__ == "__main__":
    app.run(
        host='0.0.0.0',
        port=3000,
        debug=True
    )