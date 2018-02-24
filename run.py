from flask import Flask
from flask import jsonify
from flask import request
from model import GraphModel
from model import graph_driver
from model import graph_session

app = Flask(__name__)


g_model = GraphModel()


@app.route('/', methods=['POST'])
def get_distances():
    lat = request.form.get('lat', None)
    lon = request.form.get('lon', None)
    trash_types = [x.strip() for x in request.form.get('trash_types', '').split(',')]
    print(trash_types)
    if lat and lon:
        if 'clean_point' in trash_types:
            distances_points = [
                {
                    'latitude': r['latitude'],
                    'longitude': r['longitude'],
                    'distance': r['point_distance'],
                    'container_type': r['container_type'],
                    'trash_types': r['trash_types']
                } for r in g_model.get_distances(float(lat), float(lon),"clean_point").records()
            ]
        else:
            distances_points = []

        if "battery_recycling_point" in trash_types:
            distances_batteries = [
                {
                    'latitude': r['latitude'],
                    'longitude': r['longitude'],
                    'distance': r['point_distance'],
                    'container_type': r['container_type'],
                    'trash_types': r['trash_types']
                } for r in g_model.get_distances(float(lat), float(lon),"battery_recycling_point").records()
            ]
        else:
            distances_batteries = []

        if "dog_shit_trash" in trash_types:
            distances_dog_shit = [
                {
                    'latitude': r['latitude'],
                    'longitude': r['longitude'],
                    'distance': r['point_distance'],
                    'container_type': r['container_type'],
                    'trash_types': r['trash_types']
                } for r in g_model.get_distances(float(lat), float(lon),"dog_shit_trash").records()
            ]
        else:
            distances_dog_shit = []

        return jsonify(
            {
                'clean_points': distances_points,
                'battery_recycling_points': distances_batteries,
                'dog_shits': distances_dog_shit
            },

        ), 200
    else:
        return jsonify({'msj': "You must provide lat and lon params"}), 400


@app.route('/create_point', methods=['POST'])
def create_point():
    code = 400
    try:
        lat = request.form.get('lat', None)
        lon = request.form.get('lon', None)
        container_type = request.form.get('container_type', '').split(',')
        query = "CREATE (p:ProvisionalPoint{lat: %s, lon:% s , container_type: \"%s\"});"
        s = graph_session.run(query % (
            str(lat),
            str(lon),
            container_type
        ))
        msj = "Created point"
        code = 201
    except ValueError:
        msj = "Latitude or longitude is not a float number"
        code = 400
    except:
        print(e)
        msj = "Invalid container type"

    return jsonify({"msj": msj}), code


if __name__ == "__main__":
    app.run(
        host='0.0.0.0',
        port=3000,
        debug=True
    )
