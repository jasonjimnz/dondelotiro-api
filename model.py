import json
import csv
from neo4j.v1 import GraphDatabase
from neo4j.v1 import basic_auth
from bs4 import BeautifulSoup
from config import GRAPH_DATABASE as gdb


graph_driver = GraphDatabase.driver(
    'bolt://%s:%s' % (
        gdb['host'],
        gdb['port']
    ), auth=basic_auth(
        gdb['user'], gdb['auth']
    )
)


graph_session = graph_driver.session()


class PuntosLimpiosExtractor(object):
    file = None
    puntos_limpios = []

    def __init__(self, file_path):
        self.file = open(file_path).read()

    def load_punto_limpio(self, **kwargs):
        self.puntos_limpios.append(kwargs)

    def read_puntos_limpios_xml(self):
        b_object = BeautifulSoup(self.file, 'html.parser')
        lista_puntos = b_object.find_all('contenido')
        for l in lista_puntos:
            self.load_punto_limpio(
                **{
                    "id_entidad": l.select_one('atributo[nombre="ID-ENTIDAD"]').text,
                    "nombre": l.select_one('atributo[nombre="NOMBRE"]').text,
                    "horario": l.select_one('atributo[nombre="HORARIO"]').text,
                    "transporte": l.select_one('atributo[nombre="TRANSPORTE"]').text,
                    "descripcion": l.select_one('atributo[nombre="DESCRIPCION"]').text,
                    "accesibilidad": l.select_one('atributo[nombre="ACCESIBILIDAD"]').text,
                    "content-url": l.select_one('atributo[nombre="CONTENT-URL"]').text,
                    "localizacion": "%s %s %s" % (
                        l.select_one('atributo[nombre="CLASE-VIAL"]').text,
                        l.select_one('atributo[nombre="NOMBRE-VIA"]').text,
                        l.select_one('atributo[nombre="NUM"]').text

                    ),
                    "localidad": l.select_one('atributo[nombre="LOCALIDAD"]').text,
                    "provincia": l.select_one('atributo[nombre="PROVINCIA"]').text,
                    "codigo_postal": l.select_one('atributo[nombre="CODIGO-POSTAL"]').text,
                    "barrio": l.select_one('atributo[nombre="BARRIO"]').text,
                    "distrito": l.select_one('atributo[nombre="DISTRITO"]').text,
                    "coord_x": l.select_one('atributo[nombre="COORDENADA-X"]').text,
                    "coord_y": l.select_one('atributo[nombre="COORDENADA-Y"]').text,
                    "lat": l.select_one('atributo[nombre="LATITUD"]').text,
                    "lon": l.select_one('atributo[nombre="LONGITUD"]').text,
                }
            )


class GraphModel(object):
    def __init__(self):
        print("Initialized graph model")

    def fill_model(self):
        entries = json.load(open('puntos_limpios.json'))
        base_query = "MERGE (p:Containers{%s}) RETURN p;"
        # Clean points
        for e in entries:
            params = ''
            params += 'container_type: "clean_point",'
            params += 'entity_id: %s,' % e['id_entidad']
            params += 'name: "%s",' % e['nombre']
            params += 'schedule: "%s",' % e['horario']
            params += 'public_transport: "%s",' % e['transporte']
            params += 'description: "%s",' % e['descripcion']
            params += 'accesibility: %s,' % e['accesibilidad']
            params += 'address: "%s",' % e['localizacion']
            params += 'city: "%s",' % e['localidad']
            params += 'province: "%s",' % e['provincia']
            params += 'postal_code: %s,' % e['codigo_postal']
            params += 'neighborhood: "%s",' % e['barrio']
            params += 'district:" %s",' % e['distrito']
            params += 'lat: %s,' % e['lat']
            params += 'lon: %s' % e['lon']
            graph_session.run(base_query % params)
        # Trash types
        trash_types = ['furniture', 'electronics', 'batteries', 'dog_shit']
        container_types = [
            'clean_point',
            'dog_shit_trash',
            'battery_recycling_point'
        ]
        for t in trash_types:
            query = """
            MERGE (t:TrashType{name:"%s"})
            """
            graph_session.run(query % t)
        # marquesinas
        list_batteries_container = json.load(open('Marquesinas_contenedores_pilas_2017.json'))
        for i, e in enumerate(list_batteries_container):
            params = ''
            params += 'container_type: "battery_recycling_point",'
            params += 'entity_id: %s,' % e['Parada']
            params += 'name: "bus_stop_%s-%s",' % (str(i),e['Parada'])
            params += 'district:" %s",' % e['DISTRITO']
            params += 'lat: %s,' % e['Latitud']
            params += 'lon: %s' % e['Longitud']
            graph_session.run(base_query % params)
            print("Saved %d batteries container" % i)
        list_dog_shit_container = json.load(open('Papeleras_con_expendedor_de_bolsas.json'))
        for i,e in enumerate(list_dog_shit_container):
            print(e['latitud'].replace(',','.'), e['longitud'].replace(',','.'))
            params = ''
            params += 'container_type: "dog_shit_trash",'
            params += 'entity_id: %s,' % str(e['codigo'])
            params += 'name: "%s-%s %s",' % (str(i), str(e['codigo']), e['direccion'])
            params += 'address: "%s",' % e['direccion']
            params += 'district:" %s",' % str(e['distrito'])
            params += 'lat: %s,' % str(e['latitud'].replace(',','.'))
            params += 'lon: %s' % str(e['longitud'].replace(',','.'))
            try:
                graph_session.run(base_query % params)
            except:
                import pdb; pdb.set_trace()

            print("Saved %d dog shit containers" % i)

        # Link clean_point related trash types
        graph_session.run("""
        MATCH (t:TrashType), (c:Containers) 
        WHERE t.name in ["furniture","electronics","batteries"] 
        AND c.container_type = "clean_point" 
        MERGE (t)-[:CAN_BE_DEPLOYED_IN]->(c) 
        RETURN t,c
        """)
        # Link batteries container with their trash type
        graph_session.run("""
                MATCH (t:TrashType), (c:Containers) 
                WHERE t.name in ["batteries"] 
                AND c.container_type = "battery_recycling_point" 
                MERGE (t)-[:CAN_BE_DEPLOYED_IN]->(c) 
                RETURN t,c
                """)
        # Link dog shit container with their trash type
        graph_session.run("""
                    MATCH (t:TrashType), (c:Containers) 
                    WHERE t.name in ["dog_shit"] 
                    AND c.container_type = "dog_shit_trash" 
                    MERGE (t)-[:CAN_BE_DEPLOYED_IN]->(c) 
                    RETURN t,c
                    """)

    def get_distances(self, lat, lon, container_type):
        query = """
        MATCH p=(n:Containers)<-[r:CAN_BE_DEPLOYED_IN]-(t:TrashType)
        WHERE n.container_type = "%s"
        RETURN  
        n.lat as latitude, 
        n.lon as longitude,
        n.container_type as container_type,
        collect(t.name)  as trash_types,
        distance(
            point(
                {latitude: n.lat, longitude:n.lon}
            ), 
            point(
                {latitude: %s, longitude: %s}
            )
        ) 
        as point_distance

        ORDER BY point_distance LIMIT 5
        """ % (
            container_type,
            str(lat),
            str(lon)
        )
        return graph_session.run(query)
