# DondeLoTiro API

## Requirements
- Python 3.5+
- Neo4J 3.3+

## Python requirements
- flask
- neo4j-driver
- requests
- beautifulsoup4

## Installation process

- Create a copy of config.py.dist file with your Neo4J data
- Run the method fill_model() in model.GraphModel() for filling the database
- Run python3 run.py for running server
- It will be deployed in the port 3000