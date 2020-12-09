from os import environ as env

HOST = env.get('MONGO_HOST', 'mongo')
PORT = env.get('MONGO_PORT', '27017')
USER = env.get('MONGO_USER', 'root')
PASSWORD = env.get('MONGO_PASSWORD', 'pass4mongo')


def output_uri(db: str, collection: str) -> str:
    return f'mongodb://{USER}:{PASSWORD}@{HOST}:{PORT}/{db}.{collection}'


def input_uri(db: str, collection: str) -> str:
    return f'{output_uri(db, collection)}?readPreference=primaryPreferred'
