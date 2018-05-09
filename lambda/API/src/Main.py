'''
Created on 13 apr 2018

@author: gnafu
'''
from flask import Flask, g
from flask_restful import  Api
from flask.json import JSONEncoder
from Vehicles import Vehicle, VehiclesList, UserVehiclesList
from Tags import Tag, TagsList
from Users import User, UsersList
from datetime import date

class CustomJSONEncoder(JSONEncoder):

    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

## app.register_blueprint(vehicles_api)

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'pgsql_db'):
        g.pgsql_db.close()


api = Api(app)

API_VERSION = 'v1.0'

api.add_resource(VehiclesList, '/'+ API_VERSION +'/vehicles')
api.add_resource(Vehicle, '/'+ API_VERSION +'/vehicles/<vehicle_id>')
api.add_resource(TagsList, '/'+ API_VERSION +'/vehicles/<vehicle_id>/tags')
api.add_resource(Tag, '/'+ API_VERSION +'/vehicles/<vehicle_id>/tags/<tag_epc>')

api.add_resource(UsersList, '/'+ API_VERSION +'/users')
api.add_resource(User, '/'+ API_VERSION +'/users/<user_id>')
api.add_resource(UserVehiclesList, '/'+ API_VERSION +'/users/<user_id>/vehicles')
api.add_resource(Vehicle, '/'+ API_VERSION +'/users/<user_id>/vehicles/<vehicle_id>', endpoint='user-vehicle')
api.add_resource(TagsList, '/'+ API_VERSION +'/users/<user_id>/vehicles/<vehicle_id>/tags', endpoint='user-vehicle-tags')
api.add_resource(Tag, '/'+ API_VERSION +'/users/<user_id>/vehicles/<vehicle_id>/tags/<tag_epc>', endpoint='user-vehicle-tag')


if __name__ == '__main__':
    app.run(debug=True)