'''
Created on 13 apr 2018

@author: gnafu
'''

from flask import jsonify, request, json
from flask_restful import reqparse, Resource

from Database import get_db
from Utility import limit_int

from geojson import Point

# https://developer.github.com/v3/guides/traversing-with-pagination/
# https://developer.wordpress.org/rest-api/using-the-rest-api/pagination/
searchParser= reqparse.RequestParser()
searchParser.add_argument('orderBy').add_argument('page').add_argument('per_page').add_argument('tagId')

# VehiclesList
# shows a list of all vehicles, and lets you POST to add new vehicles
class VehiclesList(Resource):
    def get(self):
        
        args = searchParser.parse_args()

        per_page = 50;
        offset = 0;
        tagId = None
        
        if args['per_page'] is not None:
            try:
                per_page=limit_int(int(args['per_page']), 0, 100)
            except ValueError: 
                pass
        
        if args['page'] is not None:
            try:
                offset=limit_int(int(args['page']) * per_page, 0)
            except ValueError: 
                pass
            
        if args['tagId'] is not None:
            try:
                tagId=limit_int(int(args['tagId']), 0)
            except ValueError: 
                pass
            
        
        if tagId is not None:
            SQL="SELECT v.id, v.lastupdate, v.type, v.name, v.status, v.lastposition, v.image, v.owner FROM vehicles as v JOIN tags as t ON v.id = t.vehicle_id where t.epc = %s;"
            data = (tagId,)
        else :            
            #SQL="SELECT id, lastupdate, type, name, status, lastposition, image, owner FROM vehicles order by id limit %s offset %s;"
            SQL = """Select v.id, v.lastupdate, v.type, v.name, v.status, v.image, v.owner, 
                        CASE WHEN ll.lastpos IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         ll.gid,
                                'geometry',   ST_AsGeoJSON(ll.lastpos)::jsonb,
                                'properties', CASE WHEN ll.reporter IS NOT NULL THEN to_jsonb(ll.reporter) ELSE '{}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        from vehicles as v left join 
                        (
                        SELECT d1.vehicle_id, d1.the_geom as lastpos, d1._id as gid, d1.reporter
                        FROM datapoints d1
                        LEFT JOIN datapoints d2 ON d1.vehicle_id = d2.vehicle_id AND coalesce(d1.timestamp, 0) < d2.timestamp
                        WHERE d2.timestamp IS NULL
                        ) as ll
                         on v._id = ll.vehicle_id
                 order by id limit %s offset %s;"""
            data = (per_page, offset)
            
        conn = get_db()
        cur = conn.cursor()
        cur.execute(SQL, data)
        # row = cur.fetchone()
        rows = cur.fetchall()
        if rows == None:
            print("There are no results for this query")
            rows = []
        
        columns = [desc[0] for desc in cur.description]
        result = []
        for row in rows:
            row = dict(zip(columns, row))
            # print(json.dumps(row))
            # row['id'] = row['_id']
            # del row['_id']
            print(json.dumps(row))
            result.append(row)

        conn.commit()
        cur.close()
        return jsonify(result)

    def post(self):
        content = request.json
        print(content)
        
        _type = content.get('type', 1)
        name = content.get('name', None)
        status = content.get('status', 0)
        lastposition  = content.get('lastposition', None)
        image  = content.get('image', None)
        owner  = content.get('owner', None)
        
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "INSERT INTO vehicles (type, name, status, image, owner) VALUES (%s, %s, %s, %s, %s) RETURNING id;" 
        data = (_type, name, status, image, owner )
        cur.execute(SQL, data) 
        id_of_new_row = cur.fetchone()[0]        
        
        # TODO: insert lastposition in the datapoints table
        
        conn.commit()
        cur.close()
        
        return id_of_new_row, 201
    

# Vehicle
# shows a single Vehicle item and lets you delete a Vehicle item
class Vehicle(Resource):
    def get(self, vehicle_id, user_id=None):

        try:
            int(vehicle_id)
        except ValueError: 
            return None # the input is not an integer
        
        conn = get_db()
        cur = conn.cursor()
        #SQL = "SELECT v.*, ST_AsGeoJSON(d.the_geom) as last_position FROM vehicles v LEFT JOIN datapoints d on d.vehicle_id = v._id where v.id = %s order by d.timestamp desc limit 1;" 
        SQL = """Select v.id, v.lastupdate, v.type, v.name, v.status, v.image, v.owner, 
                        CASE WHEN ll.lastpos IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         ll.gid,
                                'geometry',   ST_AsGeoJSON(ll.lastpos)::jsonb,
                                'properties', CASE WHEN ll.reporter IS NOT NULL THEN to_jsonb(ll.reporter) ELSE '{}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        from vehicles as v left join 
                        (
                        SELECT d1.vehicle_id, d1.the_geom as lastpos, d1._id as gid, d1.reporter
                        FROM datapoints d1
                        LEFT JOIN datapoints d2 ON d1.vehicle_id = d2.vehicle_id AND coalesce(d1.timestamp, 0) < d2.timestamp
                        WHERE d2.timestamp IS NULL
                        ) as ll
                         on v._id = ll.vehicle_id
                 WHERE v.id = %s;"""
        data = (vehicle_id,) # using vehicle id , not uuid (uuid will be used in V2.0)
        
        try:
            cur.execute(SQL, data)
        except Exception as error: 
            print(error)
            return jsonify([])
        
        rows = cur.fetchall()
        print(rows)
        if rows == None:
            print("There are no results for this query")
            rows = []
        
        columns = [desc[0] for desc in cur.description]
        result = []
        for row in rows:
            row = dict(zip(columns, row))
            print(json.dumps(row))
            
            result.append(row)

        conn.commit()
        cur.close()
        return jsonify(result)

    def delete(self, vehicle_id, user_id=None):
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "DELETE FROM vehicles WHERE id = %s;" 
        data = (vehicle_id, )
        cur.execute(SQL, data) 
        
        conn.commit()
        cur.close()
        return '', 204

    def post(self, vehicle_id, user_id=None):
        content = request.json #: :type content: dict
        print(' -- content -- ')
        print(content)
        
        if content is None: return None, 304
        
        _type = content.get('type', 1)
        name = content.get('name', None)
        status = content.get('status', 0)
        lastposition  = content.get('lastposition', None)
        image  = content.get('image', None)
        owner  = content.get('owner', None) #: :type owner: tuple
        
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "Select _id from vehicles where id = %s"
        data = (vehicle_id,)
        cur.execute(SQL, data) 
        
        vehicle_uuid = cur.fetchone()[0]
        
        
        # update the position
        if lastposition is not None :
            # parse GeoJSON
            # position = json.loads(lastposition)
            # print(json.dumps(lastposition))
            print(' -- lastposition -- ')
            print(lastposition)
            if lastposition['type'] == 'Feature':
                print(' -- geometry -- ')
                print(lastposition['geometry'])
                print(' -- coordinates -- ')
                print(lastposition['geometry']['coordinates'])
                lon = lastposition['geometry']['coordinates'][0]
                lat = lastposition['geometry']['coordinates'][1]
                
                try:
                    reporter = lastposition['properties']['reporter']
                except:
                    reporter = "N/A"
                    
                SQL = "INSERT INTO datapoints (vehicle_id, the_geom, reporter, timestamp) VALUES ( %s, ST_SetSRID(ST_Point(%s, %s), 4326), %s, extract(epoch FROM now())*1000 :: bigint) returning id;"
                data = (vehicle_uuid, lon, lat, reporter)
                
                cur.execute(SQL, data) 
                id_of_new_row = cur.fetchone()[0]
                    
                print('new datapoint row: %s' % (id_of_new_row,))
            else:
                return {'Error': "Please provide 'lastposition' as a valid GeoJSON point"}, 500
        
        inputslist = []
        SQL = "UPDATE vehicles SET lastupdate = now()" 
        if 'type' in content :
            SQL += ', type = %s'
            inputslist.append(_type)
        if 'name' in content :
            SQL += ', name = %s'
            inputslist.append(name)
        if 'status' in content :
            SQL += ', status = %s'
            inputslist.append(status)
        if 'image' in content :
            SQL += ', image = %s'
            inputslist.append(image)
        if 'owner' in content :
            SQL += ', owner = %s'
            inputslist.append(owner)
        
        SQL += " where id = %s RETURNING id;"
        inputslist.append(vehicle_id)
        
        data = tuple(inputslist)
        cur.execute(SQL, data) 
        id_of_new_row = cur.fetchone()[0]        
        
        conn.commit()
        cur.close()
        
        return id_of_new_row, 201



# UserVehiclesList
# shows a list of the user's vehicles, and lets you POST to add new vehicles
class UserVehiclesList(Resource):
    def get(self, user_id):
        
        try:
            int(user_id)
        except ValueError: 
            return None # the input is not an integer
        
        args = searchParser.parse_args()

        per_page = 50;
        offset = 0;
        tagId = None
        
        if args['per_page'] is not None:
            try:
                per_page=limit_int(int(args['per_page']), 0, 100)
            except ValueError: 
                pass
        
        if args['page'] is not None:
            try:
                offset=limit_int(int(args['page']) * per_page, 0)
            except ValueError: 
                pass
            
        if args['tagId'] is not None:
            try:
                tagId=limit_int(int(args['tagId']), 0)
            except ValueError: 
                pass
            
        
        if tagId is not None:
            SQL="SELECT v.* FROM vehicles as v JOIN tags as t ON v.id = t.vehicle_id where t.epc = %s;"
            data = (tagId,)
        else :            
            SQL="""Select v.id, v.lastupdate, v.type, v.name, v.status, v.image, v.owner, 
                        CASE WHEN ll.lastpos IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         ll.gid,
                                'geometry',   ST_AsGeoJSON(ll.lastpos)::jsonb,
                                'properties', CASE WHEN ll.reporter IS NOT NULL THEN to_jsonb(ll.reporter) ELSE '{}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        from vehicles as v left join 
                        (
                        SELECT d1.vehicle_id, d1.the_geom as lastpos, d1._id as gid, d1.reporter
                        FROM datapoints d1
                        LEFT JOIN datapoints d2 ON d1.vehicle_id = d2.vehicle_id AND coalesce(d1.timestamp, 0) < d2.timestamp
                        WHERE d2.timestamp IS NULL
                        ) as ll
                         on v._id = ll.vehicle_id
                    WHERE owner = %s order by id limit %s offset %s;"""
            data = (user_id, per_page, offset)
            
        conn = get_db()
        cur = conn.cursor()
        cur.execute(SQL, data)
        # row = cur.fetchone()
        rows = cur.fetchall()
        if rows == None:
            print("There are no results for this query")
            rows = []
        
        columns = [desc[0] for desc in cur.description]
        result = []
        for row in rows:
            row = dict(zip(columns, row))
            result.append(row)

        conn.commit()
        cur.close()
        return jsonify(result)

    def post(self):
        content = request.json
        print(content)
        
        _type = content.get('type', 1)
        name = content.get('name', None)
        status = content.get('status', 0)
        lastposition  = content.get('lastposition', None)
        image  = content.get('image', None)
        owner  = content.get('owner', None)
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "INSERT INTO vehicles (type, name, status, lastposition, image, owner) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;" 
        data = (_type, name, status, lastposition, image, owner )
        cur.execute(SQL, data) 
        id_of_new_row = cur.fetchone()[0]        
        
        conn.commit()
        cur.close()
        
        return id_of_new_row, 201
