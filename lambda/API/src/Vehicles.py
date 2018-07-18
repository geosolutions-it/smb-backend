'''
Created on 13 apr 2018

@author: gnafu
'''

from flask import jsonify, request, json
from flask_restful import reqparse, Resource

from Database import get_db, TABLE_NAMES, sql
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
                tagId=args['tagId']
            except ValueError: 
                pass
            
        
        if tagId is not None:
            SQL="""Select v.id as id, v.lastupdate, 1 as type, v.nickname as name, CASE WHEN vs.lost = true THEN 1 ELSE 0 END as status, v.picture_gallery_id, v.owner_id as owner, 
                        CASE WHEN vp.position IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         vp.id,
                                'geometry',   ST_AsGeoJSON(vp.position)::jsonb,
                                'properties', CASE WHEN vp.reporter_id IS NOT NULL THEN  json_build_object(
                                                                                        'reporter_id', vp.reporter_id
                                                                                     ) ELSE '{{}}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        FROM {} as v 
                        LEFT JOIN 
                        (
                            SELECT vs1.bike_id, vs1.lost
                            FROM vehicles_bikestatus vs1
                            LEFT JOIN vehicles_bikestatus vs2 ON vs1.bike_id = vs2.bike_id AND vs1.creation_date < vs2.creation_date
                            WHERE vs2.creation_date IS NULL
                        ) as vs
                        ON vs.bike_id = v.id
                        LEFT JOIN 
                        (
                            SELECT vp1.id, vp1.bike_id, vp1.position, vp1.reporter_id
                            FROM {} vp1
                            LEFT JOIN {} vp2 ON vp1.bike_id = vp2.bike_id AND vp1.observed_at < vp2.observed_at
                            WHERE vp2.observed_at IS NULL
                        ) as vp
                        ON vp.bike_id = v.id
                    JOIN {} as t ON v.id = t.bike_id where t.epc = %s;"""
            SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['tags']))
            data = ( tagId,)
        else :            
            #SQL="SELECT id, lastupdate, type, name, status, lastposition, image, owner FROM vehicles order by id limit %s offset %s;"
            SQL = """Select v.id as id, v.lastupdate, 1 as type, v.nickname as name, CASE WHEN vs.lost = true THEN 1 ELSE 0 END as status, v.picture_gallery_id, v.owner_id as owner, 
                        CASE WHEN vp.position IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         vp.id,
                                'geometry',   ST_AsGeoJSON(vp.position)::jsonb,
                                'properties', CASE WHEN vp.reporter_id IS NOT NULL THEN  json_build_object(
                                                                                        'reporter_id', vp.reporter_id
                                                                                     ) ELSE '{{}}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        FROM {} as v 
                        LEFT JOIN 
                        (
                            SELECT vs1.bike_id, vs1.lost
                            FROM vehicles_bikestatus vs1
                            LEFT JOIN vehicles_bikestatus vs2 ON vs1.bike_id = vs2.bike_id AND vs1.creation_date < vs2.creation_date
                            WHERE vs2.creation_date IS NULL
                        ) as vs
                        ON vs.bike_id = v.id
                        LEFT JOIN 
                        (
                            SELECT vp1.id, vp1.bike_id, vp1.position, vp1.reporter_id
                            FROM {} vp1
                            LEFT JOIN {} vp2 ON vp1.bike_id = vp2.bike_id AND vp1.observed_at < vp2.observed_at
                            WHERE vp2.observed_at IS NULL
                        ) as vp
                        ON vp.bike_id = v.id
                 order by id limit %s offset %s;"""
            SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']))
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
        
        name = content.get('name', None)
        status = content.get('status', 0)
        lastposition  = content.get('lastposition', None)
        image  = content.get('image', None)
        owner  = content.get('owner', None)
        
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "INSERT INTO {} ( nickname, picture_gallery_id, owner_id) VALUES (%s, %s, %s) RETURNING id;" 
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']))
        data = ( name,  image, owner )
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
        
        conn = get_db()
        cur = conn.cursor()
        #SQL = "SELECT v.*, ST_AsGeoJSON(d.the_geom) as last_position FROM vehicles v LEFT JOIN datapoints d on d.vehicle_id = v._id where v.id = %s order by d.timestamp desc limit 1;" 
        SQL = """Select v.id as id, v.lastupdate, 1 as type, v.nickname as name, CASE WHEN vs.lost = true THEN 1 ELSE 0 END as status, v.picture_gallery_id, v.owner_id as owner, 
                        CASE WHEN vp.position IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         vp.id,
                                'geometry',   ST_AsGeoJSON(vp.position)::jsonb,
                                'properties', CASE WHEN vp.reporter_id IS NOT NULL THEN  json_build_object(
                                                                                        'reporter_id', vp.reporter_id
                                                                                     ) ELSE '{{}}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        FROM {} as v 
                        LEFT JOIN 
                        (
                            SELECT vs1.bike_id, vs1.lost
                            FROM vehicles_bikestatus vs1
                            LEFT JOIN vehicles_bikestatus vs2 ON vs1.bike_id = vs2.bike_id AND vs1.creation_date < vs2.creation_date
                            WHERE vs2.creation_date IS NULL
                        ) as vs
                        ON vs.bike_id = v.id
                        LEFT JOIN 
                        (
                            SELECT vp1.id, vp1.bike_id, vp1.position, vp1.reporter_id
                            FROM {} vp1
                            LEFT JOIN {} vp2 ON vp1.bike_id = vp2.bike_id AND vp1.observed_at < vp2.observed_at
                            WHERE vp2.observed_at IS NULL
                        ) as vp
                        ON vp.bike_id = v.id
                 WHERE v.id = %s;"""
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']))
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
        
        SQL = "DELETE FROM {} WHERE id = %s;" 
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']))
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
        
        name = content.get('name', None)
        status = content.get('status', 0)
        lastposition  = content.get('lastposition', None)
        image  = content.get('image', None)
        owner  = content.get('owner', None) #: :type owner: tuple
        
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "Select id from {} where id::text = %s"
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']))
        data = ( vehicle_id,)
        cur.execute(SQL, data) 
        
        query_results = cur.fetchone()
        
        if query_results is None:
            return {"Error":"Cannot find vehicle"}, 500
        
        vehicle_uuid = query_results[0]
        
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
                    reporter_id = lastposition['properties']['reporter_id']
                except:
                    #TODO use the id of the actual user using this API
                    reporter_id = 1
                    
                try:
                    reporter_name = lastposition['properties']['reporter_name']
                except:
                    #TODO use the id of the actual user using this API
                    reporter_name = 1
                
                try:
                    reporter_type = lastposition['properties']['reporter_type']
                except:
                    #TODO use the id of the actual user using this API
                    reporter_type = 1
                    
                SQL = "INSERT INTO {} (bike_id, position, reporter_id, reporter_name, reporter_type, created_at, observed_at, details, address) VALUES ( %s, ST_SetSRID(ST_Point(%s, %s), 4326), %s, %s, %s, now(), now(), '', '') returning id;"
                SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']))
                data = (vehicle_uuid, lon, lat, reporter_id, reporter_name, reporter_type)
                
                cur.execute(SQL, data) 
                id_of_new_row = cur.fetchone()[0]
                    
                print('new datapoint row: %s' % (id_of_new_row,))
            else:
                return {'Error': "Please provide 'lastposition' as a valid GeoJSON point"}, 500
        
        inputslist = []
        SQL = "UPDATE {} SET lastupdate = now()" 
        if 'name' in content :
            SQL += ', nickname = %s'
            inputslist.append(name)
        if 'status' in content :
            # TODO INSERT THE NEW STATUS TO THE DATABASE
            None
        if 'image' in content :
            SQL += ', picture_gallery_id = %s'
            inputslist.append(image)
        if 'owner' in content :
            SQL += ', owner_id = %s'
            inputslist.append(owner)
        
        SQL += " where id = %s RETURNING id;"
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']))
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
            SQL="SELECT v.* FROM {} as v JOIN {} as t ON v.id = t.vehicle_id where t.epc = %s;"
            SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']), sql.Identifier(TABLE_NAMES['tags']))
            data = (tagId,)
        else :
            SQL="""Select v.id as id, v.lastupdate, 1 as type, v.nickname as name, CASE WHEN vs.lost = true THEN 1 ELSE 0 END as status, v.picture_gallery_id, k.kuid as owner, 
                        CASE WHEN vp.position IS NOT NULL THEN
                            jsonb_build_object(
                                'type',       'Feature',
                                'id',         vp.id,
                                'geometry',   ST_AsGeoJSON(vp.position)::jsonb,
                                'properties', CASE WHEN vp.reporter_id IS NOT NULL THEN  json_build_object(
                                                                                        'reporter_id', vp.reporter_id
                                                                                     ) ELSE '{{}}' END
                            )
                            ELSE NULL
                        END as lastposition 
                        FROM {} as v 
                        LEFT JOIN 
                        (
                            SELECT vs1.bike_id, vs1.lost
                            FROM vehicles_bikestatus vs1
                            LEFT JOIN vehicles_bikestatus vs2 ON vs1.bike_id = vs2.bike_id AND vs1.creation_date < vs2.creation_date
                            WHERE vs2.creation_date IS NULL
                        ) as vs
                        ON vs.bike_id = v.id
                        LEFT JOIN 
                        (
                            SELECT vp1.id, vp1.bike_id, vp1.position, vp1.reporter_id
                            FROM {} vp1
                            LEFT JOIN {} vp2 ON vp1.bike_id = vp2.bike_id AND vp1.observed_at < vp2.observed_at
                            WHERE vp2.observed_at IS NULL
                        ) as vp
                        ON vp.bike_id = v.id
                        LEFT JOIN
                        (
                            SELECT \"UID\" as kuid, user_id as portal_id
                            FROM {} as u
                        ) as k
                        ON k.portal_id = v.owner_id
                    WHERE k.kuid = %s order by k.kuid limit %s offset %s;"""
            SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['users_mapping']))
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
        
        name = content.get('name', None)
        status = content.get('status', 0)
        lastposition  = content.get('lastposition', None)
        image  = content.get('image', None)
        owner  = content.get('owner', None)
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "INSERT INTO {} (nickname, status, last_position_id, picture_gallery_id, owner_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;" 
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']))
        data = (name, status, lastposition, image, owner )
        cur.execute(SQL, data) 
        id_of_new_row = cur.fetchone()[0]        
        
        conn.commit()
        cur.close()
        
        return id_of_new_row, 201
