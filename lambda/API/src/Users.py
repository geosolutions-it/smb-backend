'''
Created on 13 apr 2018

@author: gnafu
'''

from flask import jsonify, json
from flask_restful import reqparse, Resource

from Database import get_db, TABLE_NAMES, sql
from Utility import limit_int

searchParser= reqparse.RequestParser()
searchParser.add_argument('orderBy').add_argument('page').add_argument('per_page').add_argument('tagId').add_argument('dump')

# UsersList
# shows a list of all users, and lets you POST to add new users
class UsersList(Resource):
    def get(self):
        args = searchParser.parse_args()
         
        per_page = 50;
        offset = 0;
        dump = False;
        
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
        
        if args['dump'] is not None:
            if args['dump'] == 'true':
                dump = True 

        print("DUMP is "+str(dump))        

        conn = get_db()
        cur = conn.cursor()
        
        SQL="SELECT k.\"UID\" as id, u.username, u.first_name, u.last_name, u.email, u.is_staff, u.is_active, u.date_joined, u.nickname, u.language_preference FROM {} as u LEFT JOIN {} as k ON k.user_id = u.id order by id limit %s offset %s;"
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['users']), sql.Identifier(TABLE_NAMES['users_mapping']))
        data = (per_page, offset)

        # if dump is true compose all users/vehicles/tags and output them
        if dump:
            SQL="SELECT k.\"UID\" as id, u.id as numeric_id, u.username, u.first_name, u.last_name, u.email, u.is_staff, u.is_active, u.date_joined, u.nickname, u.language_preference FROM {} as u LEFT JOIN {} as k ON k.user_id = u.id order by id asc;"
            SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['users']), sql.Identifier(TABLE_NAMES['users_mapping']))
            data = None
        
        
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

        
        if dump:
            for i in result:
                i['vehicles'] = []
                print(json.dumps(i))
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
                        WHERE owner_id = %s order by id asc;"""
                SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['vehicles']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']), sql.Identifier(TABLE_NAMES['vehiclemonitor_bikeobservation']))
                data = (i['numeric_id'],)
                cur.execute(SQL, data)
                vehicles = cur.fetchall()
                if vehicles == None:
                    print("There are no results for vehicles query")
                    vehicles = []
                
                v_columns = [desc[0] for desc in cur.description]
                for v in vehicles:
                    v = dict(zip(v_columns, v))
                    
                    #Fill the tags
                    v['tags'] = []
                    #print(json.dumps(v))
                    
                    SQL = "SELECT epc FROM {} where bike_id = %s order by epc;" 
                    SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['tags']))
                    data = (v['id'],)
                    cur.execute(SQL, data)
                    tags = cur.fetchall()
                    if tags == None:
                        print("There are no tags for this vehicles")
                        tags = []
                    
                    t_columns = [desc[0] for desc in cur.description]
                    for t in tags:
                        t = dict(zip(t_columns, t))
                        print(json.dumps(t))
                        v['tags'].append(t)
                    
                    #Fill the images
                    v['images'] = []
                    print(json.dumps(v))
                    
                    SQL = "SELECT concat('https://dev.savemybike.geo-solutions.it/media/', image) url FROM public.photologue_photo pp LEFT JOIN public.photologue_gallery_photos pgp on pp.id = pgp.photo_id where pgp.gallery_id = %s" 
                    data = (v['picture_gallery_id'],)
                    cur.execute(SQL, data)
                    images = cur.fetchall()
                    if images == None:
                        print("There are no images for this vehicles")
                        images = []
                    
                    #t_columns = [desc[0] for desc in cur.description]
                    for img in images:
                        #t = dict(zip(t_columns, t))
                        print(json.dumps(img))
                        v['images'].append(img[0])
                    
                    
                    i['vehicles'].append(v)
                del i['numeric_id']

        conn.commit()
        cur.close()
        return jsonify(result)
'''
    def post(self):
        content = request.json
        print(content)
        
        username = content.get('username', None)
        email = content.get('email', None)
        name = content.get('name', None)
        given_name  = content.get('given_name', None)
        family_name  = content.get('family_name', None)
        preferred_username  = content.get('preferred_username', None)
        cognito_user_status  = content.get('cognito:user_status', True)
        status = content.get('status', 0)
        sub  = content.get('sub', None)
        
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "INSERT INTO {} (username, email, name, given_name, family_name, preferred_username, \"cognito:user_status\", status, sub) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;" 
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['users']))
        data = (username, email, name, given_name, family_name, preferred_username, cognito_user_status, status, sub)
        cur.execute(SQL, data) 
        id_of_new_row = cur.fetchone()[0]        
        
        conn.commit()
        cur.close()
        
        return id_of_new_row, 201
'''

# User
# shows a single User item and lets you delete a User item
class User(Resource):
    def get(self, user_id):
              
        conn = get_db()
        cur = conn.cursor()
        SQL = "SELECT k.\"UID\" as id, u.username, u.first_name, u.last_name, u.email, u.is_staff, u.is_active, u.date_joined, u.nickname, u.language_preference FROM {} as u LEFT JOIN {} as k ON k.user_id = u.id where k.\"UID\" = %s limit 1;" 
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['users']), sql.Identifier(TABLE_NAMES['users_mapping']))
        data = (user_id,) # keep the comma to make it a tuple
        cur.execute(SQL, data) 
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
'''
    def delete(self, user_id):
        conn = get_db()
        cur = conn.cursor()
        
        SQL = "DELETE FROM {} WHERE id = %s;" 
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['users']))
        data = (user_id, )
        cur.execute(SQL, data) 
        
        conn.commit()
        cur.close()
        return '', 204

    def post(self, user_id):
        content = request.json #: :type content: dict
        print(content)
        
        if content is None: return None, 304
        
        username = content.get('username', None)
        email = content.get('email', None)
        name = content.get('name', None)
        given_name  = content.get('given_name', None)
        family_name  = content.get('family_name', None)
        preferred_username  = content.get('preferred_username', None)
        cognito_user_status  = content.get('cognito:user_status', True)
        status = content.get('status', 0)
        sub  = content.get('sub', None)        
        
        conn = get_db()
        cur = conn.cursor()
        
        inputslist = []
        SQL = "UPDATE {} SET lastupdate = now()" 
        if 'username' in content :
            SQL += ', username = %s'
            inputslist.append(username)
        if 'email' in content :
            SQL += ', email = %s'
            inputslist.append(email)
        if 'name' in content :
            SQL += ', name = %s'
            inputslist.append(name)
        if 'given_name' in content :
            SQL += ', given_name = %s'
            inputslist.append(given_name)
        if 'family_name' in content :
            SQL += ', family_name = %s'
            inputslist.append(family_name)
        if 'preferred_username' in content :
            SQL += ', preferred_username = %s'
            inputslist.append(preferred_username)
        if 'cognito:user_status' in content :
            SQL += ', \"cognito:user_status\" = %s'
            inputslist.append(cognito_user_status)
        if 'status' in content :
            SQL += ', status = %s'
            inputslist.append(status)
        if 'sub' in content :
            SQL += ', sub = %s'
            inputslist.append(sub)
        
        SQL += " where id = %s RETURNING id;"
        SQL = sql.SQL(SQL).format(sql.Identifier(TABLE_NAMES['users']))
        inputslist.append(user_id)
        
        data = tuple(inputslist)
        cur.execute(SQL, data) 
        id_of_new_row = cur.fetchone()[0]        
        
        conn.commit()
        cur.close()
        
        return id_of_new_row, 201
'''