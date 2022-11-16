import json
import operator
import sys
import uuid

from sqlalchemy.exc import IntegrityError
from models import Basin_info, Basin, Reach, ReachData, RecordDateData, User, UserRequests
import Celery.tasks
from db import session
from flask import jsonify, Response, request, make_response
import config, os
from resources_data import basinlist

from flask_restplus import reqparse, abort, Resource, fields, marshal_with
from flask_restful import inputs
from sqlalchemy import func, asc, desc

from werkzeug.security import generate_password_hash, check_password_hash
from shapely.geometry import geo
from geoalchemy2 import functions
from geoalchemy2.shape import to_shape

import jwt, datetime
from functools import wraps

# from app import cache
import urllib


def cache_key():
    args = request.args
    key = request.path + '?' + urllib.parse.urlencode([
        (k, v) for k in sorted(args) for v in sorted(args.getlist(k))
    ])
    return key


# ========== User ==================
user_info_fields = {
    'id': fields.Integer,
    'public_id': fields.String,
    'username': fields.String
}
"""
    Decorator: User Auth Validation Before Getting Data from Database.
    @method: GET
    @return: User Lists
    @return-type: JSON
    @raise keyError: raises an exception
    """


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']

        if not token:
            # return Response(jsonify({'message': 'Token is missing!'}), status = 401, mimetype='application/json')
            return make_response({'message': 'Could not verify'}, 401)

        try:
            data = jwt.decode(token, 'hydroclim_20211212_%23fsd3', algorithms=["HS256"])
            print(data)
            current_user = session.query(User).filter(User.public_id == data['public_id']).first()
        except Exception as error:
            return make_response({'message': 'Token is invalid!', 'error': str(error)}, 401)
        finally:
            session.close()
        return f(current_user, *args, **kwargs)

    return decorated


class UserInfo(Resource):
    """
    Get Users information
    @url: /user
    @method: GET 
    @return: User Lists
    @return-type: JSON
    @raise keyError: raises an exception
    """

    @marshal_with(user_info_fields)
    def get(self):
        userinfo = session.query(User).all()
        if not userinfo:
            abort(404, message="There is no user existed")
        return userinfo

    """
        Add/Register Users information
        @url: /user
        @method: POST 
        @return: User Lists
        @return-type: JSON
        @raise keyError: raises an exception
        """

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('email', type=str, location='form')
        parser.add_argument('password', type=str, location='form')
        parser.add_argument('institution', type=str, location='form')
        args = parser.parse_args()
        hashed_password = generate_password_hash(args['password'], method='sha256')

        new_user = User(public_id=str(uuid.uuid4()), username=args['email'], password=hashed_password,
                        institution_name=args['institution'])
        session.add(new_user)
        try:
            session.commit()
            return jsonify({'message': 'registered successfully.'})
        except IntegrityError as error:
            if error.orig.pgcode == '23505':
                abort(409, message="The email is already existed.")
                session.rollback()
            else:
                abort(500, message=str(error.orig.pgerror))
                session.rollback()
        finally:
            session.close()


class userLogin(Resource):
    """
    Get Users information 
    @url: /login
    @method: POST
	@param: Basic Auth
    @return: JWT
    @return-type: JSON
    @raise keyError: raises an exception
    """

    def post(self):
        auth = request.authorization
        if not auth or not auth.username or not auth.password:
            return make_response({"error": "Username or password is incorrect!"}, 401)
        userinfo = session.query(User).filter(User.username == auth.username).first()

        if not userinfo:
            return make_response({"error": "You have entered an invalid username. Did you register an account before?"},
                                 401)
        # if userinfo.password == auth.password:
        if check_password_hash(userinfo.password, auth.password):
            token = jwt.encode({'public_id': userinfo.public_id,
                                'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60)},
                               "[salt-string]",  # add your salt string here
                               algorithm="HS256")
            return jsonify({'token': token.decode('UTF-8')})
        return make_response({"error": "You have entered an invalid password."}, 401)


# ==========Basin ==================
basin_info_fields = {
    'id': fields.Integer,
    'name': fields.String,
    'description': fields.String
}
basin_fields = {
    'OBJECTID': fields.Integer,
    'disID': fields.Float,
    'Shape_Leng': fields.Float,
    'Shape_Area': fields.Float,
    'geom': fields.String,
    'basin_info_id': fields.Integer
    # 'basin_shp_id': fields.Integer
}
parser = reqparse.RequestParser()
parser.add_argument('basininfo', type=str)


class BasininfoResource(Resource):
    """
    Get Basin information by Basin ID.
    @url: /basininfo/<string:id>
    @method: GET
    @param id: basin information id
    @return: Basin information
    @return-type: JSON
    @raise keyError: raises an exception
    """

    @marshal_with(basin_info_fields)
    def get(self, id):
        basinfo = session.query(Basin_info).filter(Basin_info.id == id).first()
        if not basinfo:
            abort(404, message="Basin_Info {} doesn't exist".format(id))
        return basinfo

    # """
    # Delete Basin information by Basin ID.
    # @url: /basininfo/<string:id>
    # @method: DELETE
    # @param id: basin information id
    # @return: Basin information
    # @return-type: JSON
    # @raise keyError: raises an exception
    # """
    #
    # def delete(self, id):
    #     basinfo = session.query(Basin_info).filter(Basin_info.id == id).first()
    #     if not basinfo:
    #         abort(404, message="Basin_Info {} doesn't exist".format(id))
    #     session.delete(basinfo)
    #     session.commit()
    #     return {}, 204


class BasinListResource(Resource):
    """
    Get a list of Basin information
    @url: /basinlist
    @method: GET
    @return: List of Basin information
    @return-type: JSON
    @raise keyError: raises an exception
    """

    @token_required
    @marshal_with(basin_info_fields)
    def get(self):
        basinlists = session.query(Basin_info).all()
        return basinlists


class BasinResource(Resource):
    """
    Return geojson of Basins
    @url: /basin
    @return: GeoJSON of Basin information
    @return-type: GeoJSON
    @raise keyError: raises an exception
    """

    def get(self):
        # basins = session.scalar(functions.ST_AsGeoJSON(Basin.geom))
        smapping = geo.mapping
        basins = session.query(Basin).all()
        data = [{"geometry": {"coordinates": smapping(to_shape(basin.geom))["coordinates"],
                              "type": "MultiPolygon"
                              },
                 "type": "Feature",
                 "properties": {"basin_info_id": basin.basin_info_id},
                 } for basin in basins]
        return jsonify({"features": data, "type": "FeatureCollection"})


parser.add_argument('X', type=float)
parser.add_argument('Y', type=float)


# ==========Reach ==================
class ReachResource(Resource):
    """
     Return geojson of Reaches by location, if location is None, return all of the reaches features.
     @url: /basin
     @param X(option): Longtitue
     @param Y(option): Latitue
     @return: GeoJSON of Reaches information
     @return-type: GeoJSON
     @raise keyError: raises an exception
     """

    def get(self):
        # basins = session.scalar(functions.ST_AsGeoJSON(Basin.geom))
        args = parser.parse_args()
        if args['X'] is not None:
            x = args['X']
            y = args['Y']
            geom = 'SRID=4326;POINT({0} {1})'.format(y, x)
            basinquery = session.query(Basin.basin_info_id).filter(functions.ST_Contains(Basin.geom, geom)).all();
            if len(basinquery) != 0:
                basin_id = basinquery[0].basin_info_id
                smapping = geo.mapping
                # reaches= session.query(Reach.OBJECTID,functions.ST_Transform(Reach.geom,4326)).filter(Reach.OBJECTID == 80).all()
                reaches = session.query(functions.ST_Transform(Reach.geom, 4326), Reach.OBJECTID, Reach.ARCID,
                                        Reach.GRID_CODE, Reach.AreaC, Reach.Dep2, Reach.FROM_NODE, Reach.TO_NODE,
                                        Reach.HydroID, Reach.Len2, Reach.MaxEl, Reach.Len2, Reach.MinEl, Reach.OutletID,
                                        Reach.Shape_Leng, Reach.Slo2, Reach.Subbasin, Reach.SubbasinR, Reach.Wid2,
                                        ).filter(Reach.basin_id == basin_id).all()
                data = [{"type": "Feature",
                         "properties": {"OBJECTID": reach.OBJECTID,
                                        "ARCID": reach.ARCID,
                                        "GRID_CODE": reach.GRID_CODE,
                                        "AreaC": reach.AreaC,
                                        "Dep2": reach.Dep2,
                                        "FROM_NODE": reach.FROM_NODE,
                                        "TO_NODE": reach.TO_NODE,
                                        "HydroID": reach.HydroID,
                                        "Len2": reach.Len2,
                                        "MaxEl": reach.MaxEl,
                                        "Len2": reach.Len2,
                                        "MinEl": reach.MinEl,
                                        "OutletID": reach.OutletID,
                                        "Shape_Leng": reach.Shape_Leng,
                                        "Slo2": reach.Slo2,
                                        "Subbasin": reach.Subbasin,
                                        "SubbasinR": reach.SubbasinR,
                                        "Wid2": reach.Wid2
                                        },
                         "geometry": {"type": "LineString",
                                      "coordinates": smapping(to_shape(reach[0]))["coordinates"]
                                      },
                         } for reach in reaches]
                json = jsonify({"type": "FeatureCollection", "features": data})
                return json
            else:
                return jsonify({})
        else:
            smapping = geo.mapping
            # reaches= session.query(Reach.OBJECTID,functions.ST_Transform(Reach.geom,4326)).filter(Reach.OBJECTID == 80).all()
            reaches = session.query(Reach.OBJECTID, functions.ST_Transform(Reach.geom, 4326)).all()
            data = [{"type": "Feature",
                     "properties": {"OBJECTID": reach.OBJECTID},
                     "geometry": {"type": "LineString",
                                  "coordinates": smapping(to_shape(reach[1]))["coordinates"]
                                  },
                     } for reach in reaches]
            json = jsonify({"type": "FeatureCollection", "features": data})
            return json


parser.add_argument('monthstart', type=int)
parser.add_argument('monthend', type=int)
parser.add_argument('yearstart', type=int)
parser.add_argument('yearend', type=int)
parser.add_argument('basin_id', type=int)
parser.add_argument('isobserved', type=bool)
parser.add_argument('model_id', type=int)


# ==========Records ==================
class getReachData(Resource):
    """
    Return csv of reaches temp&flow information
    @url: /reachdata
    @method: GET
    @return: csv of temp&flow information
    @return-type: csv
    @raise keyError: raises an exception
    """

    def get(self):
        args = parser.parse_args()
        yearstart = args['yearstart']
        yearend = args['yearend']
        monthstart = args['monthstart']
        monthend = args['monthend']
        basinid = args['basin_id']
        model_id = args['model_id']
        # isobserved = True if args['isobserved'] == 'off' else False
        smapping = geo.mapping
        reaches = session.query(functions.ST_Transform(Reach.geom, 4326), Reach.OBJECTID, Reach.ARCID, Reach.Shape_Leng,
                                ReachData, RecordDateData).join(ReachData, Reach.id == ReachData.rch).join(
            RecordDateData, ReachData.record_month_year_id == RecordDateData.id). \
            filter(RecordDateData.year >= yearstart). \
            filter(RecordDateData.year <= yearend). \
            filter(RecordDateData.month >= monthstart). \
            filter(RecordDateData.month <= monthend). \
            filter(ReachData.basin_id == basinid). \
            filter(ReachData.model_id == model_id).all();
        # filter(RecordDateData.year == 1950).filter(RecordDateData.month == 1).all()
        data = [{"type": "Feature",
                 "properties": {"OBJECTID": reach.OBJECTID,
                                "ARCID": reach.ARCID,
                                # "GRID_CODE": reach.GRID_CODE,
                                # "AreaC": reach.AreaC,
                                # "Dep2": reach.Dep2,
                                # "FROM_NODE": reach.FROM_NODE,
                                # "TO_NODE": reach.TO_NODE,
                                # "HydroID": reach.HydroID,
                                # "Len2": reach.Len2,
                                # "MaxEl": reach.MaxEl,
                                # "Len2": reach.Len2,
                                # "MinEl": reach.MinEl,
                                # "OutletID": reach.OutletID,
                                "Shape_Leng": reach.Shape_Leng,
                                # "Slo2": reach.Slo2,
                                # "Subbasin": reach.Subbasin,
                                # "SubbasinR": reach.SubbasinR,
                                # "Wid2": reach.Wid2,
                                "temp": reach.ReachData.wtmpdegc,
                                "discharge": reach.ReachData.flow_outcms
                                },
                 "geometry": {"type": "LineString",
                              "coordinates": smapping(to_shape(reach[0]))["coordinates"]
                              },
                 } for reach in reaches]
        json = jsonify({"type": "FeatureCollection", "features": data})
        return json


class TimeFormat(fields.Raw):
    def format(self, value):
        return datetime.datetime.strftime(value, "%m/%d/%Y %I:%M:%S %p")


queries_fields = {
    'id': fields.Integer,
    'task_id': fields.String,
    'user_id': fields.Integer,
    'arguments': fields.String,
    'create_time': TimeFormat,
    'status': fields.String,
    'file_name': fields.String,
    'error_message': fields.String,
    'checked_flag': fields.Boolean
}


class getAllRequestRecords(Resource):
    """
    Return csv of reaches temp&flow information
    @url: /queries
    @method: GET
    @return: get all queries for one user
    @return-type: json
    @raise keyError: raises an exception
    """

    # parser.add_argument('timerangetype', type=int)
    @token_required
    # @marshal_with(queries_fields)
    def get(current_user, self):
        try:
            # data = session.query(UserRequests).filter(UserRequests.user_id==current_user.id).order_by(desc(UserRequests.create_time)).all();
            data = session.query(UserRequests).filter(UserRequests.user_id == current_user.id).order_by(
                desc(UserRequests.create_time)).all();
            data_dict = [{"arguments": query.arguments,
                          "geometry": query.checked_flag,
                          "create_time": query.create_time.strftime("%m/%d/%Y %I:%M:%S %p"),
                          "error_message": query.error_message,
                          "file_name": query.file_name,
                          "id": query.id,
                          "status": query.status,
                          "task_id": query.task_id,
                          "user_id": query.user_id,
                          } for query in data]

            for item in data_dict:
                if item["arguments"]:
                    arguments = json.loads(item["arguments"])
                    basinids = arguments["basinids"]
                    basin_names = ""
                    for basin_id in basinids:
                        basinfo = session.query(Basin_info).filter(Basin_info.id == basin_id).all()
                        if len(basinfo) > 0:
                            if basin_names == "":
                                basin_names = basinfo[0].name[0]
                            else:
                                basin_names = basin_names + "," + basinfo[0].name[0]
                    arguments["basin_names"] = basin_names
                    arguments.pop("basinids")
                    item["arguments"] = arguments
            return jsonify(data_dict)
        except Exception as error:
            abort(404, message=str(error))
        finally:
            session.close()


class getAllReachData(Resource):
    """
    Return csv of reaches temp&flow information
    @url: /getallreachdata
    @method: GET
    @return: geojson of all basin avg temp&flow information
    @return-type: geojson
    @raise keyError: raises an exception
    """
    parser.add_argument('timerangetype', type=int)
    parser.add_argument('model_id', type=int)
    parser.add_argument('isobserved', type=str)

    def get(self):
        args = parser.parse_args()
        yearstart = args['yearstart']
        yearend = args['yearend']
        monthstart = args['monthstart']
        monthend = args['monthend']
        basin_id = args['basin_id']
        basinids = []
        # basinids = "1_2_3_4_5_6_7_8_9_10_11_12_14_16_17_18_19_20_21_22_23_24_25_26_27_28".split("_")
        if basin_id is None:
            basinids = '1_2_3_4_5_6_7_8_9_10_11_12_14_16_17_18_19_20_21_22_23_24_25_26_27_28'.split("_")
        else:
            basinids = str(basin_id).split("_")
            # basinids = "1_2".split("_")
        model_id = args['model_id']
        timerangetype = 'subset' if args['timerangetype'] == 1 else 'full'  ## subset:1, full:2
        # isobserved = True if args['isobserved'] == 'off' else False
        smapping = geo.mapping
        # subquery 1: inner join RecordDate and ReachData by reach id; filter data by time, model, basin etc.
        alldata = []
        for basinid in basinids:
            if timerangetype == 'subset':
                subq1 = (
                    session.query(ReachData).join(RecordDateData,
                                                  ReachData.record_month_year_id == RecordDateData.id).filter(
                        RecordDateData.year >= yearstart). \
                        filter(RecordDateData.year <= yearend). \
                        filter(RecordDateData.month >= monthstart). \
                        filter(RecordDateData.month <= monthend). \
                        filter(ReachData.basin_id == basinid). \
                        filter(ReachData.model_id == model_id)).subquery()
            else:
                subq1 = (
                    session.query(ReachData).join(RecordDateData, ReachData.record_month_year_id == RecordDateData.id). \
                        filter(
                        ((RecordDateData.year == yearstart) & (RecordDateData.month >= monthstart)).self_group() | (
                                (RecordDateData.year == yearend) & (RecordDateData.month <= monthend)).self_group() | (
                                (RecordDateData.year > yearstart) & (RecordDateData.year < yearend)).self_group()). \
                        filter(ReachData.basin_id == basinid). \
                        filter(ReachData.model_id == model_id)).subquery()
            #    subquery 2: get all statisc value of temp and flow
            try:
                subq = (session.query(
                    subq1.c.rch,
                    func.avg(subq1.c.flow_outcms).label("avg_flow_outcms"),
                    func.avg(subq1.c.wtmpdegc).label("avg_wtmpdegc"))
                        .group_by(subq1.c.rch)).subquery()
                #   main query: inner join subquery 2 and reach by reach id; filter by basin id
                reaches = session.query(subq.c.avg_wtmpdegc, subq.c.avg_flow_outcms, Reach.OBJECTID.label("reachid"),
                                        functions.ST_Transform(Reach.geom, 4326), Reach.ARCID.label("ARCID"),
                                        Reach.GRID_CODE,
                                        Reach.FROM_NODE,
                                        Reach.TO_NODE, Reach.AreaC, Reach.Len2, Reach.Slo2, Reach.Wid2, Reach.Dep2,
                                        Reach.MinEl,
                                        Reach.MaxEl, Reach.Shape_Leng, Reach.HydroID, Reach.OutletID,
                                        Reach.basin_id).join(subq,
                                                             Reach.OBJECTID == subq.c.rch).filter(
                    Reach.basin_id == basinid).all()
                # filter(RecordDateData.year == 1950).filter(RecordDateData.month == 1).all()
                data = [{"type": "Feature",
                         "properties": {"OBJECTID": reach.reachid,
                                        "ARCID": reach.ARCID,
                                        # "GRID_CODE": reach.GRID_CODE,
                                        # "AreaC": reach.AreaC,
                                        # "Dep2": reach.Dep2,
                                        # "FROM_NODE": reach.FROM_NODE,
                                        # "TO_NODE": reach.TO_NODE,
                                        # "HydroID": reach.HydroID,
                                        # "Len2": reach.Len2,
                                        # "MaxEl": reach.MaxEl,
                                        # "Len2": reach.Len2,
                                        # "MinEl": reach.MinEl,
                                        # "OutletID": reach.OutletID,
                                        "Shape_Leng": reach.Shape_Leng,
                                        # "Slo2": reach.Slo2,
                                        # "Subbasin": reach.Subbasin,
                                        # "SubbasinR": reach.SubbasinR,
                                        # "Wid2": reach.Wid2,
                                        "basin_id": reach.basin_id,
                                        "basin_name": basinlist[str(basinid)],
                                        "temp": reach.avg_wtmpdegc,
                                        "discharge": reach.avg_flow_outcms
                                        },
                         "geometry": {"type": "LineString",
                                      "coordinates": smapping(to_shape(reach[3]))["coordinates"]
                                      },
                         } for reach in reaches]
                alldata.extend(data)
            except Exception as error:
                abort(500, message=str(error))
                session.rollback()
            finally:
                session.close()
        json = jsonify({"type": "FeatureCollection", "features": alldata})
        return json


class ReachDataZip(Resource):
    """
    Return zip of reaches temp&flow information: 1.shapefiles 2. Statistics 3. raw data
    @url: /reachdatazip
    @method: GET
    @return: zip of temp&flow information: 1.shapefiles README 2. Statistics 3. raw data
    @return-type: zip
    @raise keyError: raises an exception
    """
    parser.add_argument('timerangetype', type=int)
    parser.add_argument('basinids', type=str)
    parser.add_argument('isobserved', type=str)
    parser.add_argument('isRCP45', type=str)
    parser.add_argument('isRCP85', type=str)
    parser.add_argument('rcp45', type=str)
    parser.add_argument('rcp85', type=str)
    parser.add_argument('israwdata', type=inputs.boolean)
    parser.add_argument('isstastics', type=inputs.boolean)
    parser.add_argument('isavg', type=inputs.boolean)
    parser.add_argument('ismax', type=inputs.boolean)
    parser.add_argument('ismin', type=inputs.boolean)
    parser.add_argument('isSD', type=inputs.boolean)
    parser.add_argument('isVa', type=inputs.boolean)

    # @cache.cached(key_prefix=cache_key)
    @token_required
    def get(current_user, self):

        # basinlist = returnBasinDict()

        args = parser.parse_args()
        # time range
        yearstart = args['yearstart']
        yearend = args['yearend']
        monthstart = args['monthstart']
        monthend = args['monthend']
        timerangetype = 'subset' if args['timerangetype'] == 1 else 'full'  ## subset:1, full:2

        # basin range
        basinstr = args['basinids']
        basinids = basinstr.split('_')

        # model range
        isobserved = True if args['isobserved'] == 'on' else False
        isRCP45 = True if args['isRCP45'] == 'on' else False
        isRCP85 = True if args['isRCP85'] == 'on' else False
        model_RCP45_ids = str(args['rcp45']).split('_')
        model_RCP85_ids = str(args['rcp85']).split('_')

        # stastics
        if args['israwdata']:
            israwdata = True
        else:
            israwdata = False
        isstastics = True if args['isstastics'] else False
        isavg = True if args['isavg'] else False
        ismax = True if args['ismax'] else False
        ismin = True if args['ismin'] else False
        isSD = True if args['isSD'] else False
        isVa = True if args['isVa'] else False

        # ###STARTS QUERYING AND WRITE ZIP FILES HERE
        # ###Create Zip Files, starts IO
        # # memory_file = BytesIO()
        # zf = zipstream.ZipFile(compression=zipstream.ZIP_DEFLATED)
        # ### 1. Each Basin
        # for basinid in basinids:
        #     print(basinlist[str(basinid)])
        #     basinname = str(basinlist[basinid])
        #
        #     # with zipfile.ZipFile(memory_file, 'a') as zf:
        #
        #     # with zipstream.ZipFile(memory_file,compression=zipstream.ZIP_DEFLATED ) as zf:
        #
        #     ### 1. ShapeFiles and README: locate shape file path and add them to archive by .
        #     shapefileSourcePath = os.path.join(config.SHAPEFILES_PATH, basinname, "Shape.zip").replace("\\", "/");
        #     arcnamePath = os.path.join('results', basinname, "Shape.zip").replace("\\", "/")
        #     zf.write(shapefileSourcePath, arcnamePath, zipfile.ZIP_DEFLATED)
        #
        #     readmefileSourcePath = os.path.join(config.SHAPEFILES_PATH, "README.txt").replace("\\", "/");
        #     readmearcnamePath = os.path.join('results', "README.txt").replace("\\", "/")
        #     zf.write(readmefileSourcePath, readmearcnamePath, zipfile.ZIP_DEFLATED)
        #
        #     ### 2. Raw data and Stastistics data
        #     ### (A).if query have observered data
        #     if isobserved:
        #         ### Check if historical data sit in date range[1950-1999]
        #         if int(yearstart) <= 1999:
        #             ### (a)if return raw data
        #             if israwdata:
        #                 model_t_id = 0
        #                 observed_raw_csv = fetchRawData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
        #                                                 model_t_id)
        #
        #                 #### add observed raw data to ZIP files
        #                 # with zipfile.ZipFile(memory_file, 'a') as zf:
        #                 # with zipstream.ZipFile(memory_file, mode="w", compression=zipstream.ZIP_DEFLATED) as zf:
        #                 raw_obs_file_path = os.path.join('results', basinname,'Historical observations/',
        #                                                  # file path and name EXAMPLE: ../[basinname]/Historical observations/Raw_Historical_[basinname]_subset_1950_1955_02_03.csv
        #                                                  "Raw_Historical_" + basinname + "_" + timerangetype + '_' + str(
        #                                                      yearstart) + "_" + str(
        #                                                      yearend) + "_" + str(monthstart) + "_" + str(
        #                                                      monthend) + ".csv").replace("\\", "/")
        #                 # data = zipfile.ZipInfo(raw_obs_file_path)
        #                 # data.date_time = time.localtime(time.time())[:6]
        #                 # data.compress_type = zipfile.ZIP_DEFLATED
        #                 zf.write_iter(raw_obs_file_path, iterable(observed_raw_csv))
        #             ##memory_file, csv, subpath, basinname, f_prefix, timerangetype, yearstart, yearend, monthstart, monthend
        #             ### (b).if return stastics data
        #             if isstastics:
        #                 stastics_csv = fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
        #                                                  0, isavg, ismax,
        #                                                  ismin, isSD, isVa)
        #                 # file path and name EXAMPLE: ../[basinname]/Historical observations/Raw_Statistics_[basinname]_subset_1950_1955_02_03.csv
        #                 addToZipFiles(zf, stastics_csv, 'Historical observations/', basinname, 'Statistics_Historical_', timerangetype,
        #                               yearstart, yearend,
        #                               monthstart, monthend)
        #     ### (B). if query have RCP 45 models data
        #     if isRCP45:
        #         ### (a)if return raw data
        #         if israwdata:
        #             for model_id in model_RCP45_ids:
        #                 model_name = rcp45list[str(model_id)]
        #                 model_raw_csv = fetchRawData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
        #                                              model_id)
        #                 # file path and name EXAMPLE: ../[basinname]/RCP45/1_access1-0-rcp85/raw_1_access1-0-rcp85_[basinname]_subset_1950_1955_02_03.csv
        #                 addToZipFiles(zf, model_raw_csv, 'RCP45/' + model_name, basinname, 'Raw_' + model_name,
        #                               timerangetype, yearstart,
        #                               yearend, monthstart, monthend)
        #         ### (b).if return stastics data
        #         if isstastics:
        #             for model_id in model_RCP45_ids:
        #                 model_name = rcp45list[str(model_id)]
        #                 model_statsics_csv = fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype,
        #                                                        basinid,
        #                                                        model_id, isavg, ismax, ismin, isSD, isVa)
        #                 # file path and name EXAMPLE: ../[basinname]/RCP45/1_access1-0-rcp45/stastics_1_access1-0-rcp45_[basinname]_subset_1950_1955_02_03.csv
        #                 addToZipFiles(zf, model_statsics_csv, 'RCP45/' + model_name, basinname,
        #                               'Statistics_' + model_name,
        #                               timerangetype, yearstart,
        #                               yearend, monthstart, monthend)
        #     if isRCP85:
        #         ### (a)if return raw data
        #         if israwdata:
        #             for model_id in model_RCP85_ids:
        #                 model_name = rcp85list[str(model_id)]
        #                 model_raw_csv = fetchRawData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
        #                                              model_id)
        #                 # file path and name EXAMPLE: ../[basinname]/RCP85/1_access1-0-rcp85/raw_1_access1-0-rcp85_[basinname]_subset_1950_1955_02_03.csv
        #                 addToZipFiles(zf, model_raw_csv, 'RCP85/' + model_name, basinname, 'Raw_' + model_name,
        #                               timerangetype, yearstart,
        #                               yearend, monthstart, monthend)
        #         ### (b).if return stastics data
        #         if isstastics:
        #             for model_id in model_RCP85_ids:
        #                 model_name = rcp85list[str(model_id)]
        #                 model_statsics_csv = fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype,
        #                                                        basinid,
        #                                                        model_id, isavg, ismax, ismin, isSD, isVa)
        #                 # file path and name EXAMPLE: ../[basinname]/RCP85/1_access1-0-rcp85/stastics_1_access1-0-rcp85_[basinname]_subset_1950_1955_02_03.csv
        #                 addToZipFiles(zf, model_statsics_csv, 'RCP85/' + model_name, basinname,
        #                               'Statistics_' + model_name,
        #                               timerangetype, yearstart,
        #                               yearend, monthstart, monthend)
        #
        # # zf = zipstream.ZipFile(memory_file, mode="w", compression=zipstream.ZIP_DEFLATED)
        # def generator(zip):
        #     for chunk in zip:
        #         yield chunk

        create_time = datetime.datetime.now()
        create_time_string = create_time.strftime("%Y%m%d_%H%M%S")

        curr_user_id = current_user.public_id
        curr_user_name = current_user.username

        new_record = UserRequests(
            user_id=current_user.id,
            status='start',
            create_time=create_time,
            checked_flag=True

        )

        try:
            session.add(new_record)
            session.commit()
            session.refresh(new_record)
            arguments = {"request_id": new_record.id,
                         "curr_user_id": curr_user_id,
                         "curr_user_name": curr_user_name,
                         "create_time": create_time_string,
                         "yearstart": yearstart,
                         "yearend": yearend,
                         "monthstart": monthstart,
                         "monthend": monthend,
                         "timerangetype": timerangetype,
                         "basinids": basinids,
                         "isobserved": isobserved,
                         "isRCP45": isRCP45,
                         "isRCP85": isRCP85,
                         "model_RCP45_ids": model_RCP45_ids,
                         "model_RCP85_ids": model_RCP85_ids,
                         "israwdata": israwdata,
                         "isstastics": isstastics,
                         "isavg": isavg,
                         "ismax": ismax,
                         "ismin": ismin,
                         "isSD": isSD,
                         "isVa": isVa
                         }
            parameters = json.dumps(arguments)
            new_record.arguments = parameters
            session.commit()
            session.refresh(new_record)
        except:
            session.rollback()
            e = sys.exc_info()[0]
            return jsonify({"error": "Error: %s" % e})
        request_id = new_record.id
        task = Celery.tasks.generateZipFile.apply_async((request_id, curr_user_id, curr_user_name, create_time_string,
                                                         yearstart, yearend, monthstart, monthend, timerangetype,
                                                         basinids, isobserved, isRCP45, isRCP85, model_RCP45_ids,
                                                         model_RCP85_ids, israwdata, isstastics, isavg, ismax, ismin,
                                                         isSD, isVa), link_error=Celery.tasks.error_handler.s())
        # memory_file.seek(0)
        return jsonify({"task_id": task.task_id})
        ###return Response(
        #    generator(zf),
        #    mimetype="application/zip",
        #    # content_type="application/octet-stream",
        ###    headers={"Content-disposition": "attachment; filename=hydroclim_data.zip"})


class GetZip(Resource):
    def get(self, filename):
        try:
            def send_chunk():
                store_path = os.path.join(config.ZIPFILES_PATH, filename)
                with open(store_path, 'rb') as target_file:
                    while True:
                        chunk = target_file.read(20 * 1024 * 1024)
                        if not chunk:
                            break
                        yield chunk

            if os.path.exists(os.path.join(config.ZIPFILES_PATH, filename)):
                return Response(send_chunk(), content_type='application/octet-stream')
            else:
                abort(404, message="File Not Found")
        except Exception as error:
            session.rollback()
            abort(404, message=str(error))


class GetUnCheckedNumber(Resource):
    @token_required
    def get(current_user, self):
        try:
            unchecked = session.query(UserRequests).filter(UserRequests.checked_flag == False).filter(
                UserRequests.user_id == current_user.id).count()
            return jsonify({"unchecked_number": unchecked})
        except Exception as error:
            abort(404, message=str(error))
        finally:
            session.close()


class UpdateCheckedRequest(Resource):
    def get(self, id):
        unchecked = session.query(UserRequests).get(id)
        unchecked.checked_flag = True
        try:
            session.commit()
            return jsonify({"success": "updated success"})
        except Exception as error:
            session.rollback()
            abort(404, message=str(error))
        finally:
            session.close()


# @app.cache.memoize()
def fetchRawData(yearstart, yearend, monthstart, monthend, timerangerype, basinid, model_t_id):
    # recordeds = session.query(ReachData, RecordDateData).join(RecordDateData,
    #                                                         ReachData.record_month_year_id == RecordDateData.id). \

    if timerangerype == "subset":
        recoreds = session.query(ReachData, RecordDateData). \
            join(
            RecordDateData, ReachData.record_month_year_id == RecordDateData.id). \
            filter(RecordDateData.year >= yearstart). \
            filter(RecordDateData.year <= yearend). \
            filter(RecordDateData.month >= monthstart). \
            filter(RecordDateData.month <= monthend). \
            filter(ReachData.basin_id == basinid). \
            filter(ReachData.model_id == model_t_id).all()
    else:
        recoreds = session.query(ReachData, RecordDateData).join(
            RecordDateData, ReachData.record_month_year_id == RecordDateData.id). \
            filter(((RecordDateData.year == yearstart) & (RecordDateData.month >= monthstart)).self_group() | (
                (RecordDateData.year == yearend) & (RecordDateData.month <= monthend)).self_group() | (
                           (RecordDateData.year > yearstart) & (RecordDateData.year < yearend)).self_group()). \
            filter(ReachData.basin_id == basinid). \
            filter(ReachData.model_id == model_t_id).all()
    observed_raw_csv = 'subbasin,year,month,streamflow,water temperature\n'
    for record in recoreds:
        recstring = str(record.ReachData.rch) + ',' + str(record.RecordDateData.year) + ',' + str(
            record.RecordDateData.month) + ',' + str(record.ReachData.flow_outcms) + ',' + str(
            record.ReachData.wtmpdegc) + '\n'
        observed_raw_csv += recstring
    return observed_raw_csv


def iterable(csv):
    yield str.encode(csv)


def addToZipFiles(zf, csv, subpath, basinname, f_prefix, timerangetype, yearstart, yearend, monthstart,
                  monthend):
    # with zipfile.ZipFile(memory_file, 'a') as zf:
    # with zipstream.ZipFile(memory_file, mode="w", compression=zipstream.ZIP_DEFLATED) as zf:
    raw_obs_file_path = os.path.join('results', basinname, subpath,
                                     f_prefix + "_" + basinname + "_" + timerangetype + '_' + str(
                                         yearstart) + "_" + str(
                                         yearend) + "_" + str(monthstart) + "_" + str(
                                         monthend) + ".csv").replace("\\", "/")
    # data = zipfile.ZipInfo(raw_obs_file_path)
    # data.date_time = time.localtime(time.time())[:6]
    # data.compress_type = zipfile.ZIP_DEFLATED
    # zf.write("G:\\hydroclim_data\\test.csv", raw_obs_file_path)
    zf.write_iter(raw_obs_file_path, iterable(csv))


# @cache.memoize()
def fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype, basinid, model_id, isAVG, isMAX, isMIN,
                      isSD, isVar):
    #     subquery 1: inner join RecordDate and ReachData by reach id; filter data by time, model, basin etc.
    if timerangetype == 'subset':
        subq1 = (
            session.query(ReachData).join(RecordDateData, ReachData.record_month_year_id == RecordDateData.id).filter(
                RecordDateData.year >= yearstart). \
                filter(RecordDateData.year <= yearend). \
                filter(RecordDateData.month >= monthstart). \
                filter(RecordDateData.month <= monthend). \
                filter(ReachData.basin_id == basinid). \
                filter(ReachData.model_id == model_id)).subquery()
    else:
        subq1 = (
            session.query(ReachData).join(RecordDateData, ReachData.record_month_year_id == RecordDateData.id). \
                filter(((RecordDateData.year == yearstart) & (RecordDateData.month >= monthstart)).self_group() | (
                    (RecordDateData.year == yearend) & (RecordDateData.month <= monthend)).self_group() | (
                               (RecordDateData.year > yearstart) & (RecordDateData.year < yearend)).self_group()). \
                filter(ReachData.basin_id == basinid). \
                filter(ReachData.model_id == model_id)).subquery()
    #    subquery 2: get all statistics value of temp and flow
    qry = (session.query(
        subq1.c.rch, func.max(subq1.c.flow_outcms).label("max_flow_outcms"),
        func.min(subq1.c.flow_outcms).label("min_flow_outcms"),
        func.avg(subq1.c.flow_outcms).label("avg_flow_outcms"),
        func.stddev(subq1.c.flow_outcms).label("std_flow_outcms"),
        func.variance(subq1.c.flow_outcms).label("var_flow_outcms"),
        func.max(subq1.c.wtmpdegc).label("max_wtmpdegc"),
        func.min(subq1.c.wtmpdegc).label("min_wtmpdegc"),
        func.avg(subq1.c.wtmpdegc).label("avg_wtmpdegc"),
        func.stddev(subq1.c.wtmpdegc).label("std_wtmpdegc"),
        func.variance(subq1.c.wtmpdegc).label("var_wtmpdegc"))
           .group_by(subq1.c.rch).order_by(asc(subq1.c.rch))).all()

    # qry = (session.query(ReachData).join(RecordDateData, ReachData.record_month_year_id == RecordDateData.id))
    csv = 'subbasin,'
    csv += ('avg_streamflow, avg_water_temperature,') if isAVG else ''
    csv += ('max_streamflow,max_water_temperature,') if isMAX else ''
    csv += ('min_streamflow,min_water_temperature,') if isMIN else ''
    csv += ('std_streamflow,std_water_temperature') if isSD else ''
    csv += ('var_streamflow,var_water_temperature,') if isVar else ''
    csv += '\n'
    # qry = list(set(qry))
    for record in qry:
        recstring = str(record.rch) + ','
        recstring += str(record.avg_flow_outcms) + ',' + (str(record.avg_wtmpdegc) + ',') if isAVG else ''
        recstring += str(record.max_flow_outcms) + ',' + (str(record.max_wtmpdegc) + ',') if isMAX else ''
        recstring += str(record.min_flow_outcms) + ',' + (str(record.min_wtmpdegc) + ',') if isMIN else ''
        recstring += str(record.std_flow_outcms) + ',' + (str(record.std_wtmpdegc) + ',') if isSD else ''
        recstring += str(record.var_flow_outcms) + ',' + (str(record.var_wtmpdegc) + ',') if isVar else ''
        recstring += '\n'
        csv += recstring

    return csv


class ReachDataResource(Resource):
    """
    Return csv of reaches temp&flow information
    @url: /reachdata
    @method: GET
    @return: csv of temp&flow information
    @return-type: csv
    @raise keyError: raises an exception
    """
    parser.add_argument('timerangetype', type=int)
    parser.add_argument('model_id', type=int)
    parser.add_argument('isobserved', type=str)
    parser.add_argument('isRCP45', type=str)
    parser.add_argument('isRCP85', type=str)
    parser.add_argument('rcp45', type=str)
    parser.add_argument('rcp85', type=str)

    def get(self):
        args = parser.parse_args()
        basinid = args['basin_id']
        yearstart = args['yearstart']
        yearend = args['yearend']
        monthstart = args['monthstart']
        monthend = args['monthend']
        timerangetype = args['timerangetype']
        model_id = 0 if args['model_id'] == 0 else args['model_id']
        isobserved = True if args['isobserved'] == 'off' else False
        # israw = args['israw'] # if no, output statics result
        # recoreds = session.query(ReachData,RecordDateData).join( RecordDateData,ReachData.record_month_year_id == RecordDateData.id).\
        recoreds = session.query(functions.ST_Transform(Reach.geom, 4326), Reach.OBJECTID, Reach.ARCID,
                                 Reach.Shape_Leng,
                                 ReachData, RecordDateData).join(ReachData, Reach.id == ReachData.rch).join(
            RecordDateData, ReachData.record_month_year_id == RecordDateData.id). \
            filter(RecordDateData.year >= yearstart). \
            filter(RecordDateData.year <= yearend). \
            filter(RecordDateData.month >= monthstart). \
            filter(RecordDateData.month <= monthend). \
            filter(ReachData.basin_id == basinid). \
            filter(ReachData.model_id == model_id).all()
        # filter(ReachData.is_observed == isobserved).all()

        csv = 'Id,rch,flow_outcms,wtmpdegc,year,month\n'
        for record in recoreds:
            recstring = str(record.ReachData.Id) + ',' + str(record.ReachData.rch) + ',' + str(
                record.ReachData.flow_outcms) + ',' + str(record.ReachData.wtmpdegc) + ',' + str(
                record.RecordDateData.year) + ',' + str(record.RecordDateData.month) + '\n'
            csv += recstring
        return Response(
            csv,
            mimetype="text/csv",
            headers={"Content-disposition":
                         "attachment; filename=myplot.csv"})
