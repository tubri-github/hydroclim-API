#from gevent import monkey
#monkey.patch_all()
import os

from flask import Flask
from flask_restplus import Resource, Namespace, Api
from flask_cors import CORS
from flask_caching import Cache
from Celery.celery import celery,create_app

import resources

from resources import BasininfoResource, BasinListResource

cache = Cache(config = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': '127.0.0.1',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': '',
    'CACHE_REDIS_PASSWORD': ''
})

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
app.app_context().push()
CORS(app)
cache.init_app(app)
celery.conf.update(app.config)
api = Api(app, prefix="/v1", title="Hydroclim", description="Hydroclim  api.")

@api.route('/users')
class UserApi(Resource):
    def get(self):
        return {'user': '1'}

#Basin NameSpace

from resources import BasininfoResource
from resources import BasinListResource
from resources import BasinResource
from resources import ReachResource
from resources import ReachDataResource
from resources import getReachData
from resources import ReachDataZip
from resources import getAllReachData
from resources import UserInfo
from resources import userLogin
from resources import getAllRequestRecords
from resources import GetZip, GetUnCheckedNumber, UpdateCheckedRequest

basin = Namespace("basin")

reach = Namespace("reach")

records = Namespace("records")

basin.add_resource(BasininfoResource,'/basininfo/<string:id>', endpoint = 'basininfo')
basin.add_resource(BasinListResource,'/basinlist', endpoint = 'basinlists')
basin.add_resource(BasinResource, "/basin", endpoint = 'basins')
api.add_namespace(basin)

reach.add_resource(ReachResource, "/reach", endpoint = 'reaches')
#reach.add_resource(ReachDataResource, "/reachdata", endpoint = 'records')

#reach.add_resource(ReachGeoResource, "/reachbyloc/<int:x>/<int:y>", endpoint = 'reaches')
api.add_namespace(reach)

records.add_resource(ReachDataResource, "/reachdata", endpoint = 'records')
records.add_resource(getReachData, "/getreachdata", endpoint = 'reachrecord')
records.add_resource(getAllReachData, "/getallreachdata", endpoint = 'allreachrecord')
records.add_resource(ReachDataZip, "/reachdatazip", endpoint = 'reachrecordzip')
records.add_resource(getAllRequestRecords, "/queries", endpoint ='queries')
records.add_resource(GetZip, "/zip/<string:filename>", endpoint ='getzip')
records.add_resource(GetUnCheckedNumber, "/unchecked", endpoint ='getunchecked')
records.add_resource(UpdateCheckedRequest, "/updateunchecked/<int:id>", endpoint ='updateunchecked')
api.add_namespace(records)

#Model NameSpace

#Reach NameSpace

#Subbasin NameSpace

#User NameSpace
user = Namespace("user")
user.add_resource(UserInfo, "/user", endpoint = 'userinfolist')
user.add_resource(userLogin, "/login", endpoint = 'userlogin')
api.add_namespace(user)


if __name__ == '__main__':
    app.run(debug=True,threaded=True)
    #gevent_server = gevent.pywsgi.WSGIServer(('0.0.0.0', 5000), app)
    #gevent_server.serve_forever()
