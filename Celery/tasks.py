from celery.result import AsyncResult
from flask_mail import Message


from .celery import celery,mail
from resources_data import basinlist, rcp45list, rcp85list
from models import ReachData, RecordDateData, User, UserRequests
from db import session
from sqlalchemy import func, asc
import time, config, os, inspect, json, sys, uuid
import zipfile, zipstream
#from app import cache

def get_kwargs():
    frame = inspect.currentframe().f_back
    keys, _, _, values = inspect.getargvalues(frame)
    kwargs = {}
    for key in keys:
        if key != 'self':
            kwargs[key] = values[key]
    return kwargs


@celery.task(bind=True)
def generateZipFile(self,request_id,curr_user_id, curr_user_name,create_time, yearstart, yearend, monthstart, monthend, timerangetype, basinids, isobserved, isRCP45, isRCP85,
                    model_RCP45_ids, model_RCP85_ids, israwdata, isstastics, isavg, ismax, ismin, isSD, isVa):
    ###STARTS QUERYING AND WRITE ZIP FILES HERE
    ###Create Zip Files, starts IO
    # memory_file = BytesIO()
    # update "status" and "filename" for this user request in db
    arguments = get_kwargs()
    user_record = session.query(UserRequests).get(request_id)
    user_record.task_id = self.request.id
    user_record.arguments = json.dumps(arguments)
    session.commit()
    try:
        zf = zipstream.ZipFile(compression=zipstream.ZIP_DEFLATED)
        readmefileSourcePath = os.path.join(config.SHAPEFILES_PATH, "README.txt").replace("\\", "/");
        readmearcnamePath = os.path.join('results', "README.txt").replace("\\", "/")
        zf.write(readmefileSourcePath, readmearcnamePath, zipfile.ZIP_DEFLATED)
        ### 1. Each Basin
        for basinid in basinids:
            print(basinlist[str(basinid)])
            basinname = str(basinlist[basinid])

            # with zipfile.ZipFile(memory_file, 'a') as zf:

            # with zipstream.ZipFile(memory_file,compression=zipstream.ZIP_DEFLATED ) as zf:

            ### 1. ShapeFiles and README: locate shape file path and add them to archive by .
            shapefileSourcePath = os.path.join(config.SHAPEFILES_PATH, basinname, "Shape.zip").replace("\\", "/");
            arcnamePath = os.path.join('results', basinname, "Shape.zip").replace("\\", "/")
            zf.write(shapefileSourcePath, arcnamePath, zipfile.ZIP_DEFLATED)

            ### 2. Raw data and Stastistics data
            ### (A).if query have observered data
            if isobserved:
                ### Check if historical data sit in date range[1950-1999]
                if int(yearstart) <= 1999:
                    ### (a)if return raw data
                    if israwdata:
                        model_t_id = 0
                        observed_raw_csv = fetchRawData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
                                                        model_t_id)

                        #### add observed raw data to ZIP files
                        # with zipfile.ZipFile(memory_file, 'a') as zf:
                        # with zipstream.ZipFile(memory_file, mode="w", compression=zipstream.ZIP_DEFLATED) as zf:
                        raw_obs_file_path = os.path.join('results', basinname, 'Historical observations/',
                                                         # file path and name EXAMPLE: ../[basinname]/Historical observations/Raw_Historical_[basinname]_subset_1950_1955_02_03.csv
                                                         "Raw_Historical_" + basinname + "_" + timerangetype + '_' + str(
                                                             yearstart) + "_" + str(
                                                             yearend) + "_" + str(monthstart) + "_" + str(
                                                             monthend) + ".csv").replace("\\", "/")
                        # data = zipfile.ZipInfo(raw_obs_file_path)
                        # data.date_time = time.localtime(time.time())[:6]
                        # data.compress_type = zipfile.ZIP_DEFLATED
                        zf.write_iter(raw_obs_file_path, iterable(observed_raw_csv))
                    ##memory_file, csv, subpath, basinname, f_prefix, timerangetype, yearstart, yearend, monthstart, monthend
                    ### (b).if return stastics data
                    if isstastics:
                        stastics_csv = fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
                                                         0, isavg, ismax,
                                                         ismin, isSD, isVa)
                        # file path and name EXAMPLE: ../[basinname]/Historical observations/Raw_Statistics_[basinname]_subset_1950_1955_02_03.csv
                        addToZipFiles(zf, stastics_csv, 'Historical observations/', basinname, 'Statistics_Historical_',
                                      timerangetype,
                                      yearstart, yearend,
                                      monthstart, monthend)
            ### (B). if query have RCP 45 models data
            if isRCP45:
                ### (a)if return raw data
                if israwdata:
                    for model_id in model_RCP45_ids:
                        model_name = rcp45list[str(model_id)]
                        model_raw_csv = fetchRawData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
                                                     model_id)
                        # file path and name EXAMPLE: ../[basinname]/RCP45/1_access1-0-rcp85/raw_1_access1-0-rcp85_[basinname]_subset_1950_1955_02_03.csv
                        addToZipFiles(zf, model_raw_csv, 'RCP45/' + model_name, basinname, 'Raw_' + model_name,
                                      timerangetype, yearstart,
                                      yearend, monthstart, monthend)
                ### (b).if return stastics data
                if isstastics:
                    for model_id in model_RCP45_ids:
                        model_name = rcp45list[str(model_id)]
                        model_statsics_csv = fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype,
                                                               basinid,
                                                               model_id, isavg, ismax, ismin, isSD, isVa)
                        # file path and name EXAMPLE: ../[basinname]/RCP45/1_access1-0-rcp45/stastics_1_access1-0-rcp45_[basinname]_subset_1950_1955_02_03.csv
                        addToZipFiles(zf, model_statsics_csv, 'RCP45/' + model_name, basinname,
                                      'Statistics_' + model_name,
                                      timerangetype, yearstart,
                                      yearend, monthstart, monthend)
            if isRCP85:
                ### (a)if return raw data
                if israwdata:
                    for model_id in model_RCP85_ids:
                        model_name = rcp85list[str(model_id)]
                        model_raw_csv = fetchRawData(yearstart, yearend, monthstart, monthend, timerangetype, basinid,
                                                     model_id)
                        # file path and name EXAMPLE: ../[basinname]/RCP85/1_access1-0-rcp85/raw_1_access1-0-rcp85_[basinname]_subset_1950_1955_02_03.csv
                        addToZipFiles(zf, model_raw_csv, 'RCP85/' + model_name, basinname, 'Raw_' + model_name,
                                      timerangetype, yearstart,
                                      yearend, monthstart, monthend)
                ### (b).if return stastics data
                if isstastics:
                    for model_id in model_RCP85_ids:
                        model_name = rcp85list[str(model_id)]
                        model_statsics_csv = fetchStasticsData(yearstart, yearend, monthstart, monthend, timerangetype,
                                                               basinid,
                                                               model_id, isavg, ismax, ismin, isSD, isVa)
                        # file path and name EXAMPLE: ../[basinname]/RCP85/1_access1-0-rcp85/stastics_1_access1-0-rcp85_[basinname]_subset_1950_1955_02_03.csv
                        addToZipFiles(zf, model_statsics_csv, 'RCP85/' + model_name, basinname,
                                      'Statistics_' + model_name,
                                      timerangetype, yearstart,
                                      yearend, monthstart, monthend)
        #zf.filename ="Hydroclim_data_" + curr_user_name + create_time + '_' + uuid.uuid5(uuid.NAMESPACE_URL,'hydroclim.org').hex + ".zip"
        zf.filename ="Hydroclim_data_" + curr_user_name + create_time + ".zip"
        with open(os.path.join(config.ZIPFILES_PATH,zf.filename), 'wb') as f:
            for data in zf:
                f.write(data)


        user_record.status = "SUCCESS"
        user_record.file_name = zf.filename
        user_record.checked_flag = False
        session.commit()
        #send_async_email(Message("EmailTest", recipients=[curr_user_name]))
        with mail.app.app_context():
            msg = Message("Hydroclim Data query complete!", recipients=[curr_user_name])
            msg.body= "Hi," \
                      "Your Hydroclim data is ready. Please download the zip file here: https://www.hydroclim.org/api/v1/records/zip/" + zf.filename + "\n" \
                                                                                                                                                                   "Thanks!"
            mail.send(msg)
    except Exception as error:
        session.rollback()
        raise self.retry(exc=error, countdown=3, max_retries=2)

    return os.path.join(zf.filename)

@celery.task
def error_handler(uuid):
    result = AsyncResult(uuid)
    user_records = session.query(UserRequests).filter_by(task_id=uuid).all()
    if len(user_records) > 0:
        user_record =user_records[0]
        user_record.status = "FAILURE"
        user_record.error_message = str(result.info)
        session.commit()
    #if len = 0, the record wasn't created by task. do it later...

#@cache.memoize()
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
            record.RecordDateData.month) + ',' + ('NoData' if (record.ReachData.flow_outcms is None) else str(record.ReachData.flow_outcms)) + ',' + ('NoData' if (record.ReachData.wtmpdegc is None) else str(
            record.ReachData.wtmpdegc)) + '\n'
        observed_raw_csv += recstring
    return observed_raw_csv


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


def iterable(csv):
    yield str.encode(csv)

#@cache.memoize()
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
    csv += ('std_streamflow,std_water_temperature,') if isSD else ''
    csv += ('var_streamflow,var_water_temperature,') if isVar else ''
    csv += '\n'
    # qry = list(set(qry))
    for record in qry:
        recstring = str(record.rch) + ','
        recstring += ('NoData' if (record.avg_flow_outcms is None) else str(record.avg_flow_outcms)) + ',' + ('NoData' if (record.avg_wtmpdegc is None) else str(record.avg_wtmpdegc)) + ',' if isAVG else ''
        recstring += ('NoData' if (record.max_flow_outcms is None) else str(record.max_flow_outcms)) + ',' + ('NoData' if (record.max_wtmpdegc is None) else str(record.max_wtmpdegc)) + ',' if isMAX else ''
        recstring += ('NoData' if (record.min_flow_outcms is None) else str(record.min_flow_outcms)) + ',' + ('NoData' if (record.min_wtmpdegc is None) else str(record.min_wtmpdegc)) + ',' if isMIN else ''
        recstring += ('NoData' if (record.std_flow_outcms is None) else str(record.std_flow_outcms)) + ',' + ('NoData' if (record.std_wtmpdegc is None) else str(record.std_wtmpdegc)) + ',' if isSD else ''
        recstring += ('NoData' if (record.var_flow_outcms is None) else str(record.var_flow_outcms)) + ',' + ('NoData' if (record.var_wtmpdegc is None) else str(record.var_wtmpdegc)) + ',' if isVar else ''
        recstring += '\n'
        csv += recstring

    return csv
