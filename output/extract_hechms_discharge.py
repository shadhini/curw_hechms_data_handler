#!/home/uwcc-admin/hechms_data_handler/venv/bin/python3
import json
import traceback
import sys
import os
import getopt
from datetime import datetime, timedelta
import re
import csv
import time

from db_adapter.logger import logger
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.base import get_Pool
from db_adapter.curw_fcst.source import get_source_id, get_source_parameters
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id, UnitType
from db_adapter.curw_fcst.station import get_hechms_stations
from db_adapter.curw_fcst.timeseries import Timeseries

ROOT_DIRECTORY = '/home/uwcc-admin/hechms_data_handler'
hechms_stations = {}


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][2:]

    return data


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute] != ""):
        return config[attribute]
    elif compulsory:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        logger.error("{} not specified in config file.".format(attribute))
        return None


def getUTCOffset(utcOffset, default=False):
    """
    Get timedelta instance of given UTC offset string.
    E.g. Given UTC offset string '+05:30' will return
    datetime.timedelta(hours=5, minutes=30))

    :param string utcOffset: UTC offset in format of [+/1][HH]:[MM]
    :param boolean default: If True then return 00:00 time offset on invalid format.
    Otherwise return False on invalid format.
    """
    offset_pattern = re.compile("[+-]\d\d:\d\d")
    match = offset_pattern.match(utcOffset)
    if match:
        utcOffset = match.group()
    else:
        if default:
            print("UTC_OFFSET :", utcOffset, " not in correct format. Using +00:00")
            return timedelta()
        else:
            return False

    if utcOffset[0] == "+":  # If timestamp in positive zone, add it to current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=int(offset_str[0]), minutes=int(offset_str[1]))
    if utcOffset[0] == "-":  # If timestamp in negative zone, deduct it from current time
        offset_str = utcOffset[1:].split(':')
        return timedelta(hours=-1 * int(offset_str[0]), minutes=-1 * int(offset_str[1]))


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def get_file_last_modified_time(file_path):
    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


def check_time_format(time):
    try:
        time = datetime.strptime(time, COMMON_DATE_TIME_FORMAT)

        if time.strftime('%S') != '00':
            print("Seconds should be always 00")
            exit(1)
        if time.strftime('%M') not in ('00', '05', '10', '15', '20', '25', '30', '35', '40', '45', '50', '55'):
            print("Minutes should be always multiples of 5")
            exit(1)

        return time
    except Exception:
        print("Time {} is not in proper format".format(time))
        exit(1)


def usage():
    usageText = """
    --------------------------------------------------------------------
    Extract HecHMS discharge output to the curw_fcst database.
    --------------------------------------------------------------------

    Usage: .\output\extract_hechms_discharge.py [-m hechms_XXX] [-s "YYYY-MM-DD HH:MM:SS"] [-r "YYYY-MM-DD HH:MM:SS"] [-t XXX]
    [-d "C:\\udp_150\\2019-09-23"]

    -h  --help          Show usage
    -m  --model         HecHMS model (e.g. hechms_single, hechms_distributed).
    -s  --ts_start_time Timeseries start time (e.g: "2019-06-05 23:00:00").
    -r  --run_time      Run time (e.g: "2019-06-05 23:00:00").
    -d  --dir           Output directory (e.g. "C:\\udp_150\\2019-09-23"); 
                        Directory where output file (DailyDischarge.csv) is located.
    -t  --sim_tag       Simulation tag
    """
    print(usageText)


def extractForecastTimeseries(timeseries, extract_date, extract_time, by_day=False):
    """
    Extracted timeseries upward from given date and time
    E.g. Consider timeseries 2017-09-01 to 2017-09-03
    date: 2017-09-01 and time: 14:00:00 will extract a timeseries which contains
    values that timestamp onwards
    """
    print('LibForecastTimeseries:: extractForecastTimeseries')
    if by_day:
        extract_date_time = datetime.strptime(extract_date, '%Y-%m-%d')
    else:
        extract_date_time = datetime.strptime('%s %s' % (extract_date, extract_time), '%Y-%m-%d %H:%M:%S')

    is_date_time = isinstance(timeseries[0][0], datetime)
    new_timeseries = []
    for i, tt in enumerate(timeseries):
        tt_date_time = tt[0] if is_date_time else datetime.strptime(tt[0], '%Y-%m-%d %H:%M:%S')
        if tt_date_time >= extract_date_time:
            new_timeseries = timeseries[i:]
            break

    return new_timeseries


def save_forecast_timeseries_to_db(pool, timeseries, run_date, run_time, tms_meta, fgt):
    print('EXTRACTFLO2DWATERLEVEL:: save_forecast_timeseries >>', tms_meta)

    # {
    #         'tms_id'     : '',
    #         'sim_tag'    : '',
    #         'station_id' : '',
    #         'source_id'  : '',
    #         'unit_id'    : '',
    #         'variable_id': ''
    #         }

    date_time = datetime.strptime('%s %s' % (run_date, run_time), COMMON_DATE_TIME_FORMAT)

    forecast_timeseries = []

    if 'utcOffset' in tms_meta:  # If there is an offset, shift by offset before proceed
        print('Shift by utcOffset:', tms_meta['utcOffset'].resolution)
        # Convert date time with offset
        date_time = date_time + tms_meta['utcOffset']
        run_date = date_time.strftime('%Y-%m-%d')
        run_time = date_time.strftime('%H:%M:%S')
        for item in timeseries:
            forecast_timeseries.append(
                [datetime.strptime(item[0], COMMON_DATE_TIME_FORMAT) + tms_meta['utcOffset'], item[1]])

        forecast_timeseries = extractForecastTimeseries(timeseries=forecast_timeseries, extract_date=run_date,
                                                        extract_time=run_time)
    else:
        forecast_timeseries = extractForecastTimeseries(timeseries=timeseries, extract_date=run_date,
                                                        extract_time=run_time)

    try:

        TS = Timeseries(pool=pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=tms_meta)

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=tms_meta)
            tms_meta['tms_id'] = tms_id
            TS.insert_run(run_meta=tms_meta)
            TS.update_start_date(id_=tms_id, start_date=fgt)

        TS.insert_data(timeseries=forecast_timeseries, tms_id=tms_id, fgt=fgt, upsert=True)
        TS.update_latest_fgt(id_=tms_id, fgt=fgt)

    except Exception:
        logger.error("Exception occurred while pushing data to the curw_fcst database")
        traceback.print_exc()


if __name__ == "__main__":

    """
    hechms_distributed_config.json 
    {
    
      "utc_offset": "",

      "sim_tag": "hourly_run",

      "model": "HECHMS",
      "version": "single",

      "unit": "m3/s",
      "unit_type": "Instantaneous",

      "variable": "Discharge",

      "station_name": "Hanwella"
    }

    """
    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        in_ts_start_time = None
        in_run_time = None
        hechms_model = None
        output_dir = "/mnt/disks/curwsl_nfs/distributed_hechms"
        sim_tag = None
        file_name = "DailyDischarge.csv"

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:s:r:d:t:",
                                       ["help", "model=", "ts_start_time=", "run_time=", "dir=", "sim_tag="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--model"):
                hechms_model = arg.strip()
            elif opt in ("-s", "--ts_start_time"):
                in_ts_start_time = arg.strip()
            elif opt in ("-r", "--run_time"):
                in_run_time = arg.strip()
            elif opt in ("-d", "--dir"):
                output_dir = arg.strip()
            elif opt in ("-t", "--sim_tag"):
                sim_tag = arg.strip()

        config = json.loads(open(os.path.join(ROOT_DIRECTORY, 'output', '{}_config.json'.format(hechms_model))).read())

        if in_ts_start_time is None:
            print("Please specify the time series start time.")
            usage()
            exit(1)
        if hechms_model is None:
            print("Please specify hechms model.")
            usage()
            exit(1)
        if output_dir is None:
            print("Please specify hechms output directory.")
            usage()
            exit(1)

        if not os.path.isdir(output_dir):
            print("Given output directory doesn't exist")
            exit(1)
        if hechms_model not in ("hechms_distributed", "hechms_single"):
            print("Flo2d model should be either \"hechms_distributed\" or \"hechms_single\"")
            exit(1)

        if in_run_time is None:
            in_run_time = in_ts_start_time

        in_ts_start_time = check_time_format(in_ts_start_time)
        in_run_time = check_time_format(in_run_time)

        run_date = in_run_time.strftime("%Y-%m-%d")
        run_time = in_run_time.strftime("%H:%M:%S")

        ts_start_date = in_ts_start_time.strftime("%Y-%m-%d")
        ts_start_time = in_ts_start_time.strftime("%H:%M:%S")

        utc_offset = read_attribute_from_config_file('utc_offset', config, False)
        if utc_offset is None:
            utc_offset = ''

        # sim tag
        if sim_tag is None:
            sim_tag = read_attribute_from_config_file('sim_tag', config, True)

        # source details
        model = read_attribute_from_config_file('model', config, True)
        version = hechms_model.split("_")[1]

        # unit details
        unit = read_attribute_from_config_file('unit', config, True)
        unit_type = UnitType.getType(read_attribute_from_config_file('unit_type', config, True))

        # variable details
        variable = read_attribute_from_config_file('variable', config, True)

        output_file_path = os.path.join(output_dir, file_name)

        if not os.path.exists(output_file_path):
            msg = 'no file :: {}'.format(output_file_path)
            logger.warning(msg)
            print(msg)
            exit(1)

        fgt = get_file_last_modified_time(output_file_path)

        timeseries = read_csv(output_file_path)

        pool = get_Pool(host=con_params.CURW_FCST_HOST, port=con_params.CURW_FCST_PORT, db=con_params.CURW_FCST_DATABASE,
                        user=con_params.CURW_FCST_USERNAME, password=con_params.CURW_FCST_PASSWORD)

        hechms_stations = get_hechms_stations(pool=pool)

        source_id = get_source_id(pool=pool, model=model, version=version)
        source_params = json.loads(get_source_parameters(pool=pool, model=model, version=version))
        station_name = source_params["EXTRACT_STATION"]

        station_id = hechms_stations.get(station_name)[0]
        lat = str(hechms_stations.get(station_name)[1])
        lon = str(hechms_stations.get(station_name)[2])

        variable_id = get_variable_id(pool=pool, variable=variable)

        unit_id = get_unit_id(pool=pool, unit=unit, unit_type=unit_type)

        tms_meta = {
            'sim_tag': sim_tag,
            'model': model,
            'version': version,
            'variable': variable,
            'unit': unit,
            'unit_type': unit_type.value,
            'latitude': lat,
            'longitude': lon,
            'station_id': station_id,
            'source_id': source_id,
            'variable_id': variable_id,
            'unit_id': unit_id
        }

        utcOffset = getUTCOffset(utc_offset, default=True)

        if utcOffset != timedelta():
            tms_meta['utcOffset'] = utcOffset

        # Push timeseries to database
        save_forecast_timeseries_to_db(pool=pool, timeseries=timeseries,
                                       run_date=run_date, run_time=run_time, tms_meta=tms_meta, fgt=fgt)

    except Exception as e:
        logger.error('JSON config data loading error.')
        print('JSON config data loading error.')
        traceback.print_exc()
    finally:
        logger.info("Process finished.")
        print("Process finished.")
