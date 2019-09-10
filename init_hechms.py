import traceback
import json

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.logger import logger

from db_adapter.curw_fcst.source import get_source_id, add_source
from db_adapter.curw_fcst.variable import get_variable_id, add_variable
from db_adapter.curw_fcst.unit import get_unit_id, add_unit, UnitType
from db_adapter.curw_fcst.station import add_station, StationEnum
from db_adapter.constants import CURW_FCST_HOST, CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_DATABASE
from db_adapter.curw_sim.constants import FLO2D_250, FLO2D_150

from db_adapter.csv_utils import read_csv


if __name__=="__main__":

    try:

        ####################################
        # Initialize parameters for HECHMS #
        ####################################

        # source details
        model = 'HECHMS'
        version = 'single'

        # unit details
        unit = 'm3/s'
        unit_type = UnitType.getType('Instantaneous')

        # variable details
        variable = 'Discharge'

        # station details
        hechms_stations = read_csv('hechms_stations.csv')

        # pool = get_Pool(host=CURW_FCST_HOST, port=CURW_FCST_PORT, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
        #         db=CURW_FCST_DATABASE)

        # ########
        # # test
        # ########

        USERNAME = "root"
        PASSWORD = "password"
        HOST = "127.0.0.1"
        PORT = 3306
        DATABASE = "curw_fcst"

        pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)

        add_source(pool=pool, model=model, version=version, parameters=None)
        add_variable(pool=pool, variable=variable)
        add_unit(pool=pool, unit=unit, unit_type=unit_type)

        # add hechms output stations

        for i in range(len(hechms_stations)):
            station_name = hechms_stations[i][0]
            lat = hechms_stations[i][1]
            lon = hechms_stations[i][2]
            add_station(pool=pool, name=station_name, latitude="%.6f" % float(lat), longitude="%.6f" % float(lon),
                    station_type=StationEnum.HECHMS, description="hecHMS output station")

    except Exception:
        logger.info("Initialization process failed.")
        traceback.print_exc()
    finally:
        logger.info("Initialization process finished.")
        destroy_Pool(pool=pool)

