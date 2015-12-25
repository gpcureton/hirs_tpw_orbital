import os,sys
import time
from datetime import datetime
from flo.time import TimeInterval
from flo.ui import local_prepare, local_execute

from flo.sw.hirs import HIRS
from flo.sw.hirs_ctp_orbital import HIRS_CTP_ORBITAL
from flo.sw.hirs_tpw_orbital import HIRS_TPW_ORBITAL

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)


# Set up the logging
console_logFormat = '%(asctime)s : (%(levelname)s):%(filename)s:%(funcName)s:%(lineno)d:  %(message)s'
dateFormat = '%Y-%m-%d %H:%M:%S'
levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
logging.basicConfig(level = levels[3], 
        format = console_logFormat, 
        datefmt = dateFormat)


def local_execute_example(sat, hirs_version, collo_version, csrb_version, ctp_version, tpw_version, granule):
    try:
        LOG.info("Running local_prepare(HIRS_TPW_ORBITAL(),...)") # GPC
        local_prepare(HIRS_TPW_ORBITAL(), { 
                                      'sat': sat, 
                                      'hirs_version': hirs_version, 
                                      'collo_version': collo_version, 
                                      'csrb_version': csrb_version,
                                      'ctp_version': ctp_version,
                                      'tpw_version': tpw_version,
                                      'granule': granule
                                      },
                      download_only=[HIRS(), HIRS_CTP_ORBITAL()]
                     )
        LOG.info("Running local_execute(HIRS_TPW_ORBITAL(),...)") # GPC
        local_execute(HIRS_TPW_ORBITAL(), {
                                      'sat': sat, 
                                      'hirs_version': hirs_version, 
                                      'collo_version': collo_version, 
                                      'csrb_version': csrb_version,
                                      'ctp_version': ctp_version,
                                      'tpw_version': tpw_version,
                                      'granule': granule
                                      }
                                      )
    except Exception, err :
        LOG.error("{}.".format(err))
        LOG.debug(traceback.format_exc())



sat = 'metop-b'
hirs_version  = 'v20151014'
collo_version = 'v20151014'
csrb_version  = 'v20150915'
ctp_version = 'v20150915'
tpw_version = 'v20150915'
granule = datetime(2014, 1, 15, 0, 0)

#local_execute_example(sat, hirs_version, collo_version, csrb_version, ctp_version, tpw_version, granule)
