from datetime import datetime
from flo.time import TimeInterval
from flo.sw.hirs_ctp_orbital import HIRS_CTP_ORBITAL
from flo.sw.hirs_tpw_orbital import HIRS_TPW_ORBITAL
from flo.ui import submit_order
from flo.ui import local_prepare, local_execute
import logging
import sys
import time

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)

def _local_execute_example(platform,interval):
    '''
    Run this computation locally
    '''

    hirs_version = 'v20151014'
    collo_version = 'v20140204'
    csrb_version = 'v20150915'
    ctp_version = 'v20150915'
    tpw_version = 'v20150915'

    c = HIRS_TPW_ORBITAL()
    contexts = c.find_contexts(platform, hirs_version, collo_version, csrb_version, ctp_version,
                               tpw_version, interval)
    local_prepare(comp, contexts[0])
    local_execute(comp, contexts[0])


def submit(logger, interval, platform):
    '''
    Submit this computation to the cluster.
    '''

    hirs_version = 'v20151014'
    collo_version = 'v20140204'
    csrb_version = 'v20150915'
    ctp_version = 'v20150915'
    tpw_version = 'v20150915'

    c = HIRS_TPW_ORBITAL()
    contexts = c.find_contexts(platform, hirs_version, collo_version, csrb_version, ctp_version,
                               tpw_version, interval)

    while 1:
        try:
            return submit_order(c, [c.dataset('out')], contexts, (HIRS_CTP_ORBITAL(),))
        except:
            logger.info('Failed submiting jobs.  Sleeping for 5 minutes and submitting again')
            time.sleep(5*60)

# Setup Logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

# Submitting Jobs
for platform in ['metop-a']:
    for interval in [TimeInterval(datetime(2009, 1, 1), datetime(2009, 2, 1))]:
        jobIDRange = submit(logger, interval, platform)

        if len(jobIDRange) > 0:
            logger.info('Submitting hirs_tpw_orbital jobs for {} '.format(platform) +
                        'from {} to {}'.format(interval.left, interval.right))
        else:
            logger.info('No hirs_tpw_orbital jobs for {} '.format(platform) +
                        'from {} to {}'.format(interval.left, interval.right))
