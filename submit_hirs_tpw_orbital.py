from datetime import datetime
from flo.time import TimeInterval
from flo.sw.hirs_ctp_orbital import HIRS_CTP_ORBITAL
from flo.sw.hirs_tpw_orbital import HIRS_TPW_ORBITAL
from flo.ui import submit_order
import logging
import sys
import time


def submit(logger, interval, platform):

    hirs_version = 'v20140204'
    collo_version = 'v20140204'
    csrb_version = 'v20140204'
    ctp_version = 'v20140204'
    tpw_version = 'v20150212'

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