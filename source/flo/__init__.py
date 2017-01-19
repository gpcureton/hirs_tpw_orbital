from datetime import timedelta
from glob import glob
import shutil
import os,sys

from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog
from flo.ingest import IngestCatalog

from flo.sw.hirs import HIRS
from flo.sw.hirs_ctp_orbital import HIRS_CTP_ORBITAL
from flo.sw.hirs.delta import delta_catalog

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)

ingest_catalog = IngestCatalog('PEATE')
SPC = StoredProductCatalog()


class HIRS_TPW_ORBITAL(Computation):

    parameters = parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version',
                               'ctp_version', 'tpw_version']
    outputs = ['out']

    def build_task(self, context, task):
        LOG.debug("Running build_task()") # GPC
        LOG.debug("context:  {}".format(context)) # GPC
        LOG.debug("Initial task.inputs:  {}".format(task.inputs)) # GPC

        # HIRS L1B Input
        hirs_context = context.copy()
        [hirs_context.pop(k) for k in ['collo_version', 'csrb_version', 'ctp_version', 'tpw_version']]
        LOG.debug("hirs_context:  {}".format(hirs_context)) # GPC
        #task.input('HIR1B', HIRS().dataset('out').product(hirs_context))
        if SPC.exists(HIRS().dataset('out').product(hirs_context)):
            task.input('HIR1B', HIRS().dataset('out').product(hirs_context))
        else:
            LOG.warn("HIRS granule {} is not in the StoredProductCatalog.".
                    format(hirs_context['granule']))
            task.inputs = {}
            return

        # CTP Orbital Input
        ctp_context = context.copy()
        ctp_context.pop('tpw_version')
        LOG.debug("ctp_context:  {}".format(ctp_context)) # GPC
        task.input('CTPO', HIRS_CTP_ORBITAL().dataset('out').product(ctp_context))

        # CFSR Input
        cfsr_granule = round_datetime(context['granule'], timedelta(hours=6))
        LOG.debug("cfsr_granule:  {}".format(cfsr_granule)) # GPC
        cfsr_file = self.get_cfsr(cfsr_granule)
        task.input('CFSR', cfsr_file)
        #task.input('CFSR', delta_catalog.file('ancillary', 'NONE', 'CFSR', cfsr_granule))

        LOG.debug("Final task.inputs...") # GPC
        for task_key in task.inputs.keys():
            LOG.debug("\t{}: {}".format(task_key,task.inputs[task_key])) # GPC

        LOG.debug("Exiting build_task()...") # GPC


    def run_task(self, inputs, context):
        LOG.debug("Running run_task()") # GPC

        # No shift for NOAA-8
        if context['sat']=='noaa-08':
            shifted_FM_opt = 1
        else:
            shifted_FM_opt = 2

        # Inputs
        inputs = symlink_inputs_to_working_dir(inputs)

        # Directories
        package_root = os.path.join(self.package_root, context['tpw_version'])
        lib_dir = os.path.join(package_root, 'lib')

        LOG.debug("inputs :  {}".format(inputs)) # GPC

        # Output Name
        output = 'tpw.orbital.hirs.{}.{}.{}.ssec.nc'.format(context['sat'], inputs['HIR1B'][12:30],
                                                            context['tpw_version'])
        LOG.debug("output :  {}".format(output)) # GPC

        # Create links to static input files
        try:
            os.symlink(os.path.join(package_root, 'coeffs/hirscbnd_orig.dat'), 'hirscbnd_orig.dat')
            os.symlink(os.path.join(package_root, 'coeffs/hirscbnd_shft.dat'), 'hirscbnd_shft.dat')
            shift_file = self.sat_name_to_coeff(context['sat'])
            os.symlink(os.path.join(package_root, 'coeffs/{}'.format(shift_file)), shift_file)
        except Exception, err :
            LOG.error("{}.".format(err))

        # Generating CFSR Binaries
        cfsr_bin_files = self.generate_cfsr_bin(package_root, lib_dir)
        LOG.debug("cfsr_bin_files :  {}".format(cfsr_bin_files)) # GPC

        # Running TPW Orbital
        cmd = os.path.join(package_root, 'bin/hirs_regrtvl_main_cdf.exe')
        cmd += ' {} {} {}.bin'.format(inputs['HIR1B'], inputs['CTPO'], inputs['CFSR'])
        cmd += ' {} {}.qc {}'.format(output, output, shifted_FM_opt)

        LOG.debug(cmd)
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}


    def get_cfsr(self,cfsr_granule):

        num_cfsr_files = 0

        # Search for the old style pgbhnl.gdas.*.grb2 file from the PEATE
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve pgbhnl.gdas.*.grb2 CFSR files from PEATE...") # GPC
            try:
                cfsr_file = ingest_catalog.file('CFSR_PGRBHANL',cfsr_granule)
                num_cfsr_files = len(cfsr_file)
                if num_cfsr_files != 0:
                    LOG.debug("\tpgbhnl.gdas.*.grb2 CFSR_PGRBHANL files from PEATE : {}".format(cfsr_file)) # GPC
            except Exception, err :
                #LOG.error("{}.".format(err))
                LOG.debug("\tRetrieval of pgbhnl.gdas.*.grb2 CFSR file from PEATE failed") # GPC

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2 file from PEATE
        if num_cfsr_files == 0:
            LOG.debug("Trying to retrieve cdas1.*.t*z.pgrbhanl.grib2 CFSR file from PEATE...") # GPC
            try:
                cfsr_file = ingest_catalog.file('CFSV2_PGRBHANL',cfsr_granule)
                num_cfsr_files = len(cfsr_file)
                if num_cfsr_files != 0:
                    LOG.debug("\tcdas1.*.t*z.pgrbhanl.grib2 CFSV2_PGRBHANL file from PEATE : {}".format(cfsr_file)) # GPC
            except Exception, err :
                #LOG.error("{}.".format(err))
                LOG.debug("\tRetrieval of cdas1.*.t*z.pgrbhanl.grib2 CFSR file from PEATE failed") # GPC

        if num_cfsr_files == 0:
            raise WorkflowNotReady('No CSFR data exists for context {}'.format(cfsr_granule))

        # Search for the old style pgbhnl.gdas.*.grb2 file from the file list
        #num_cfsr_files = 0
        #if num_cfsr_files == 0:
            #LOG.debug("Trying to retrieve pgbhnl.gdas.*.grb2 CFSR file from DELTA...") # GPC
            #try:
                #cfsr_file = delta_catalog.file('ancillary', 'NONE', 'CFSR', cfsr_granule)
                #num_cfsr_files = len(cfsr_file)
                #if num_cfsr_files != 0:
                    #LOG.debug("pgbhnl.gdas.*.grb2 CFSR file from DELTA : {}\n".format(cfsr_file)) # GPC
            #except Exception, err :
                #LOG.error("{}.".format(err))
                #LOG.warn("\tRetrieval of pgbhnl.gdas.*.grb2 CFSR file from DELTA failed\n") # GPC

        return cfsr_file


    def generate_cfsr_bin(self, package_root, lib_dir):

        os.mkdir('scripts')
        shutil.copy(os.path.join(package_root, 'bin/wgrib2'), './scripts')

        # Search for the old style pgbhnl.gdas.*.grb2 files
        LOG.debug("Searching for pgbhnl.gdas.*.grb2 ...")
        files = glob('pgbhnl.gdas.*.grb2')
        LOG.debug("... found {}".format(files))

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2
        if len(files)==0:
            LOG.debug("Searching for cdas1.*.pgrbhanl.grib2 ...")
            files = glob('cdas1.*.pgrbhanl.grib2')
            LOG.debug("... found {}".format(files))

        LOG.debug("CFSR files: {}".format(files)) # GPC

        new_cfsr_files = []
        for file in files:
            cmd = os.path.join(package_root, 'bin/extract_ncep_cfsr_psfc.csh')
            cmd += ' ./ {} {}.bin'.format(file, file)

            LOG.debug(cmd)

            try:
                check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))
                new_cfsr_files.append('{}.bin'.format(file))
            except:
                pass

        return new_cfsr_files


    def sat_name_to_coeff(self, sat):

        coeff_file = {'metop-a': 'RC_CRTM21_ODPS_hirs4_moa_shift.nc',
                      'metop-b': 'RC_CRTM21_ODPS_hirs4_mob_shift.nc',
                      'noaa-06': 'RC_CRTM21_ODPS_hirs2_n06_shift.nc',
                      'noaa-07': 'RC_CRTM21_ODPS_hirs2_n07_shift.nc',
                      'noaa-08': 'RC_CRTM21_ODPS_hirs2_n08_noshift.nc',
                      'noaa-09': 'RC_CRTM21_ODPS_hirs2_n09_shift.nc',
                      'noaa-10': 'RC_CRTM21_ODPS_hirs2_n10_shift.nc',
                      'noaa-11': 'RC_CRTM21_ODPS_hirs2_n11_shift.nc',
                      'noaa-12': 'RC_CRTM21_ODPS_hirs2_n12_shift.nc',
                      'noaa-14': 'RC_CRTM21_ODPS_hirs2_n14_shift.nc',
                      'noaa-15': 'RC_CRTM21_ODPS_hirs3_n15_shift.nc',
                      'noaa-16': 'RC_CRTM21_ODPS_hirs3_n16_shift.nc',
                      'noaa-17': 'RC_CRTM21_ODPS_hirs3_n17_shift.nc',
                      'noaa-18': 'RC_CRTM21_ODPS_hirs4_n18_shift.nc',
                      'noaa-19': 'RC_CRTM21_ODPS_hirs4_n19_shift.nc'}

        return coeff_file[sat]


    def find_contexts(self, sat, hirs_version, collo_version, csrb_version, ctp_version,
                      tpw_version, time_interval):

        files = delta_catalog.files('hirs', sat, 'HIR1B', time_interval)
        return [{'granule': file.data_interval.left,
                 'sat': sat,
                 'hirs_version': hirs_version,
                 'collo_version': collo_version,
                 'csrb_version': csrb_version,
                 'ctp_version': ctp_version,
                 'tpw_version': tpw_version}
                for file in files
                if file.data_interval.left >= time_interval.left]

    def context_path(self, context, output):

        return os.path.join('HIRS',
                            '{}/{}'.format(context['sat'], context['granule'].year),
                            'TPW_ORBITAL')
