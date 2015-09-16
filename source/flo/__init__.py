from datetime import timedelta
from glob import glob
import shutil
import os
from flo.computation import Computation
from flo.subprocess import check_call
from flo.time import round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.sw.hirs import HIRS
from flo.sw.hirs_ctp_orbital import HIRS_CTP_ORBITAL
from flo.sw.hirs.delta import delta_catalog

# every module should have a LOG object
import logging, traceback
LOG = logging.getLogger(__file__)


class HIRS_TPW_ORBITAL(Computation):

    parameters = parameters = ['granule', 'sat', 'hirs_version', 'collo_version', 'csrb_version',
                               'ctp_version', 'tpw_version']
    outputs = ['out']

    def build_task(self, context, task):

        # HIRS L1B Input
        hirs_context = context.copy()
        [hirs_context.pop(k)
         for k in ['collo_version', 'csrb_version', 'ctp_version', 'tpw_version']]
        task.input('HIR1B', HIRS().dataset('out').product(hirs_context))

        # CTP Orbital Input
        ctp_context = context.copy()
        ctp_context.pop('tpw_version')
        task.input('CTPO', HIRS_CTP_ORBITAL().dataset('out').product(ctp_context))

        # CFSR Input
        cfsr_granule = round_datetime(context['granule'], timedelta(hours=6))
        task.input('CFSR', delta_catalog.file('ancillary', 'NONE', 'CFSR', cfsr_granule))

    def run_task(self, inputs, context):

        shifted_FM_opt = 2

        # Inputs
        inputs = symlink_inputs_to_working_dir(inputs)

        # Directories
        package_root = os.path.join(self.package_root, context['tpw_version'])
        lib_dir = os.path.join(package_root, 'lib')

        # Output Name
        output = 'tpw.orbital.hirs.{}.{}.{}.ssec.nc'.format(context['sat'], inputs['HIR1B'][12:30],
                                                            context['tpw_version'])

        # Create links to static input files
        os.symlink(os.path.join(package_root, 'coeffs/hirscbnd_orig.dat'), 'hirscbnd_orig.dat')
        os.symlink(os.path.join(package_root, 'coeffs/hirscbnd.dat'), 'hirscbnd_shft.dat')
        shift_file = self.sat_name_to_coeff(context['sat'])
        os.symlink(os.path.join(package_root, 'coeffs/{}'.format(shift_file)), shift_file)

        # Generating CFSR Binaries
        self.generate_cfsr_bin(package_root, lib_dir)

        # Running TPW Orbital
        cmd = os.path.join(package_root, 'bin/hirs_regrtvl_main_cdf.exe')
        cmd += ' {} {} {}.bin'.format(inputs['HIR1B'], inputs['CTPO'], inputs['CFSR'])
        cmd += ' {} {}.qc {}'.format(output, output, shifted_FM_opt)

        print cmd
        check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

        return {'out': output}

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

    def generate_cfsr_bin(self, package_root, lib_dir):

        os.mkdir('scripts')
        shutil.copy(os.path.join(package_root, 'bin/wgrib2'), './scripts')

        # FIXME: There may be more than one type of these files...
        files = glob('pgbhnl.gdas.*.grb2')

        for file in files:
            cmd = os.path.join(package_root, 'bin/extract_ncep_cfsr_psfc.csh')
            cmd += ' ./ {} {}.bin'.format(file, file)

            print cmd
            check_call(cmd, shell=True, env=augmented_env({'LD_LIBRARY_PATH': lib_dir}))

    def sat_name_to_coeff(self, sat):

        coeff_file = {'metop-a': 'RC_CRTM21_ODPS_hirs4_moa_shift.nc',
                      'metop-b': 'RC_CRTM21_ODPS_hirs4_mob_shift.nc',
                      'noaa-06': 'RC_CRTM21_ODPS_hirs2_n06_shift.nc',
                      'noaa-07': 'RC_CRTM21_ODPS_hirs2_n07_shift.nc',
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
