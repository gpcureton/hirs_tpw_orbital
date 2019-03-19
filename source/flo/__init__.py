#!/usr/bin/env python
# encoding: utf-8
"""

Purpose: Run the hirs_tpw_orbital package

Copyright (c) 2015 University of Wisconsin Regents.
Licensed under GNU GPLv3.
"""

import os
from os.path import basename, dirname, curdir, abspath, isdir, isfile, exists, splitext, join as pjoin
import sys
from glob import glob
import shutil
import logging
import traceback
from subprocess import CalledProcessError

from flo.computation import Computation
from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta, round_datetime
from flo.util import augmented_env, symlink_inputs_to_working_dir
from flo.product import StoredProductCatalog

import sipsprod
from glutil import (
    check_call,
    dawg_catalog,
    delivered_software,
    #support_software,
    runscript,
    #prepare_env,
    #nc_gen,
    nc_compress,
    reraise_as,
    #set_official_product_metadata,
    FileNotFound
)
import flo.sw.hirs2nc as hirs2nc
import flo.sw.hirs_ctp_orbital as hirs_ctp_orbital
from flo.sw.hirs2nc.delta import DeltaCatalog
from flo.sw.hirs2nc.utils import link_files

# every module should have a LOG object
LOG = logging.getLogger(__name__)

def set_input_sources(input_locations):
    global delta_catalog
    delta_catalog = DeltaCatalog(**input_locations)

class HIRS_TPW_ORBITAL(Computation):

    parameters = ['granule', 'satellite', 'hirs2nc_delivery_id', 'hirs_avhrr_delivery_id',
                  'hirs_csrb_daily_delivery_id', 'hirs_csrb_monthly_delivery_id',
                  'hirs_ctp_orbital_delivery_id', 'hirs_ctp_daily_delivery_id',
                  'hirs_ctp_monthly_delivery_id', 'hirs_tpw_orbital_delivery_id']
    outputs = ['shift', 'noshift']

    def find_contexts(self, time_interval, satellite, hirs2nc_delivery_id, hirs_avhrr_delivery_id,
                      hirs_csrb_daily_delivery_id, hirs_csrb_monthly_delivery_id,
                      hirs_ctp_orbital_delivery_id, hirs_ctp_daily_delivery_id,
                      hirs_ctp_monthly_delivery_id, hirs_tpw_orbital_delivery_id):

        LOG.debug("Running find_contexts()")
        files = delta_catalog.files('hirs', satellite, 'HIR1B', time_interval)
        return [{'granule': file.data_interval.left,
                 'satellite': satellite,
                 'hirs2nc_delivery_id': hirs2nc_delivery_id,
                 'hirs_avhrr_delivery_id': hirs_avhrr_delivery_id,
                 'hirs_csrb_daily_delivery_id': hirs_csrb_daily_delivery_id,
                 'hirs_csrb_monthly_delivery_id': hirs_csrb_monthly_delivery_id,
                 'hirs_ctp_orbital_delivery_id': hirs_ctp_orbital_delivery_id,
                 'hirs_ctp_daily_delivery_id': hirs_ctp_daily_delivery_id,
                 'hirs_ctp_monthly_delivery_id': hirs_ctp_monthly_delivery_id,
                 'hirs_tpw_orbital_delivery_id': hirs_tpw_orbital_delivery_id}
                for file in files
                if file.data_interval.left >= time_interval.left]

    def get_cfsr(self, granule):
        '''
        Retrieve the CFSR file which covers the desired granule.
        '''

        wedge = timedelta(seconds=1)
        day = timedelta(days=1)

        cfsr_granule = round_datetime(granule, timedelta(hours=6))
        cfsr_file = None

        have_cfsr_file = False

        # Search for the old style pgbhnl.gdas.*.grb2 file from DAWG
        if not have_cfsr_file:
            LOG.debug("Trying to retrieve CFSR_PGRBHANL product (pgbhnl.gdas.*.grb2) CFSR files from DAWG...")
            try:
                cfsr_file = dawg_catalog.file('', 'CFSR_PGRBHANL', cfsr_granule)
                have_cfsr_file = True
            except Exception, err :
                LOG.debug("{}.".format(err))

        # Search for the new style cdas1.*.t*z.pgrbhanl.grib2 file DAWG
        if not have_cfsr_file:
            LOG.debug("Trying to retrieve cdas1.*.t*z.pgrbhanl.grib2 CFSR file from DAWG...")
            try:
                cfsr_file = dawg_catalog.file('', 'CFSV2_PGRBHANL', cfsr_granule)
                have_cfsr_file = True
            except Exception, err :
                LOG.debug("{}.".format(err))

        return cfsr_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_TPW_ORBITAL')
    def build_task(self, context, task):
        '''
        Build up a set of inputs for a single context
        '''
        global delta_catalog

        LOG.debug("Running build_task()")

        # Initialize the hirs2nc module with the data locations
        hirs2nc.delta_catalog = delta_catalog

        # Instantiate the hirs2nc and hirs_ctp_orbital computations
        hirs2nc_comp = hirs2nc.HIRS2NC()
        hirs_ctp_orbital_comp = hirs_ctp_orbital.HIRS_CTP_ORBITAL()

        SPC = StoredProductCatalog()

        #
        # HIRS L1B Input
        #
        hirs2nc_context ={
            'satellite': context['satellite'],
            'granule': context['granule'],
            'hirs2nc_delivery_id': context['hirs2nc_delivery_id']}

        hirs2nc_prod = hirs2nc_comp.dataset('out').product(hirs2nc_context)

        if SPC.exists(hirs2nc_prod):
            task.input('HIR1B', hirs2nc_prod)
        else:
            raise WorkflowNotReady('No HIRS inputs available for {}'.format(hirs2nc_context['granule']))

        #
        # CTP Orbital Input
        #
        hirs_ctp_orbital_context = context.copy()
        [hirs_ctp_orbital_context.pop(k) for k in ['hirs_ctp_daily_delivery_id',
                                                   'hirs_ctp_monthly_delivery_id',
                                                   'hirs_tpw_orbital_delivery_id']]

        hirs_ctp_orbital_prod = hirs_ctp_orbital_comp.dataset('out').product(hirs_ctp_orbital_context)

        if SPC.exists(hirs_ctp_orbital_prod):
            task.input('CTPO', hirs_ctp_orbital_prod)
        else:
            raise WorkflowNotReady('No HIRS CTP Orbital inputs available for {}'.format(
                hirs_ctp_orbital_context['granule']))

        #
        # CFSR Input
        #
        cfsr_granule = round_datetime(context['granule'], timedelta(hours=6))
        cfsr_file = self.get_cfsr(cfsr_granule)

        if cfsr_file is not None:
            task.input('CFSR', cfsr_file)
        else:
            raise WorkflowNotReady('No CFSR inputs available for {}'.format(cfsr_granule))

        LOG.debug("Final task.inputs...") # GPC
        for task_key in task.inputs.keys():
            LOG.debug("\t{}: {}".format(task_key,task.inputs[task_key])) # GPC

        LOG.debug("Exiting build_task()...") # GPC

    def extract_bin_from_cfsr(self, inputs, context):
        '''
        Run wgrib2 on the  input CFSR grib files, to create flat binary files
        containing the desired data.
        '''

        # Where are we running the package
        work_dir = abspath(curdir)
        LOG.debug("working dir = {}".format(work_dir))

        # Get the required CFSR and wgrib2 script locations
        hirs_tpw_orbital_delivery_id = context['hirs_tpw_orbital_delivery_id']
        delivery = delivered_software.lookup('hirstpw_L2', delivery_id=hirs_tpw_orbital_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        extract_cfsr_bin = pjoin(dist_root, 'extract_ncep_cfsr_psfc.csh')
        version = delivery.version

        # Get the CFSR input
        cfsr_file = inputs['CFSR']
        LOG.debug("CFSR file: {}".format(cfsr_file))

        # Extract the desired datasets for each CFSR file
        rc = 0
        new_cfsr_files = []

        output_cfsr_file = '{}.bin'.format(basename(cfsr_file))
        cmd = '{} {} {} {}'.format(extract_cfsr_bin, dist_root, cfsr_file, output_cfsr_file)
        #cmd = 'sleep 0; touch {}'.format(output_cfsr_file) # DEBUG

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_extract_cfsr = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_extract_cfsr = err.returncode
            LOG.error("extract_cfsr binary {} returned a value of {}".format(extract_cfsr_bin, rc_extract_cfsr))
            return rc_extract_cfsr, []

        # Verify output file
        output_cfsr_files = glob(output_cfsr_file)
        if len(output_cfsr_files) != 0:
            output_cfsr_files = output_cfsr_files[0]
            LOG.debug('Found flat CFSR file "{}"'.format(output_cfsr_files))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_cfsr_file))
            rc = 1
            return rc, None

        return rc, output_cfsr_files

    def sat_name_to_coeff(self, satellite, shifted=True):

        coeff_files = {'metop-a' : 'HIRS_AVHRR_TPW_regcoef_metop_1_noshift_v2017216.nc',
                       'metop-b' : 'HIRS_AVHRR_TPW_regcoef_metop_2_noshift_v2017216.nc',
                       'noaa-06' : 'HIRS_AVHRR_TPW_regcoef_noaa_06_noshift_v2017216.nc',
                       'noaa-07' : 'HIRS_AVHRR_TPW_regcoef_noaa_07_noshift_v2017216.nc',
                       'noaa-08' : 'HIRS_AVHRR_TPW_regcoef_noaa_08_noshift_v2017216.nc',
                       'noaa-09' : 'HIRS_AVHRR_TPW_regcoef_noaa_09_noshift_v2017216.nc',
                       'noaa-10' : 'HIRS_AVHRR_TPW_regcoef_noaa_10_noshift_v2017216.nc',
                       'noaa-11' : 'HIRS_AVHRR_TPW_regcoef_noaa_11_noshift_v2017216.nc',
                       'noaa-12' : 'HIRS_AVHRR_TPW_regcoef_noaa_12_noshift_v2017216.nc',
                       'noaa-14' : 'HIRS_AVHRR_TPW_regcoef_noaa_14_noshift_v2017216.nc',
                       'noaa-15' : 'HIRS_AVHRR_TPW_regcoef_noaa_15_noshift_v2017216.nc',
                       'noaa-16' : 'HIRS_AVHRR_TPW_regcoef_noaa_16_noshift_v2017216.nc',
                       'noaa-17' : 'HIRS_AVHRR_TPW_regcoef_noaa_17_noshift_v2017216.nc',
                       'noaa-18' : 'HIRS_AVHRR_TPW_regcoef_noaa_18_noshift_v2017216.nc',
                       'noaa-19' : 'HIRS_AVHRR_TPW_regcoef_noaa_19_noshift_v2017216.nc'}

        coeff_files_shift = {'metop-a' : 'HIRS_AVHRR_TPW_regcoef_metop_1_shift_v2017216.nc',
                             'metop-b' : 'HIRS_AVHRR_TPW_regcoef_metop_2_shift_v2017216.nc',
                             'noaa-06' : 'HIRS_AVHRR_TPW_regcoef_noaa_06_shift_v2017216.nc',
                             'noaa-07' : 'HIRS_AVHRR_TPW_regcoef_noaa_07_shift_v2017216.nc',
                             'noaa-08' : 'HIRS_AVHRR_TPW_regcoef_noaa_08_noshift_v2017216.nc',
                             'noaa-09' : 'HIRS_AVHRR_TPW_regcoef_noaa_09_shift_v2017216.nc',
                             'noaa-10' : 'HIRS_AVHRR_TPW_regcoef_noaa_10_shift_v2017216.nc',
                             'noaa-11' : 'HIRS_AVHRR_TPW_regcoef_noaa_11_shift_v2017216.nc',
                             'noaa-12' : 'HIRS_AVHRR_TPW_regcoef_noaa_12_shift_v2017216.nc',
                             'noaa-14' : 'HIRS_AVHRR_TPW_regcoef_noaa_14_shift_v2017216.nc',
                             'noaa-15' : 'HIRS_AVHRR_TPW_regcoef_noaa_15_shift_v2017216.nc',
                             'noaa-16' : 'HIRS_AVHRR_TPW_regcoef_noaa_16_shift_v2017216.nc',
                             'noaa-17' : 'HIRS_AVHRR_TPW_regcoef_noaa_17_shift_v2017216.nc',
                             'noaa-18' : 'HIRS_AVHRR_TPW_regcoef_noaa_18_shift_v2017216.nc',
                             'noaa-19' : 'HIRS_AVHRR_TPW_regcoef_noaa_19_shift_v2017216.nc'}

        coeff_file = coeff_files_shift[satellite] if shifted else coeff_files[satellite]

        return coeff_file

    def link_coeffs(self, context):
        '''
        Link the shifted and nonshifted coefficient files into the current directory
        '''
        rc = 0
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_tpw_orbital_delivery_id = context['hirs_tpw_orbital_delivery_id']
        delivery = delivered_software.lookup('hirstpw_L2', delivery_id=hirs_tpw_orbital_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        lut_dir = dist_root

        # Link the shifted coefficient files into the working directory
        shifted_coeffs =   [abspath(pjoin(lut_dir, self.sat_name_to_coeff(context['satellite'])))]
        unshifted_coeffs = [abspath(pjoin(lut_dir, self.sat_name_to_coeff(context['satellite'], shifted=False)))]
        linked_coeffs = link_files(current_dir, shifted_coeffs + unshifted_coeffs +
                                   [
                                       abspath(pjoin(lut_dir, 'hirscbnd_orig.dat')),
                                       abspath(pjoin(lut_dir, 'hirscbnd_shft.dat'))
                                   ])

    def hirs_to_time_interval(self, filename):
        '''
        Takes the HIRS filename as input and returns the time interval
        covering that file.
        '''

        file_chunks = filename.split('.')
        begin_time = datetime.strptime('.'.join(file_chunks[3:5]), 'D%y%j.S%H%M')
        end_time = datetime.strptime('.'.join([file_chunks[3], file_chunks[5]]), 'D%y%j.E%H%M')

        if end_time < begin_time:
            end_time += timedelta(days=1)

        return TimeInterval(begin_time, end_time)

    def create_tpw_orbital(self, inputs, context, shifted=False):
        '''
        Create the the TPW Orbital for the current granule.
        '''

        rc = 0

        # Create the output directory
        current_dir = os.getcwd()

        # Get the required CFSR and wgrib2 script locations
        hirs_tpw_orbital_delivery_id = context['hirs_tpw_orbital_delivery_id']
        delivery = delivered_software.lookup('hirstpw_L2', delivery_id=hirs_tpw_orbital_delivery_id)
        dist_root = pjoin(delivery.path, 'dist')
        lut_dir = dist_root
        version = delivery.version

        # Compile a dictionary of the input orbital data files
        interval = self.hirs_to_time_interval(inputs['HIR1B'])
        LOG.debug("HIRS interval {} -> {}".format(interval.left,interval.right))

        # Determine the output filenames
        output_file = 'hirs_tpw_orbital_{}_{}_{}{}.nc'.format(context['satellite'],
                                                          'shift' if shifted else 'noshift',
                                                          interval.left.strftime('D%y%j.S%H%M'),
                                                          interval.right.strftime('.E%H%M'))
        LOG.info("output_file: {}".format(output_file))

        # No shift for NOAA-8
        if (not shifted) or (context['satellite']=='noaa-08'):
            shifted_FM_opt = 1
        else:
            shifted_FM_opt = 2

        tpw_orbital_bin = pjoin(dist_root, 'hirs_regrtvl_main_cdf.exe')

        cmd = '{} {} {} {} {} {} {} &> {}'.format(
                tpw_orbital_bin,
                inputs['HIR1B'],
                inputs['CTPO'],
                inputs['CFSR'],
                output_file,
                '{}_QC.nc'.format(splitext(output_file)[0]),
                shifted_FM_opt,
                '{}.log'.format(splitext(output_file)[0])
                )
        #cmd = 'sleep 1; touch {}'.format(output_file) # DEBUG

        try:
            LOG.debug("cmd = \\\n\t{}".format(cmd.replace(' ',' \\\n\t')))
            rc_tpw = 0
            runscript(cmd, [delivery])
        except CalledProcessError as err:
            rc_tpw = err.returncode
            LOG.error(" TPW orbital binary {} returned a value of {}".format(tpw_orbital_bin, rc_tpw))
            return rc_tpw, None

        # Verify output file
        output_file = glob(output_file)
        if len(output_file) != 0:
            output_file = output_file[0]
            LOG.debug('Found output TPW orbital file "{}"'.format(output_file))
        else:
            LOG.error('Failed to generate "{}", aborting'.format(output_file))
            rc = 1
            return rc, None

        return rc, output_file

    @reraise_as(WorkflowNotReady, FileNotFound, prefix='HIRS_TPW_ORBITAL')
    def run_task(self, inputs, context):
        '''
        Run the TPW Orbital binary on a single context
        '''

        LOG.debug("Running run_task()...")

        for key in context.keys():
            LOG.debug("run_task() context['{}'] = {}".format(key, context[key]))

        rc = 0

        # Extract a binary array from a CFSR reanalysis GRIB2 file on a
        # global equal angle grid at 0.5 degree resolution. CFSR files
        rc, cfsr_file = self.extract_bin_from_cfsr(inputs, context)

        # Link the inputs into the working directory
        inputs.pop('CFSR')
        inputs = symlink_inputs_to_working_dir(inputs)
        inputs['CFSR'] = cfsr_file

        # Link the shifted and nonshifted coefficient files into the current directory
        self.link_coeffs(context)

        # Create the TPW Orbital for the current granule.
        rc, tpw_orbital_noshift_file = self.create_tpw_orbital(inputs, context, shifted=False)
        rc, tpw_orbital_shift_file = self.create_tpw_orbital(inputs, context, shifted=True)

        interval = self.hirs_to_time_interval(inputs['HIR1B'])
        extra_attrs = {'begin_time': interval.left,
                       'end_time': interval.right}

        return {
                'shift': {
                    'file': nc_compress(tpw_orbital_shift_file), 'extra_attrs': extra_attrs},
                'noshift': {
                    'file': nc_compress(tpw_orbital_noshift_file), 'extra_attrs': extra_attrs}
                }
