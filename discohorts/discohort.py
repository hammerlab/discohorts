# Copyright (c) 2017. Mount Sinai School of Medicine
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

from copy import copy
from os import path, listdir, makedirs
from shutil import copy2, move
from collections import defaultdict
from cohorts import Cohort
import pandas as pd

from .pipeline import Pipeline
from .utils import find_files_recursive, find_patient, run_hlarp, get_logger
from .config import EpidiscoConfig

DEFAULT_ID_DELIMS = ["_", "-"]

logger = get_logger(__name__)


class Discohort(object):
    def __init__(self,
                 cohort,
                 biokepi_work_dirs,
                 biokepi_results_dirs=[],
                 id_delims=DEFAULT_ID_DELIMS,
                 batch_size=50,
                 batch_wait_secs=0):
        if len(biokepi_work_dirs) < 1:
            raise ValueError(
                "Need at least one work dir, but work_dirs = {}".format(biokepi_work_dirs))

        self.cohort = cohort
        self.pipelines = {}
        self.is_processed = False
        self.biokepi_work_dirs = biokepi_work_dirs
        self.biokepi_results_dirs = biokepi_results_dirs
        self.id_delims = id_delims
        self.batch_size = batch_size
        self.batch_wait_secs = batch_wait_secs

    def add_epidisco_pipeline(self,
                              pipeline_name,
                              config=None,
                              run_name=None,
                              pipeline_path="run_pipeline.ml"):
        if config is None:
            config = EpidiscoConfig(self)
        if run_name is None:
            run_name = lambda patient: "{}_{}".format(pipeline_name, patient.id)
        config.update("anonymous_args", [run_name])
        return self.add_pipeline(
            pipeline_name=pipeline_name, config=config, pipeline_path=pipeline_path)

    def add_pipeline(self, pipeline_name, config, pipeline_path):
        if pipeline_name in self.pipelines:
            raise ValueError("Pipeline already exists: {}".format(pipeline_name))

        if config.discohort is None:
            config.discohort = self

        pipeline = Pipeline(
            config=config,
            pipeline_path=pipeline_path,
            batch_size=self.batch_size,
            batch_wait_secs=self.batch_wait_secs)
        self.pipelines[pipeline_name] = pipeline

    def run_pipeline(self, pipeline_name, skip_num=0, wait_after_all=False, dry_run=False):
        if pipeline_name not in self.pipelines:
            raise ValueError(
                "Trying to run a pipeline that does not exist: {}".format(pipeline_name))

        pipeline = self.pipelines[pipeline_name]
        pipeline.run(self, skip_num=skip_num, wait_after_all=wait_after_all, dry_run=dry_run)

    def populate(self, must_contain, only_complete=True, cohort=None):
        """
        must_contain determines what we're populating: RNA, DNA, etc.
        e.g. must_contain="dna" looks for "dna" in the root directory.

        TODO: This does not yet work with Epidisco, where DNA and RNA are in the same root
        directory.

        If only_complete is True, raise an error if we don't populate every Patient
        in the Cohort.
        """
        if cohort is None:
            cohort = self.cohort

        # We may have different results directories on different NFS servers, for example.
        # e.g. ['/nfs-pool-2/biokepi/results', '/nfs-pool-3/biokepi/results']
        patient_to_path = {}
        new_patients = []
        for results_dir in self.biokepi_results_dirs:
            # e.g. '/nfs-pool-2/biokepi/results/lung-322'
            for patient_dir in listdir(results_dir):
                # Only look for e.g. "rna" or "dna" at a time.
                if must_contain not in patient_dir:
                    continue

                # Look for a patient ID in the directory name.
                found_patient = find_patient(self, patient_dir, self.id_delims)
                if found_patient is not None:
                    # Make sure we don't have multiple dirs per patient, either across or within the
                    # root results directories, or e.g. RNA vs. DNA.
                    patient_path = path.join(results_dir, patient_dir)
                    if found_patient in patient_to_path:
                        raise ValueError(
                            "Already have a dir for patient {} ({}), but found another dir ({})".
                            format(found_patient.id, patient_path, patient_to_path[found_patient]))
                    else:
                        patient_to_path[found_patient] = patient_path

        # Here we list out different components to populate.
        self.populate_fn(fn=populate_optitype, patient_to_path=patient_to_path, only_complete=only_complete, cohort=cohort)

    def populate_fn(self, fn, patient_to_path, only_complete, cohort):
        """
        For a given fn (e.g. populate_optitype) and patient paths (patient_to_path), update the
        patients (e.g. with patient.hla_alleles) in the cohort.

        If only_complete is True, only update the patients if they will *all* be updated.
        """
        patient_modifiers = []
        for patient in cohort:
            if patient not in patient_to_path:
                continue

            patient_path = patient_to_path[patient]
            # A patient modifier updates the patient as appropriate, when called.
            # e.g. patient.hla_alleles = hla_alleles
            patient_modifier = fn(patient, patient_path)
            if patient_modifier is not None:
                patient_modifiers.append(patient_modifier)

        if only_complete and len(patient_modifiers) < len(self):
            raise ValueError(
                "Must populate entire Cohort ({} patients), but valid data was only found for {} patients"
            )

        for patient_modifier in patient_modifiers:
            patient_modifier()


def get_hla_dirs(patient_path, caller):
    if caller == "optitype":
        tsv_files = find_files_recursive(search_path=patient_path, pattern="*.tsv")
        tsv_dirs = [path.dirname(tsv_file) for tsv_file in tsv_files]

        # Filter to those paths with the caller in the name
        optitype_tsv_dirs = [tsv_dir for tsv_dir in tsv_dirs if "optitype" in tsv_dir.lower()]

        return optitype_tsv_dirs
    raise ValueError("Invalid caller: {}".format(caller))


def populate_optitype(patient, patient_path):
    """
    Given a path to a Patient's results, return a function f() that updates
    the Patient's HLA alleles.
    """
    caller = "optitype"
    hla_dirs = get_hla_dirs(patient_path=patient_path, caller=caller)
    if len(hla_dirs) > 1:
        logger.warning(("More than one dir found for HLA results in {}, "
                        "but only one expected").format(patient_path))
        return None

    if len(hla_dirs) == 0:
        logger.warning("No HLA dirs found in {}".format(patient_path))
        return None

    hla_dir = hla_dirs[0]
    df_hlarp = run_hlarp(results_dir=hla_dir, caller=caller)
    hla_alleles = list(df_hlarp.allele)

    def modifier():
        patient.hla_alleles = hla_alleles

    return modifier
