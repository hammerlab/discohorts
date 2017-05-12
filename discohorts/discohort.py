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

from copy import deepcopy
from os import path, listdir, makedirs
from shutil import copy2, move
import subprocess
from collections import defaultdict
from cohorts import Cohort

from .pipeline import Pipeline
from .utils import find_files_recursive, find_patient_id
from .config import EpidiscoConfig

DEFAULT_ID_DELIMS = ["_", "-"]


class Discohort(Cohort):
    def __init__(self,
                 base_object,
                 biokepi_work_dirs,
                 biokepi_results_dirs,
                 dest_results_dir=None,
                 id_delims=DEFAULT_ID_DELIMS,
                 batch_size=50,
                 batch_wait_secs=0):
        if len(biokepi_work_dirs) < 1:
            raise ValueError(
                "Need at least one work dir, but work_dirs = {}".format(biokepi_work_dirs))

        self.__class__ = type(base_object.__class__.__name__, (self.__class__,
                                                               base_object.__class__), {})
        self.__dict__ = deepcopy(base_object.__dict__)
        self.pipelines = {}
        self.is_processed = False
        self.biokepi_work_dirs = biokepi_work_dirs
        self.biokepi_results_dirs = biokepi_results_dirs
        self.dest_results_dir = dest_results_dir
        self.id_delims = id_delims
        self.batch_size = batch_size
        self.batch_wait_secs = batch_wait_secs

    def add_epidisco_pipeline(self, pipeline_name, config=None, run_name=None, pipeline_path="run_pipeline.ml"):
        if config is None:
            config = EpidiscoConfig(self)
        if run_name is None:
            run_name = lambda patient: "{}_{}".format(pipeline_name, patient.id)
        config.update("anonymous_args", [run_name])
        return self.add_pipeline(pipeline_name=pipeline_name, config=config, pipeline_path=pipeline_path)

    def add_pipeline(self, pipeline_name, config, pipeline_path):
        if pipeline_name in self.pipelines:
            raise ValueError("Pipeline already exists: {}".format(pipeline_name))

        pipeline = Pipeline(
            config=config,
            pipeline_path=pipeline_path,
            batch_size=self.batch_size,
            batch_wait_secs=self.batch_wait_secs)
        self.pipelines[pipeline_name] = pipeline

    def run_pipeline(self, pipeline_name, skip_num=0, wait_after_all=False, dry_run=False):
        if pipeline_name not in self.pipelines:
            raise ValueError("Trying to run a pipeline that does not exist: {}".format(pipeline_name))

        pipeline = self.pipelines[pipeline_name]
        pipeline.run(self, skip_num=skip_num, wait_after_all=wait_after_all, dry_run=dry_run)

    def populate(self, require_all_patients=True):
        # TODO: Incomplete method.
        patient_id_to_patient_results = defaultdict(list)
        for results_dir in self.biokepi_results_dirs:
            patient_results_dirs = listdir(results_dir)
            for patient_results_dir in patient_results_dirs:
                found_patient_id = find_patient_id(self, patient_results_dir, self.id_delims)
                if found_patient_id is not None:
                    patient_id_to_patient_results[found_patient_id].append(
                        path.join(results_dir, patient_results_dir))
        if require_all_patients and (len(patient_id_to_patient_results) != len(self)):
            raise ValueError("Only found {} patients to populate the Cohort with, but expected {}".
                             format(len(patient_id_to_patient_results), len(self)))

        # TODO: Actually populate the cohort with more data.
        for patient in self:
            results_paths = patient_id_to_patient_results[patient.id]
            # TODO: Don't populate for multiple paths.
            for results_path in results_paths:
                self.populate_hla(patient, results_path)

    # TODO: Incomplete method.
    def populate_hla(self, patient, patient_results):
        tsv_files = find_files_recursive(search_path=patient_results, pattern="*.tsv")
        if len(tsv_files) > 1:
            raise ValueError(("More than one TSV found for OptiType results in {}, "
                              "but only one expected").format(patient_results))
        if len(tsv_files) == 0:
            raise ValueError("No OptiType TSV found in {}".format(patient_results))

        optitype_results_dir = path.dirname(tsv_files[0])
        optitype_hlarp = subprocess.check_output(["hlarp", "optitype", optitype_results_dir])
        print(optitype_hlarp)
        pass
