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

DEFAULT_ID_DELIMS = ["_", "-"]


class Discohort(Cohort):
    def __init__(self,
                 base_object,
                 biokepi_work_dirs,
                 biokepi_results_dirs,
                 dest_results_dir=None,
                 id_delims=DEFAULT_ID_DELIMS,
                 copy_only_patterns=["*.tsv"],
                 batch_size=50,
                 batch_wait_secs=0):
        if len(biokepi_work_dirs) < 1:
            raise ValueError("Need at least one work dir, but work_dirs = {}".
                             format(biokepi_work_dirs))

        self.__class__ = type(base_object.__class__.__name__,
                              (self.__class__, base_object.__class__), {})
        self.__dict__ = deepcopy(base_object.__dict__)
        self.pipelines = {}
        self.is_processed = False
        self.biokepi_work_dirs = biokepi_work_dirs
        self.biokepi_results_dirs = biokepi_results_dirs
        self.dest_results_dir = dest_results_dir
        self.id_delims = id_delims
        self.copy_only_patterns = copy_only_patterns
        self.batch_size = batch_size
        self.batch_wait_secs = batch_wait_secs

    def add_pipeline(self,
                     name,
                     ocaml_path,
                     name_cli_arg="name",
                     other_cli_args={},
                     patient_subset_function=None,
                     work_dir_function=None):
        if name in self.pipelines:
            raise ValueError("Pipeline already exists: {}".format(name))

        pipeline = Pipeline(
            name=name,
            ocaml_path=ocaml_path,
            name_cli_arg=name_cli_arg,
            other_cli_args=other_cli_args,
            patient_subset_function=patient_subset_function,
            work_dir_function=work_dir_function,
            batch_size=self.batch_size,
            batch_wait_secs=self.batch_wait_secs)
        self.pipelines[name] = pipeline

    def run_pipeline(self, name, skip_num=0, wait_after_all=False, dry_run=False):
        if name not in self.pipelines:
            raise ValueError(
                "Trying to run a pipeline that does not exist: {}".format(
                    name))

        pipeline = self.pipelines[name]
        pipeline.run(self, skip_num=skip_num, wait_after_all=wait_after_all, dry_run=dry_run)

    def move_results_dirs(self, updated_name):
        """
        Rename existing work directories to prevent collisions with future runs.
        """
        move_tuples = []
        for results_dir in self.biokepi_results_dirs:
            if not path.exists(results_dir):
                print("{} does not exist, so there is nothing to move".format(
                    results_dir))
                continue

            new_dir = path.join(path.dirname(results_dir), updated_name)
            if path.exists(new_dir):
                raise ValueError("{} already exists".format(new_dir))
            move_tuples.append((results_dir, new_dir))

        for old_dir, new_dir in move_tuples:
            print("Moving {} to {}".format(old_dir, new_dir))
            move(old_dir, new_dir)

    def copy_data(self, only_cohort=True):
        if self.dest_results_dir is None:
            raise ValueError("No dest_results_dir provided")

        if not (path.exists(self.dest_results_dir)):
            raise ValueError("dest_results_dir does not exist: {}".format(
                self.dest_results_dir))

        for results_dir in self.biokepi_results_dirs:
            patient_results_dirs = listdir(results_dir)
            for patient_results_dir in patient_results_dirs:
                old_path = path.join(results_dir, patient_results_dir)
                found_patient_id = find_patient_id(self, patient_results_dir,
                                                   self.id_delims)
                # If this is a patient directory for this Cohort, *or*
                # we don't care whether it's part of the Cohort or not,
                # then copy to the destination directory.
                if found_patient_id is not None or not only_cohort:
                    new_path = path.join(self.dest_results_dir,
                                         path.basename(patient_results_dir))
                    while path.exists(new_path):
                        updated_new_path = new_path + ".1"
                        print("{} exists; copying to {}".format(
                            new_path, updated_new_path))
                        new_path = updated_new_path

                    # Now only copy relevant files.
                    for pattern in self.copy_only_patterns:
                        patient_file_paths = find_files_recursive(
                            search_path=old_path, pattern=pattern)
                        for patient_file_path in patient_file_paths:
                            patient_file_new_path = patient_file_path.replace(
                                old_path, new_path)
                            makedirs(patient_file_new_path)
                            print("Copying {} to {}...".format(
                                patient_file_path, patient_file_new_path))
                            copy2(patient_file_path, patient_file_new_path)

    def populate(self, require_all_patients=True):
        # TODO: Incomplete method.
        patient_id_to_patient_results = defaultdict(list)
        for results_dir in self.biokepi_results_dirs:
            patient_results_dirs = listdir(results_dir)
            for patient_results_dir in patient_results_dirs:
                found_patient_id = find_patient_id(self, patient_results_dir,
                                                   self.id_delims)
                if found_patient_id is not None:
                    patient_id_to_patient_results[found_patient_id].append(
                        path.join(results_dir, patient_results_dir))
        if require_all_patients and (
                len(patient_id_to_patient_results) != len(self)):
            raise ValueError(
                "Only found {} patients to populate the Cohort with, but expected {}".
                format(len(patient_id_to_patient_results), len(self)))

        # TODO: Actually populate the cohort with more data.
        for patient in self:
            results_paths = patient_id_to_patient_results[patient.id]
            # TODO: Don't populate for multiple paths.
            for results_path in results_paths:
                self.populate_hla(patient, results_path)

    # TODO: Incomplete method.
    def populate_hla(self, patient, patient_results):
        tsv_files = find_files_recursive(
            search_path=patient_results, pattern="*.tsv")
        if len(tsv_files) > 1:
            raise ValueError(
                ("More than one TSV found for OptiType results in {}, "
                 "but only one expected").format(patient_results))
        if len(tsv_files) == 0:
            raise ValueError(
                "No OptiType TSV found in {}".format(patient_results))

        optitype_results_dir = path.dirname(tsv_files[0])
        optitype_hlarp = subprocess.check_output(
            ["hlarp", "optitype", optitype_results_dir])
        print(optitype_hlarp)
        pass
