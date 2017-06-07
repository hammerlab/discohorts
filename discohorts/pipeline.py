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

from subprocess import check_call
from os import environ, path
import time
from types import FunctionType


class Pipeline(object):
    def __init__(self, pipeline_path, config, batch_size, batch_wait_secs):
        self.config = config
        self.pipeline_path = pipeline_path
        self.batch_size = batch_size
        self.batch_wait_secs = batch_wait_secs

    def run(self, discohort, skip_num, wait_after_all, dry_run):
        ran_count = 0
        original_envs = dict()
        for var in ("BIOKEPI_WORK_DIR", "INSTALL_TOOLS_PATH", "PYENSEMBL_CACHE_DIR"):
            if var in environ:
                original_envs[var] = environ[var]
            else:
                original_envs[var] = None
        try:
            if original_envs["BIOKEPI_WORK_DIR"] is not None:
                environ["INSTALL_TOOLS_PATH"] = path.join(original_envs["BIOKEPI_WORK_DIR"], "toolkit")
                print("Setting INSTALL_TOOLS_PATH={}".format(environ["INSTALL_TOOLS_PATH"]))
                environ["PYENSEMBL_CACHE_DIR"] = path.join(original_envs["BIOKEPI_WORK_DIR"], "pyensembl-cache")
                print("Setting PYENSEMBL_CACHE_DIR={}".format(environ["PYENSEMBL_CACHE_DIR"]))
                environ["REFERENCE_GENOME_PATH"] = path.join(original_envs["BIOKEPI_WORK_DIR"], "reference-genome")
                print("Setting REFERENCE_GENOME_PATH={}".format(environ["REFERENCE_GENOME_PATH"]))
            work_dir_index = 0

            # Run on only the correct subset of patients.
            patient_subset = [patient for patient in discohort.cohort if self.config.keep(patient)]

            # Map from patient to the appropriate work dir.
            def get_patient_to_work_dir(patients, work_dirs):
                num_chunks = len(work_dirs)
                return_dict = {}
                for i, work_dir in enumerate(work_dirs):
                    for item in patients[i::num_chunks]:
                        return_dict[item] = work_dir
                return return_dict

            patient_to_work_dir = get_patient_to_work_dir(patient_subset,
                                                          discohort.biokepi_work_dirs)

            # Loop over all relevant patients.
            print("Running on a patient subset of {} patients".format(len(patient_subset)))
            for patient in patient_subset:
                # Grab the work_dir and run an optional function that takes in work_dir as input.
                # Also set the BIOKEPI_WORK_DIR environment variable.
                work_dir = patient_to_work_dir[patient]
                self.config.given_work_dir(patient, work_dir)
                environ["BIOKEPI_WORK_DIR"] = patient_to_work_dir[patient]
                print("Setting BIOKEPI_WORK_DIR={}".format(environ["BIOKEPI_WORK_DIR"]))

                # Build up our command.
                command = ["ocaml", self.pipeline_path]

                # If an argument has a None value, skip it.
                # If an argument has a boolean value, include it as --arg if True.
                # If an argument has a non-boolean value, include it as --arg=<value>.
                for attr in dir(self.config):
                    if attr.startswith("arg_"):
                        value = getattr(self.config, attr)
                        value = value(patient) # Always will be f(patient)
                        if value is not None:
                            arg_name = attr.split("arg_")[1]
                            arg_name = arg_name.replace("_", "-")
                            if value == True:
                                command.append("--{}".format(arg_name))
                            elif type(value) != bool:
                                command.append("--{}={}".format(arg_name, value))

                # Anonymous args (non-keyword args) have no key/value.
                for anon_arg in self.config.anonymous_args(patient):
                    if type(anon_arg) == FunctionType:
                        anon_arg = anon_arg(patient)
                    command.append(anon_arg)

                print("Running {}".format(" ".join(command)))
                ran_count += 1

                if ran_count <= skip_num:
                    print("(Actually skipping this one, number {})".format(ran_count))
                else:
                    if dry_run:
                        print("(Not actually running)")
                    else:
                        check_call(command)

                if ran_count % self.batch_size == 0:
                    print(
                        "Waiting for {} seconds after the last batch of {} ({} total submitted so far)".
                        format(self.batch_wait_secs, self.batch_size, ran_count))
                    time.sleep(self.batch_wait_secs)

                if wait_after_all and ran_count == len(patient_subset):
                    print(
                        "Waiting for {} seconds after the cohort ended ({} total submitted so far)".
                        format(self.batch_wait_secs, ran_count))
        finally:
            for var in original_envs:
                if original_envs[var] is None:
                    if var in environ:
                        del environ[var]
                else:
                    environ[var] = original_envs[var]

