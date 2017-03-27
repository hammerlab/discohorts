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
from os import path, listdir
import re
from collections import defaultdict
from cohorts import Cohort

from .pipeline import Pipeline

DEFAULT_ID_DELIMS = ["_", "-"]

class Discohort(Cohort):
    def __init__(self, base_object, biokepi_work_dirs, biokepi_results_dirs, id_delims=DEFAULT_ID_DELIMS):
        if len(biokepi_work_dirs) < 1:
            raise ValueError("Need at least one work dir, but work_dirs = {}".format(biokepi_work_dirs))

        self.__class__ = type(base_object.__class__.__name__,
                              (self.__class__, base_object.__class__),
                              {})
        self.__dict__ = deepcopy(base_object.__dict__)
        self.pipelines = {}
        self.is_processed = False
        self.biokepi_work_dirs = biokepi_work_dirs
        self.biokepi_results_dirs = biokepi_results_dirs
        self.id_delims = id_delims

    def add_pipeline(self, name, ocaml_path, name_cli_arg="name", other_cli_args={}):
        if name in self.pipelines:
            raise ValueError("Pipeline already exists: {}".format(name))

        pipeline = Pipeline(name=name,
                            ocaml_path=ocaml_path,
                            name_cli_arg=name_cli_arg,
                            other_cli_args=other_cli_args)
        self.pipelines[name] = pipeline

    def run_pipeline(self, name):
        if name not in self.pipelines:
            raise ValueError("Trying to run a pipeline that does not exist: {}".format(name))

        pipeline = self.pipelines[name]
        pipeline.run(self)

    def patient_id_(self, patient):


        return patient_ids_with_delims

    def find_patient_id(self, name):
        """
        Given a file or folder name, determine whether any patient IDs are present
        inside it using the appropriate delimiters.

        Returns the corresponding patient ID or None.
        """
        def with_delims(patient_id):
            with_delims_list = []
            # ^<id>$, ^<id>_, _<id>$, _<id>_
            for delim in self.id_delims:
                for beginning in [delim, "^"]:
                    for end in [delim, "$"]:
                        with_delims_list.append("{}{}{}".format(
                            beginning, patient_id, end))
            return with_delims_list

        found_ids = []
        for patient in self:
            for with_delim in with_delims(patient.id):
                if re.search(with_delim, name):
                    found_ids.append(patient.id)

        if len(found_ids) == 1:
            return found_ids[0]
        elif len(found_ids) > 1:
            raise ValueError("Found multiple candidate patients for name {}: {}".format(
                name, found_ids))
        return None

    def populate(self):
        patient_id_to_patient_results = {}
        for results_dir in self.biokepi_results_dirs:
            patient_results_dirs = listdir(results_dir)
            for patient_results_dir in patient_results_dirs:
                found_patient_id = self.find_patient_id(patient_results_dir)
                if found_patient_id is not None:
                    patient_id_to_patient_results[found_patient_id] = path.join(results_dir, patient_results_dir)
        print(patient_id_to_patient_results)
