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
from cohorts import Cohort

from .pipeline import Pipeline

class Discohort(Cohort):
    def __init__(self, base_object, results_path=None, work_dirs=[]):
        self.__class__ = type(base_object.__class__.__name__,
                              (self.__class__, base_object.__class__),
                              {})
        self.__dict__ = deepcopy(base_object.__dict__)
        self.pipelines = {}
        self.is_processed = False
        self.results_path = results_path
        self.work_dirs = work_dirs

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

    def populate(self):
        pass
