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

from types import FunctionType
from subprocess import check_call
from cohorts import Cohort

class Discohort(Cohort):
    def __init__(self, baseObject):
        self.__class__ = type(baseObject.__class__.__name__,
                              (self.__class__, baseObject.__class__),
                              {})
        self.__dict__ = baseObject.__dict__
        self.is_processed = False

    def run(self, pipeline_name, ocaml_pipeline_path, name_cli_arg="name", other_cli_args={}):
        for patient in self:
            command = ["ocaml", ocaml_pipeline_path]
            command.append("--{}={}".format(name_cli_arg, "{}_{}".format(pipeline_name, patient.id)))
            for cli_arg, cli_arg_value in other_cli_args.items():
                if type(cli_arg_value) == FunctionType:
                    cli_arg_value = cli_arg_value(patient)
                command.append("--{}={}".format(cli_arg, cli_arg_value))
            print("Running {}".format(" ".join(command)))
            check_call(command)
            break