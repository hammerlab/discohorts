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

class Pipeline(object):
    def __init__(self, name, ocaml_path, name_cli_arg, other_cli_args):
        self.name = name
        self.ocaml_path = ocaml_path
        self.name_cli_arg = name_cli_arg
        self.other_cli_args = other_cli_args

    def run(self, cohort):
        for patient in cohort:
            command = ["ocaml", self.ocaml_path]
            command.append("--{}={}".format(self.name_cli_arg, "{}_{}".format(self.name, patient.id)))
            for cli_arg, cli_arg_value in self.other_cli_args.items():
                if type(cli_arg_value) == FunctionType:
                    cli_arg_value = cli_arg_value(patient)
                command.append("--{}={}".format(cli_arg, cli_arg_value))
            print("Running {}".format(" ".join(command)))
            check_call(command)
