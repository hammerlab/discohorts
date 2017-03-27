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
from os import environ, path

class Pipeline(object):
    def __init__(self, name, ocaml_path, name_cli_arg, other_cli_args):
        self.name = name
        self.ocaml_path = ocaml_path
        self.name_cli_arg = name_cli_arg
        self.other_cli_args = other_cli_args

    def run(self, discohort):
        original_work_dir = environ["BIOKEPI_WORK_DIR"]
        original_install_tools_path = environ.get("INSTALL_TOOLS_PATH", None)
        original_pyensembl_cache_dir = environ.get("PYENSEMBL_CACHE_DIR", None)
        try:
            environ["INSTALL_TOOLS_PATH"] = path.join(original_work_dir, "toolkit")
            environ["PYENSEMBL_CACHE_DIR"] = path.join(original_work_dir, "pyensembl-cache")
            work_dir_index = 0
            for i, patient in enumerate(discohort):
                environ["BIOKEPI_WORK_DIR"] = discohort.biokepi_work_dirs[work_dir_index]
                print("Setting BIOKEPI_WORK_DIR={}".format(environ["BIOKEPI_WORK_DIR"]))
                command = ["ocaml", self.ocaml_path]
                command.append("--{}={}".format(self.name_cli_arg, "{}_{}".format(self.name, patient.id)))
                for cli_arg, cli_arg_value in self.other_cli_args.items():
                    if type(cli_arg_value) == FunctionType:
                        cli_arg_value = cli_arg_value(patient)
                    command.append("--{}={}".format(cli_arg, cli_arg_value))
                print("Running {}".format(" ".join(command)))
                check_call(command)

                # TODO: Divide the number of patients by the number of work dirs. Test this logic.
                if (i + 1) % (len(discohort) / len(discohort.biokepi_work_dirs)) == 0:
                    work_dir_index += 1
        finally:
            environ["BIOKEPI_WORK_DIR"] = original_work_dir
            if original_install_tools_path:
                environ["INSTALL_TOOLS_PATH"] = original_install_tools_path
            else:
                del environ["INSTALL_TOOLS_PATH"]
            if original_pyensembl_cache_dir:
                environ["PYENSEMBL_CACHE_DIR"] = original_pyensembl_cache_dir
            else:
                del environ["PYENSEMBL_CACHE_DIR"]
