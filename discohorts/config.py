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


class Config(object):
    def __init__(self, discohort, **kwargs):
        """
        Override (or create from scratch) any method with either a f(patient)
        function or a value.

        Any overriding value will be converted into a f(patient) function.

        Any argument to the CLI needs to be prefixed with "arg_".
        """
        self.discohort = discohort
        for key, value in kwargs.items():
            self.update(key, value)

    def update(self, key, value):
        if key == "given_work_dir":
            raise ValueError("Cannot replace given_work_dir in __init__")

        if type(value) == FunctionType:
            self.__dict__[key] = value
        else:
            self.__dict__[key] = lambda patient: value

    def keep(self, patient):
        return True

    def anonymous_args(self, patient):
        return []

    def given_work_dir(self, patient, work_dir):
        # This method is a little special/different. It should not be replaced
        # in __init__ becauase it isn't of the form f(patient).
        pass

    def arg_name(self, patient):
        return None

    def arg_results_path(self, patient):
        return self.discohort.dest_results_dir

    def arg_reference_build(self, patient):
        return "b37"


class EpidiscoConfig(Config):
    def arg_normal_input(self, patient):
        assert patient.normal_sample is not None, "Patient {} has no normal sample".format(patient)
        return patient.normal_sample.bam_path_dna

    def arg_tumor_input(self, patient):
        assert patient.tumor_sample is not None, "Patient {} has no tumor sample".format(patient)
        return patient.tumor_sample.bam_path_dna

    def arg_rna_input(self, patient):
        assert patient.tumor_sample is not None, "Patient {} has no tumor sample".format(patient)
        if patient.tumor_sample.bam_path_rna is not None:
            return patient.tumor_sample.bam_path_rna
        return None

    def arg_with_optitype_normal(self, patient):
        return True

    def arg_with_optitype_tumor(self, patient):
        return True

    def arg_with_optitype_rna(self, patient):
        return self.arg_rna_input(patient) is not None

    def arg_with_seq2hla(self, patient):
        return self.arg_rna_input(patient) is not None

    def arg_with_kallisto(self, patient):
        return self.arg_rna_input(patient) is not None
