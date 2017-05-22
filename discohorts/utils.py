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

import re
from glob import glob
from os import walk, path
import subprocess
import pandas as pd
import logging


def find_patient(patients, name, id_delims):
    """
    Given a file or folder name, determine whether any patient IDs are present
    inside it using the appropriate delimiters.

    Returns the corresponding patient ID or None.
    """

    def with_delims(patient_id):
        with_delims_list = []
        # ^<id>$, ^<id>_, _<id>$, _<id>_
        for delim in id_delims:
            for beginning in [delim, "^"]:
                for end in [delim, "$"]:
                    with_delims_list.append("{}{}{}".format(beginning, patient_id, end))
        return with_delims_list

    found_patients = []
    for patient in patients:
        for with_delim in with_delims(patient.id):
            if re.search(with_delim, name):
                found_patients.append(patient)

    if len(found_patients) == 1:
        return found_patients[0]
    elif len(found_patients) > 1:
        raise ValueError("Found multiple candidate patients for file/folder name {}: {}".format(
            name, found_patients))
    return None


def find_files_recursive(search_path, pattern):
    """
    Helper to traverse a path
    Returns list of full path to all files matching pattern
    """
    return [y for x in walk(search_path) for y in glob(path.join(x[0], pattern))]


def run_hlarp(results_dir, caller):
    """
    Call hlarp and return a DataFrame of results.

    caller is e.g. "optitype".
    """
    output = subprocess.check_output(["hlarp", caller, results_dir])
    hlarp_results = output.decode("utf-8").split("\n")
    header_line = hlarp_results[0].split(",")
    df = pd.DataFrame(columns=header_line)
    for i, line_str in enumerate(hlarp_results[1:]):
        if line_str == "":
            continue
        line = line_str.split(",")
        df.loc[i] = line
    return df


def get_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    if logger.handlers:
        logger.handlers = []
    logger.setLevel(level)
    return logger
