# discohorts
Generate Cohorts based on Epidisco and/or Biokepi results

## Example Usage

```
from discohorts import Discohort
cohort = Discohort(cohort)
cohort.add_pipeline(name="dna_processing",
                    ocaml_path="dna_processing.ml",
                    other_cli_args={"reference-build": "b37decoy",
                                    "bam-path": lambda patient: patient.tumor_sample.bam_path_dna})
cohort.run_pipeline("dna_processing")
```
