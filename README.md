# discohorts
Generate Cohorts based on Epidisco and/or Biokepi results

## Example Usage

```
from discohorts import Discohort
cohort = Discohort(cohort)
cohort.run(pipeline_name="dna_processing",
           ocaml_pipeline_path="dna_processing.ml",
           other_cli_args={"reference-build": "b37decoy",
                           "bam-path": lambda patient: patient.tumor_sample.bam_path_dna})
```
