# discohorts
Generate Cohorts based on Epidisco and/or Biokepi results

## Example Usage

```
from utils.data import load_cohort
from discohorts import Discohort

cohort = load_cohort(min_tumor_mtc=0, min_normal_mtc=0)
work_dirs = ["/nfs-pool-{}/biokepi/".format(i) for i in range(2, 17)]
dest_results_dir = "/nfs-pool/biokepi/results"
cohort = Discohort(
    cohort,
    biokepi_work_dirs=work_dirs,
    dest_results_dir=dest_results_dir)

cohort.add_epidisco_pipeline(
    pipeline_name="epidisco_1")

# Customize the normal BAM input.
cohort.add_epidisco_pipeline(
    pipeline_name="epidisco_2",
    config=EpidiscoConfig(cohort,
                          arg_normal_input=lambda patient: patient.normal_sample.bam_path_dna))

# Customize the normal BAM input and the run name.
cohort.add_epidisco_pipeline(
    pipeline_name="epidisco_3",
    run_name=lambda patient: "epidisco_{}".format(patient.id),
    config=EpidiscoConfig(cohort,
                          arg_normal_input=lambda patient: patient.normal_sample.bam_path_dna))

# Customize the normal BAM input, the run name, and which patients to run on.
cohort.add_epidisco_pipeline(
    pipeline_name="epidisco_4",
    run_name=lambda patient: "epidisco_{}".format(patient.id),
    config=EpidiscoConfig(cohort,
                          keep=lambda patient: patient.id == "468",
                          arg_normal_input=lambda patient: patient.normal_sample.bam_path_dna))

# Same as #4, but written differently.
class EpidiscoConfigModified(EpidiscoConfig):
    def keep(self, patient):
        return patient.id == "468"
    def arg_normal_input(self, patient):
        return patient.tumor_sample.bam_path_dna
modified_config = EpidiscoConfigModified(cohort)
cohort.add_epidisco_pipeline(
    pipeline_name="epidisco_5",
    config=modified_config)
    
# Customize any CLI argument; for example, picard_java_max_heap_size.
cohort.add_epidisco_pipeline(
    pipeline_name="epidisco_6",
    config=EpidiscoConfig(cohort,
                          arg_picard_java_max_heap_size="20g"))

# This is Discohort's own dry run functionality, FYI. Should have a better name.
cohort.run_pipeline("epidisco_1", dry_run=True)
```
