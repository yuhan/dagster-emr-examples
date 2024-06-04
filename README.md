# dagster-emr-examples

This repository contains different options to run EMR with Dagster.

## Set up: Raw EMR script + Spark script

Before working with Dagster, let's first set up the raw EMR script and Spark script. This is usually the starting point before adopting Dagster for orchestrating EMR jobs.

You have two scripts:

- `my_spark_script.py`: A simple Spark script that does data tranformation and writes data to S3.
- `run_emr_job.py`: A script that submits the Spark script to EMR.

### Upload the Script to S3

```bash
aws s3 cp my_spark_script.py s3://emr-testing-202406/my_spark_script.py
```

### Run the EMR Job

```bash
python run_emr_job.py
```

## Option 1: Keep EMR submit script and Spark script separate from Dagster

This uses the native Dagster Pipes to run your existing EMR script in a subprocess. This requires minimal changes to your existing scripts:

- Zero or minimal changes to the existing EMR submit script.

  - This change is not required. The original script can be used as is.
  - But the modified script provides better obersevability (see `run_emr_job_modified.py`). It streams stderr back to Dagster such as:

    <img width="703" alt="image" src="https://github.com/dagster-io/dagster/assets/4531914/bb83792e-079a-4b99-82c2-028ff2b46293">

- Zero changes to Spark script.

This is a Poor Man's EMR Pipes without needing to implemnt a full EMR Pipes client. While this doesn't provide the full feature set of Pipes, the main benefit of this approach is that you can use Dagster to orchestrate the EMR job with minimal changes to your existing scripts.

Further reading:

- Dagster Pipes + subprocess: https://docs.dagster.io/concepts/dagster-pipes/subprocess

## Option 2: Move EMR submit script to Dagster, keep Spark script separate

This doesn't use Dagster Pipes. Instead, the EMR submit script lives inside Dagster assets.

The benefit of this approach is that you can orchestrate the EMR job within Dagster together with other capabilities such as resources, and you have the flexiblty to model / break down your EMR script into different assets, for example:

<img width="1334" alt="image" src="https://github.com/dagster-io/dagster/assets/4531914/e2e28394-73d3-4c95-b1da-c3764f417472">

The cons are that you need to move the EMR submit script to Dagster and maintain it there.

## Option 3: Customize a Dagster Pipes + EMR, and keep the EMR submit script and Spark script separate

WIP: An exmaple implementation of Pipes + EMR integration.

Further reading

- Dagster Pipes: https://docs.dagster.io/concepts/dagster-pipes
- Customizing guide: https://docs.dagster.io/concepts/dagster-pipes/dagster-pipes-details-and-customization
