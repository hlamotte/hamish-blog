---
title: "AWS generate S3 objects manifest using python"
date: 2020-10-30T15:34:55+13:00
draft: false
comments: true
tags: ['aws']
author: "Hamish Lamotte"
showToc: true
hidemeta: false
---

AWS S3 Batch Operations is a solution to quickly process large quantities of ETL data by invoking a Lambda Function, however you first need to create a [manifest](https://docs.aws.amazon.com/AmazonS3/latest/dev/batch-ops-basics.html#specify-batchjob-manifest) file describing all the objects you want to process. I couldn't find any quick solutions to easily create these manifests online so I put together a solution in Python. You can find the GitHub code [here](https://github.com/hlamotte/aws-solutions).

## Instructions to generate an S3 Manifest CSV
For creating a csv manifest list of all files in an S3 bucket with a certain prefix and suffix. 
- Compatible for use with S3 Batch Operations.
- Manfest uses bucket, key schema.
- Manifest is uploaded to a target location on S3.

Executing from the CLI:
```bash
$ python generate_manifest.py bucket_name prefix suffix manifest_bucket manifest_key
```

Example:
```bash
$ python generate_manifest.py my-bucket path/to/data/2020-09-24/ .xml manifest-bucket 2020-10-26_manifest.csv
```

