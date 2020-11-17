---
title: "AWS Lambda Batch and Trigger Parser"
date: 2020-11-17T15:22:13+13:00
draft: false
---

Lambdas are an ideal tool available on AWS for parsing files landing in S3 as part of an ETL pipeline. Setting up a parsing process with both catch-up (historical files previously landed in S3) and new file parsing functionality requires a bit of extra work. We have created a framework to quickly create a parsing solution with both of these two core functionalities and the code is available [here](https://github.com/hlamotte/aws-solutions/tree/main/batch-trigger-lambda-template).

# Solution details
The diagram below shows the architecture of the solution.

![lambda parsing solution](/lambda-parsing-solution.png)

Boilerplate code ensures the Lambda is compatible as both a batch and event triggered Lambda and the function `parse` in parse.py is where you define the parsing process. A Cloudformation template is used to create the required AWS services, and parameters such as source and target S3 bucket names are defined in this file.

A test environment is available to enable you to test your Lambda locally using the python unittest framework. This enables you to debug your Lambda code locally and deal with permissions issues later on upload.

A manifest, a list of all file paths on S3 requiring processing, is used for the catch-up functionality and a solution for creating a manifest csv file is documented in a previous blog [post](https://datamunch.tech/posts/s3-generate-manifest/).

Full usage instructions can be found on the project [GitHub](https://github.com/hlamotte/aws-solutions/tree/main/batch-trigger-lambda-template). This should provide a number of tools to speed up creating new parsing processes using Lambda.