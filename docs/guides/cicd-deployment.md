# Deploy Guide (using DevOps CI\CD)

[Home](../index.md) > [Guides](cli-reference.md) > Deployment (using DevOps CI\CD)

Practical guide to deploy changes using DevOps yml scripts.

## Summary

This guide provides examples and explanations on how to use DevOps pipelines to deploy Fabric artefacts for an Ingenious project into a workspace.

## Sample yml files

Below are two sample yml files that can be used for DevOps pipelines and have been included under ingen_fab\project_templates:

| Script       | Description    |
|--------------|----------------|
|Sample_CICD_Initial.yml|Used for initial deployment of lakehouses and var_lib|
|Sample_CICD_Full.yml|Used for a full deployment|

## yml Scripts

The yml scripts are provied as samples and should be changed as required.

Scripts contain only a single 'DEV' stage, and should be extended to include other stages (UAT, Prod etc)

## Fabric workspace

This example require a fabric workspace called sample_dev to be created

## Keyvault configuration

This example requires a keyvault (fabricdevopskv), that contains two keys (DevOpsSCId,DevOpsSCSecret). These keys are used to store the devops service connection service principal values.

The service account needs to have rights to read the keyvault.

## DevOps Configuration

The following pre-requisites are required in DevOps:

| What       |Script example |Description    |
|--------------|----------------|----------------|
|Variable Group|Sample-Development|Varialbe (eg FABRIC_ENVIRONMENT, IS_SINGLE_WORKSPACE defaults|
|Environment|Sample-DEV|Fabric environment - no specific settings|
|Service Connection|SC_Fabric_Sample_Test|A service connection must be created that uses a service account|

In order to allow updated var_lib configuration set files to be checked back into the devops repo, the Project Build Service account needs to have Contribute rights.

Two pipelines should created using updated versions of the provided yml scripts.