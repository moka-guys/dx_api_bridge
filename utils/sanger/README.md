# Sanger data extraction and NGS based validation scripts

## extract_files.py
*** DEVELOPMENT ONLY ***
Extract sanger variant results from directory containing results in excel format (Variant reports)

## extract_vcf.py
*** DEVELOPMENT ONLY ***
Extract VCF calls from DNAnexus.

## extract_sanger.py
*** DEVELOPMENT ONLY ***
Extracts Sanger results from directory containing results in excel format

# Explain extraction pipeline

## Step 1 - find parsable excel files (variant reports)

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --filter`

Searches directoy VARIANT_REPORTS_DIR for excel variant reports. This file can contain a list of softlinked directories. Softlinks will be automatically followed on windows file systems.
The manifest file `MANIFEST_FILENAME` will be incrementally populated in the process (reused). It is advisable to create a backup of the file at each step.

## Step 2 - Extract HGVS.c variant notations from excel file manifest

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --extract`

This step extracts the variants (and classifications etc.) from the files notes in the MANIFEST_FILE.

## Step 3 - Tidy variant manifest (split fields with multiple variants into multiple lines)

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --tidy`

This step will split cells that contain multiple variants to multiple lines. The results will be a manifgest file with one variant per line.

## Step 4 - Complete missing fields

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --complete`

This will complete and correct some common errors encountered in the variant reports such as unecessary spaces in gene symbols, misspelled gene symbols or deprecated/former gene names.

## Step 5 - Validation

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --validate`

This step adds statments of missing and malformed data (variants) inthe manifest. It will alow filtering the manifest for complete datasets for the validation.


## Step 6 - Unwrap fields

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --unwrap FIELD_NAME`

Unwraps field (in column FIELDNAME) into separate columns (HGVS, Class, Gene name).

## Step 7 - Extract genomic coordinates

Calculates genomic coordinates from HGVS notation and writes to new column.

`extract_files.py -d VARIANT_REPORTS_DIR -m MANIFEST_FILENAME --genomic --genome REFERENCE_GENOME_FASTA --refgene REFGENE_FILE`

The REFGENE_FILE can be obtained from the UCSC MySQL server instance (or via their website). A script `get_refgene_data.sh` is supplied to achieve this easily.

> NB this is not yet fully implemented. 


## Step 8 - Find and unarchive relevant VCF files on DNAnexus
This had previously been implemented with the `extract_vcf.py` script. The same logic should be integrated into the `extract_files.py` script.

> NB this is not yet fully implemented. 

## Step 9 - Extract genomic coordinate recorded in manifest
Once the relevant files from the previous step have been unarchived, this step should query the relevant VCF positions from the sanger results in the manifest.
It should also extract relevant parameters (QUAL, GQ, DP, AF) that will allow to set empirical confidence thresholds for NGS variants (not requiring confirmation).

> NB this is not yet fully implemented. 

## Step 10 - Analyse concordance of variants from DNAnexus and variant report (sanger confirmation)
This should summarise the concordance between the Sanger result and the VCF and deduce thresholds for QUAL, GQ, DP and/or other parameters above which NGS variants do not require confirmation.

> NB this is not yet implemented. Use R?



