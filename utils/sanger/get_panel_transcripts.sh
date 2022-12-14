#!/bin/bash

# this extracts the gene symbols and transcripts used

grep Gene_Accession ../../../mokabed/LiveBedfiles/*data.bed | cut -f1 -d: | sort -u | xargs -L1 cut -f14 | sort -u | grep ";" | sed 's/;/\t/g' | awk '{ split($2,tx,","); for (i in tx) print $1,tx[i] }' | sort -u > panel_genes
