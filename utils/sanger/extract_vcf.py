#!/usr/bin/env python3

from collections import defaultdict
import sys
import os
import re
import csv
import argparse
import pandas as pd
from dxpy.exceptions import InvalidAuthentication
from dxlib import Dx
import pysam
import pyhgvs as hgvs
import pyhgvs.utils as hgvs_utils
from pyfaidx import Fasta
from tqdm.auto import tqdm
from functools import cache

SAMPLE_COLUMN = 'Run Name'
CHROM_COLUMN = 'Chr'
GDOT_COLUMN = 'genomics'
TRANSCRIPT_COLUMN = 'transcript'
HGVS_COLUMN = 'hgvs nom'
VALIDATED_GENOMIC = 'validated_genomic'
VARIANT_PADDING = 10
VCF_COLUMN = 'qual'
UNARCHIVED_COLUMN = 'unarchived'

'''Data read and write'''
class DataFile(object):
    def __init__(self, file):
        self.file = file
        data = pd.read_csv(file)
        if 'project-id' not in data:
            data['project-id'] = ''
        if 'vcf-id' not in data:
            data['vcf-id'] = ''
        if 'index-id' not in data:
            data['index-id'] = ''
        if 'vcf-name' not in data:
            data['vcf-name'] = ''
        if 'index-name' not in data:
            data['index-name'] = ''
        self.data = data
    
    def reload(self):
        self.data = pd.read_csv(self.file)

    def assign(self, sample, column, value):
        self.data.loc[self.data[SAMPLE_COLUMN] == sample,column] = value
    
    def has_column(self,colname):
        return colname in self.data.columns

    def samples(self,all=False):
        if all:
            return self.data[SAMPLE_COLUMN].unique()
        return self.data[self.data['vcf-id'] == ''][SAMPLE_COLUMN].unique()

    def commit(self,outfile=None):
        return self.data.to_csv(outfile if outfile else self.file, index=False)


def get_vcf_record(row, dx):
    """
    Get the vcf record from dnanexus unique file and project identfiers
    """
    # determine search window and variant alleles
    chrom, pos, ref, alt = row[VALIDATED_GENOMIC].split(':')
    start = int(pos) - 1 - VARIANT_PADDING
    end = start + len(ref) + VARIANT_PADDING
    url_vcf = dx.file_url(row['project-id'], row['vcf-id'])['url']
    url_tbi = dx.file_url(row['project-id'], row['index-id'])['url']
    if url_vcf and url_tbi:
        vcf = pysam.VariantFile(filename=url_vcf, index_filename=url_tbi)
        # get chromsome location
        records = vcf.fetch(chrom,start,end)
        for record in records:
            if (record.pos == int(pos) and record.ref == ref and ','.join(record.alts) == alt):
                f = dict(record.samples[0].items())
                i = dict(record.info.items())
                values = {
                    'GT': '/'.join(map(str,f['GT'])),
                    'GQ': f['GQ'],
                    'DP': f['DP'],
                    'AF': f['AD'][1]/sum(f['AD']),
                    'BaseQRankSum': i['BaseQRankSum'] if 'BaseQRankSum' in i else None,
                    'ExcessHet': i['ExcessHet'],
                    'QD': i['QD']
                }
                return pd.Series(dict(qual=record.qual, **values))
    return pd.Series()


def get_genomic_coordinates(row, babelfish):
    """
    Get the genomic coordinates from the HGVS.g / HGVS.c (validates)
    """
    try:
        return row[VALIDATED_GENOMIC]
    except:
        pass
    m = re.match(r'g\.(\d+)(\w+)>(\w+)',row[GDOT_COLUMN])
    chrom1 = row[CHROM_COLUMN]
    pos1, ref1, alt1 = m.groups()
    if row[TRANSCRIPT_COLUMN] and row[HGVS_COLUMN]:
        chrom2, pos2, ref2, alt2 = babelfish.get_genomic(f'{row[TRANSCRIPT_COLUMN]}:{row[HGVS_COLUMN]}')
    if int(pos1) == int(pos2) and ref1 == ref2 and alt1 == alt2:
        return ':'.join(map(str,[chrom2, pos2, ref2, alt2]))
    return ':'.join(map(str,[chrom1, pos1, ref1, alt1]))


# def get_dnanexus_resources(row, dx, suffix):
#     """
#     Get the dnanexus project and file identifiers
#     """
#     sample = row[SAMPLE_COLUMN]
#     search_pattern = f'{sample}.*{suffix}'        
#     files = dx.find_files(search_pattern, 'regexp', { "folder": args.folder })
#     # get all files and sense check for uniqueness (in case a project had to be rerun and uses the same sample identifiers)
#     for f in files:
#         # check if project matches
#         project = dx.get_project(f['project'])
#         if re.match(args.project, project['name']):
#             # add file and project id to relevant lines
#             file = dx.get_file(f['project'], f['id'])
#             file_type = 'index' if file['name'].endswith('.tbi') else 'vcf'
#             return pd.Series({
#                 f'{file_type}-id': file['id'],
#                 f'{file_type}-name': file['name'],
#                 'project-id': project['id'],
#                 'project-name': project['name']
#             })
#     return pd.Series()


def main(args):
    """
    Main function
    """
    # read CSV file
    df = DataFile(args.input)

    # init progress reporter for pandas operations
    tqdm.pandas()


    # conenct to DNAnexus
    try:
        dx = Dx(args.token)
    except InvalidAuthentication:
        print("Authentication token could not be validated")
        sys.exit(1)

    

    # find and assign files
    # dx_ids = df.data.progress_apply(get_dnanexus_resources, axis=1, args=(dx,args.suffix,))
    # df.data = df.data.join(dx_ids)
    # iterative version of the above with intermediary file writes
    print("Getting DNAnexus project and file IDs...")
    for sample in df.samples(args.force):
        search_pattern = f'{sample}.*{args.suffix}'        
        print(f'Searching for sample {search_pattern}...')
        files = dx.find_files(search_pattern, 'regexp', { "folder": args.folder })
        # get all files and sense check for uniqueness (in case a project had to be rerun and uses the same sample identifiers)
        print(files)
        for f in files:
            # check if project matches
            project = dx.get_project(f['project'])
            if re.match(args.project, project['name']):
                # add file and project id to relevant lines
                file = dx.get_file(f['project'], f['id'])
                print(file)
                print(sample, project['name'], file['folder'], file['name'])
                file_type = 'index' if file['name'].endswith('.tbi') else 'vcf'
                df.assign(sample, f'{file_type}-id', f['id'])
                df.assign(sample, f'{file_type}-name', file['name'])
                df.assign(sample, 'project-id', f['project'])
                df.assign(sample, 'project-name', project['name'])
                if args.unarchive and file['archivalState'] == 'archived':
                    if dx.unarchive(f['project-id'],f['id']):
                        df.assign(sample, UNARCHIVED_COLUMN, True)
        # save the intermediary result to file
        df.commit(args.output)
        print(df.data)
    
    # get validated genomic coordinates
    if not df.has_column(VALIDATED_GENOMIC):
        # get HGVS babelfish
        print('Loading refgene data...',end='')
        babelfish = HGVS(args.refgene, args.genome)
        print('Getting and validating genomic coordinates...')
        df.data[VALIDATED_GENOMIC] = df.data.progress_apply(get_genomic_coordinates, axis=1, args=(babelfish,))
        # df.commit(args.output)

    # get vcf records
    if not df.has_column(VCF_COLUMN):
        print('Getting VCF records...')
        vcf_data = df.data.progress_apply(get_vcf_record, axis=1, args=(dx,))
        df.data = df.data.join(vcf_data)
        df.commit(args.output)

    # rearchive if extracted
    if args.rearchive:
        print('-')
        print(df.data[df.data[UNARCHIVED_COLUMN] == True])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extracts Variant calls from DNAnexus projects")
    parser.add_argument("-i", "--input", help="Input file", required=True)
    parser.add_argument("-o", "--output", help="Output file (updates input by default)")
    parser.add_argument("--token", help="DNAnexus access token", required=True)

    parser.add_argument("--refgene", help="RefGene file for HGVS.c resolution", required=True)
    parser.add_argument("--genome", help="Genome file for HGVS.c resolution/validation", required=True)

    parser.add_argument("--force", help="Force file identification", action='store_true')
    parser.add_argument("--folder", help="File folder", default="/output")
    parser.add_argument("--project", help="Project name pattern", default="002_")
    parser.add_argument("--suffix", help="File suffix", default="_S\\d+_R1_001\\.vcf\\.gz")
    parser.add_argument("--unarchive", action="store_true", help="Unarchive to extract")
    parser.add_argument("--rearchive", action="store_true", help="Re-archive after data extraction")


    args = parser.parse_args()

    main(args)