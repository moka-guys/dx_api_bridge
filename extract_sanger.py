import sys
import pandas as pd
import os
import re
from collections import defaultdict
import argparse
import pyhgvs as hgvs
import pyhgvs.utils as hgvs_utils
from pyfaidx import Fasta

def get_variants(df):
    cols = [ i for i, col in enumerate(df.columns) if type(col)==str and re.match(r'NGS\w+_\d+_\w+_\w{2}',col) ]
    index_column = df.columns[min(cols)-1]
    rows = df.index[df[index_column].isin(['SNV variant confirmation','Final Result']) == True]
    result = df.iloc[rows,cols]
    result.index = ['request','result']
    return result


class Variant(object):
    def __init__(self,cell):
        self._zygo = re.search(r'(het|hom)', cell, re.IGNORECASE)
        self._hgvs = re.search(r'(c\.\d+\S+)', cell)
        self._gene = re.search(r'([A-Z][A-Z0-9]+)', cell)

    def __str__(self):
        if self._zygo and self._hgvs and self._gene:
            return '{} {} {}'.format(self._gene.group(1), self._hgvs.group(1), self._zygo.group(1).lower())
        else:
            return 'X'

    def check_result(self,other):
        if str(self) and not str(other):
            return 'FP'
        elif str(other) and not str(self):
            return 'FN'
        elif not str(self) and not str(other):
            return 'TN'
        elif str(self) == str(other):
            return 'TP'
    
    def gene(self):
        return self._gene.group(1) if self._gene else ''

    def hgvs(self):
        return self._hgvs.group(1) if self._hgvs else ''


class HGVS(object):
    def __init__(self, refgene, genome, genetranscripts) -> None:
        self.genome = Fasta(genome) 
        with open(refgene) as infile:
            self.transcripts = hgvs_utils.read_transcripts(infile)

        # read preferred transcripts
        self.genetranscripts = defaultdict(list)
        with open(genetranscripts) as infile:
            for line in infile:
                line = line.rstrip().split()
                self.genetranscripts[line[0]].append(line[1])
            

    def _get_transcript(self, tx):
        transcript = self.transcripts.get(tx)
        if not transcript:
            # extract transcript (following successive versioning)
            try:
                nm, version = tx.split('.')
            except ValueError as e:
                nm, version = tx, 1
            for i in range(int(version),int(version)+10):
                transcript = self.transcripts.get(f'{nm}.{i}')
                if transcript:
                    break
        return transcript

    def get_genomic(self, transcript, hgvsc):
        # if only gene known ifer from genomic from preferred transcript list
        if not transcript.startswith('NM_'):
            transcript_candidates = self.genetranscripts.get(transcript)
            genomic = list(map(lambda t: hgvs.parse_hgvs_name(f'{t}:{hgvsc}', self.genome, get_transcript=self._get_transcript),
                transcript_candidates))
            if len(genomic) > 1:
                try:
                    assert len(list(set(genomic))) == 1
                except AssertionError as e:
                    raise Exception('Cannot unambiguously determine transcript from gene name')
            return *genomic[0], transcript_candidates[0]
        # return genomic coordinate from single transcript
        chrom, offset, ref, alt = hgvs.parse_hgvs_name(f'{transcript}:{hgvsc}', self.genome, get_transcript=self._get_transcript)
        return chrom, offset, ref, alt, self._get_transcript(transcript)
        # return chrom, offset, ref, alt, transcript


def main(args):
    # load Babelfish
    print('Loading refgene data...',end='',file=sys.stderr)
    babelfish = HGVS(args.refgene, args.genome, args.genetranscripts)
    print('DONE',file=sys.stderr)

    # extract data
    dataframes = []
    for directory, subdirectories, files in os.walk(args.directory):
        for f in files:
            if f.endswith('.xlsx'):
                # read data
                a = pd.read_excel(os.path.join(directory,f))
                # get variants
                df = get_variants(a)
                dataframes.append(df)
    data = pd.concat(dataframes,axis=1)

    df = pd.DataFrame([],index=[])
    for sample in data.columns:
        if type(data[sample]['request']) == str:
            # get variant and assess FP/FN/TP/TN
            requested_variant = Variant(data[sample]['request'])
            confirmed_variant = Variant(data[sample]['result'])
            result = requested_variant.check_result(confirmed_variant)
            if requested_variant.hgvs():
                # get sample name constituents
                s = re.match(r'(\w+)_(\d+)_(\w+)_(\w{2})_([MFU])_([^_]+)_(Pan\d+)',sample)
                # get HGVS and transcript (guess)
                if requested_variant.gene() and requested_variant.hgvs():
                    chrom, pos, ref, alt, transcript = babelfish.get_genomic(requested_variant.gene(), requested_variant.hgvs())
                    genomics = f'g.{pos}{ref}>{alt}'
                # build data frame
                df = df.append(pd.DataFrame({
                    'Gene': requested_variant.gene(),
                    'DNA': s.group(3),
                    'Final result (HGVS)': confirmed_variant.hgvs(),
                    'Chr': chrom,
                    'genomics': genomics,
                    'transcript': 'NM_138701.3',
                    'hgvs nom': requested_variant.hgvs(),
                    'Real': result if result == 'TP' else '',
                    'False Pos': result if result == 'FP' else '',
                    'False Neg': result if result == 'FN' else '',
                    'Run Name': s.group(0),
                }, index=[0]), ignore_index=True)
    
    # write output
    df.to_csv(args.output if args.output else sys.stdout, sep=',', index=False)


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Extracts Variant calls from DNAnexus projects")
    parser.add_argument("-d", "--directory", help="Search Folder containing XLSX", required=True)
    parser.add_argument("-o", "--output", help="output file (default to STDOUT)")

    parser.add_argument("--refgene", help="RefGene file for HGVS.c resolution", required=True)
    parser.add_argument("--genome", help="Genome file for HGVS.c resolution/validation", required=True)
    parser.add_argument("--genetranscripts", help="Preferred transcripts")

    parser.add_argument("--force", help="Force file identification", action='store_true')
    parser.add_argument("--folder", help="File folder", default="/output")
    parser.add_argument("--project", help="Project name pattern", default="002_")
    parser.add_argument("--suffix", help="File suffix", default="_S\\d+_R1_001\\.vcf\\.gz")
    parser.add_argument("--unarchive", action="store_true", help="Unarchive to extract")
    parser.add_argument("--rearchive", action="store_true", help="Re-archive after data extraction")


    args = parser.parse_args()

    main(args)