
import sys
import pandas as pd
import os
import re
from collections import defaultdict
import argparse

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

def main(args):
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
                # build data frame
                df = df.append(pd.DataFrame({
                    'runid': s.group(0),
                    'gene': requested_variant.gene(),
                    'requested': requested_variant.hgvs(),
                    'confirmed': confirmed_variant.hgvs()
                }, index=[0]), ignore_index=True)
    
    # write output
    df.to_csv(args.output if args.output else sys.stdout, sep=',', index=False)


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Extracts Sanger results calls from Excel Files (without HGVS conversion)")
    parser.add_argument("-d", "--directory", help="Search Folder containing XLSX", required=True)
    parser.add_argument("-o", "--output", help="output file (default to STDOUT)")

    args = parser.parse_args()

    main(args)