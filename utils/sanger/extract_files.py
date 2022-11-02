from csv import excel
import sys
import pandas as pd
import os
import re
from collections import defaultdict
import argparse
from tqdm import tqdm
try:
    import win32com.client
except ImportError:
    print('Assuming POSIX system')
else:
    print('Assuming Windows system')

from Variant import Excel, Manifest

def readTarget(f):
    shell = win32com.client.Dispatch("WScript.Shell")
    shortcut = shell.CreateShortCut(f)
    return shortcut.TargetPath

def main(args):
    # add directories to be searched
    excel_files = []
    if args.directory:
        # create new manifest
        # assume top level a directory of symlinks
        paths = []
        for directory, subdirectories, files in os.walk(args.directory):
            paths += list([ os.path.join(directory,file) if not file.endswith('.lnk') else readTarget(os.path.join(directory,file)) 
                for file in files])
            break

        # walk through each root path extracting the excel files
        print('Finding Excel files...')
        for path in paths:
            for directory, _, files in os.walk(path):
                excel_files += list([ os.path.join(directory, file) for file in files if file.endswith(('.xls','.xlsx'))])
                print(f'{len(excel_files)} files', end='\r')

    # create manifest
    manifest = Manifest(file=args.manifest, files=excel_files)
    manifest.commit()

    # get excel files with results
    if args.filter:
        print(f'Filtering files...')
        try:
            with open(args.manifest+'.removed','w') as fh:
                manifest.filter(fh)
        except:
            raise
        finally:
            manifest.commit()


    # parse files that are not 
    if args.extract:
        try:
            manifest.extract()
        except:
            raise
        finally:
            manifest.commit()

    # tidy variant fields (split multiple variants)
    if args.tidy:
        try:
            manifest.tidy()
        except:
            raise
        finally:
            manifest.commit()

    # complete variant fields (infer missing fields)
    if args.complete:
        genes = set()
        with open(args.complete) as fh:
            for line in fh:
                f = line.split()
                genes.add(f[0])
        try:
            manifest.complete(genes)
        except:
            raise
        finally:
            manifest.commit()
    
    # validate variant fields (use supplied gene list)
    if args.validate:
        genes = set()
        with open(args.validate) as fh:
            for line in fh:
                f = line.split()
                genes.add(f[0])
        # run variant validation
        try:
            manifest.validate(genes)
        except:
            raise
        finally:
            manifest.commit()
    

    sys.exit()

    # extract data
    variants = []
    sample_count = 0
    for f in tqdm(excel_files):
        v = Variants(f)
        if v.fields is not None:
            variants.append(v)
            sample_count += len(v)
            print(f'--- {sample_count} ---')
    print(f'=== {sample_count} ===')
    
    sys.exit(1)
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
    parser.add_argument("-m", "--manifest", help="Read from MANIFEST", required=True)
    parser.add_argument("-d", "--directory", help="Search Folder containing XLSX")
    parser.add_argument("-f", "--filter", action='store_true', help="Filter excel files for results")
    parser.add_argument("-x", "--extract", action='store_true', help="Extract variants from excel files")
    parser.add_argument("-t", "--tidy", action='store_true', help="Tidy variants (split multiple)")
    parser.add_argument("-c", "--complete", help="Attempt to infer missing fields (use supplied gene list)")
    parser.add_argument("-v", "--validate", help="Validate variants (supply list of gene symbols)")
    parser.add_argument("-o", "--output", help="output file (default to STDOUT)")

    args = parser.parse_args()

    main(args)