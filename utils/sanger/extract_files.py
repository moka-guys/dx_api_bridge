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

from Variant import Excel, Manifest, HGVS

def readTarget(f):
    shell = win32com.client.Dispatch("WScript.Shell")
    shortcut = shell.CreateShortCut(f)
    return shortcut.TargetPath

def main(args):
    
    # read genes (and preferred transcripts)
    genes, transcripts = set(), defaultdict(list)
    with open(args.genes) as fh:
        for line in fh:
            f = line.split()
            genes.add(f[0])
            if len(f) > 1:
                transcripts[f[0]] += f[1].split(',')

    # load babelfish
    babelfish = None
    if args.refgene and args.genome:
        # load Babelfish
        print('Loading refgene data...',end='',file=sys.stderr)
        babelfish = HGVS(args.refgene, args.genome, transcripts)
        print('DONE',file=sys.stderr)

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
        try:
            manifest.complete(genes)
        except:
            raise
        finally:
            manifest.commit()
    
    # validate variant fields (use supplied gene list)
    if args.validate:
        # run variant validation
        try:
            manifest.validate(genes)
        except:
            raise
        finally:
            manifest.commit()
    
    # unwrap variants into fields
    if args.unwrap:
        # unwrap and decode HGVS
        try:
            manifest.unwrap(genes, args.unwrap)
        except:
            raise
        finally:
            manifest.commit()

    # decode HGVS
    if args.genomic:
        # unwrap and decode HGVS
        try:
            manifest.genomic(babelfish)
        except:
            raise
        finally:
            manifest.commit()


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Extracts Sanger results calls from Excel Files (without HGVS conversion)")
    parser.add_argument("-M", "--manifest", help="Read from MANIFEST", required=True)
    parser.add_argument("-G", "--genes", help="Read from GENELIST (acceptable gene symbols)", required=False)
    parser.add_argument("-N", "--genome", help="Reference genome", required=False)
    parser.add_argument("-R", "--refgene", help="Refgene file for HGVS conversion", required=False)
    # processing pipeline
    parser.add_argument("-d", "--directory", help="Search Folder containing XLSX")
    parser.add_argument("-f", "--filter", action='store_true', help="Filter excel files for results")
    parser.add_argument("-x", "--extract", action='store_true', help="Extract variants from excel files")
    parser.add_argument("-t", "--tidy", action='store_true', help="Tidy variants (split multiple)")
    parser.add_argument("-c", "--complete", action='store_true', help="Attempt to infer missing fields")
    parser.add_argument("-v", "--validate", action='store_true', help="Validate variants")
    parser.add_argument("-u", "--unwrap", metavar='FIELD', help="Unwrap variant from FIELD")
    parser.add_argument("-g", "--genomic", action='store_true', help="Infer genomic coordinates (requires -N and -R")

    parser.add_argument("--vcf_find", metavar='TOKEN', help="Find results VCF file")
    parser.add_argument("--vcf_unarchive", metavar='TOKEN', help="Unarchive VCF files")
    parser.add_argument("--vcf_archive", metavar='TOKEN', help="Archive VCF files")


    args = parser.parse_args()

    main(args)