from csv import excel
import pandas as pd
import numpy as np
from pandas.api.types import is_string_dtype
import os
import sys
import re
from copy import deepcopy
from collections import defaultdict, Counter
from itertools import zip_longest
from functools import reduce
from tqdm import tqdm
import xlrd

# ROWS = {
#     'result': ['Final Result', 'Final result', 'Variant 1', 'Variant 2', 'Variant 3'],
#     'request': ['SNV variant confirmation', 'Variant checks']
# }
ROWS = {
    'result': r'(Final Result|Final result|Variant 1|Variant 2|Variant 3)',
    'request': r'(SNV variant confirmation|Variant checks)'
}


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', 4)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 20)

def is_variant_string(s):
    return s is not np.nan and s and re.search(r'c\.\d',s)

def parser_unique_row(data,rows,cols,row_type):
    r = None
    try:
        assert len(rows)
        r = data.iloc[rows,cols]
        r.index = [row_type]
    except (IndexError, ValueError) as e:
        pass  # not unique row
    return r

def parser_block_rows(data,rows,cols,row_type):
    r = data.iloc[rows,cols]
    df = {}
    for sample in r:
        if r[sample].any():
            v = []
            for f in r[sample].dropna():
                if is_variant_string(f):
                    v.append(f)
            # if v:
            df[sample] = ';'.join(v)
    # return data frame with row_type as single index
    return pd.DataFrame(df,index=[row_type])

class Manifest(object):
    '''
    File to parse for variants
    '''
    FIELDS = ['file','sample','request','result','comments','request_genomic','result_genomic']
    SEP = "\t"
    
    def __init__(self, file, files=None):
        self.file = file
        self.items = []
        # parse manifest
        if os.path.exists(file):
            with open(file) as fh:
                for line in tqdm(fh):
                    if not line.startswith('#'):
                        f = line.rstrip().split(self.SEP)
                        self.items.append(dict(zip_longest(self.FIELDS,f,fillvalue='')))
        if files:
            # add files to manifest
            self.items += [ dict(zip_longest(self.FIELDS,[f],fillvalue='')) for f in files ]
    
    def __len__(self):
        return len(self.items)
    
    def commit(self):
        '''Overwrite manifest with new version'''
        with open(self.file,'w') as fh:
            for item in self.items:
                try:
                    fh.write(self.SEP.join([ str(item[f]) for f in self.FIELDS ])+'\n')
                except:
                    print(item)
                    raise

    def filter(self, fh=None):
        '''
        filter excel files for those containing variant results
        '''
        new_manifest = []
        for f in tqdm(self.items):
            try:
                if not f['sample']:
                    # attempt to parse
                    v = Excel(f['file'])
                    # if extractable add to manifest
                    assert v.fields is not None
                    new_manifest.append(f)
            except:
                # output files that could not be parsed
                failed_file = os.path.basename(f['file'])
                print(f'\nCould not parse {failed_file}')
                if fh:
                    print(f['file'], file=fh) 
                else:
                    # append to manifest if something goes wrong
                    new_manifest.append(f)
        self.items = new_manifest

    def extract(self):
        '''
        extracts variant information from excel files
        '''
        new_manifest = []
        for f in tqdm(self.items):
            if not f['sample']:
                # try reading excel file
                try:
                    v = Excel(f['file'])
                    assert v.fields is not None
                except:
                    new_manifest.append(f)
                    continue
                # put data in manifest
                for sample in v.fields:
                    new_f = deepcopy(f)
                    new_f['sample'] = sample
                    for field in ROWS.keys():
                        field_value = v.fields.loc[field, sample] if field in v.fields.index else ''
                        print(f'FV {field} {field_value}')
                        try:
                            new_f[field] = v.fields.loc[field, sample].replace('\n',';') if field in v.fields.index else ''
                        except:
                            pass
                    new_manifest.append(new_f)
                    print(f'--- {len(new_manifest)} ---')
            else:
                new_manifest.append(f)
        self.items = new_manifest

    def tidy(self):
        '''
        tidy variants (split multiple)
        '''
        new_manifest = []
        split_chars = ['&',' and ',';', ' + ']
        tidy_fields = ['request','result']
        for f in tqdm(self.items):
            # split fields
            split_fields = {}
            for field in tidy_fields:
                split_fields[field] = [f[field]]
                for sc in split_chars:
                    split_fields[field] = [ f2 for f1 in split_fields[field] for f2 in f1.split(sc) ]
                # remove non-HGVS fields
                split_fields[field] = list(filter(lambda x: re.search(r'c\.\d',x), split_fields[field]))
            
            # zip request and result together
            zipped_fields = list(zip_longest(*(split_fields[field] for field in tidy_fields), fillvalue=''))
            # write split variants (one line per variant)
            for variant in zipped_fields:
                new_f = deepcopy(f)
                new_f.update(dict(zip(tidy_fields, variant)))
                new_manifest.append(new_f)
        self.items = new_manifest
    
    def complete(self, genes):
        '''
        attempt to fix any errors and complete missing fields from file/path names
        '''
        validate_fields = ['request','result']
        for f in tqdm(self.items):
            previous_gene = ''  # when gene is defined in request -or- result
            for field in validate_fields:
                # remove leading and trailing spaces
                f[field] = f[field].strip()
                # get errors
                v = Variant(f[field], genes)
                e = v.errors()
                # fix invalid genes
                if 'invalid_gene' in e:
                    # attempt to get gene from file name
                    if 'CADASIL' in f['file']:
                        f[field] += ' NOTCH3'
                    elif 'FGFR3' in f['file']:
                        f[field] += ' FGFR3'
                    elif 'DMD' in f['file']:
                        f[field] += ' DMD'
                    elif 'CF' in f['file'].split('\\') or 'CFTR' in f['file'].split('\\'):
                        f[field] += ' CFTR'
                    # correct common mistakes/deprecations
                    elif 'KCN1A' in f[field]:
                        f[field] = f[field].replace('KCN1A','KCNA1')
                    elif 'MHY2' in f[field]:
                        f[field] = f[field].replace('MHY2','MYH2')
                    elif 'MHY7' in f[field]:
                        f[field] = f[field].replace('MHY7','MYH7')
                    elif 'CHEK 2' in f[field]:
                        f[field] = f[field].replace('CHEK 2','CHEK2')
                    elif 'BRCA 1' in f[field]:
                        f[field] = f[field].replace('BRCA 1','BRCA1')
                    elif 'BRCA 2' in f[field]:
                        f[field] = f[field].replace('BRCA 2','BRCA2')
                    elif 'ISPD' in f[field]:
                        f[field] = f[field].replace('ISPD','CRPPA')
                    elif 'SEPN1' in f[field]:
                        f[field] = f[field].replace('SEPN1','SELENON')
                    # attempt to get gene from previous field (request)
                    elif previous_gene:
                        f[field] += f' {previous_gene}'
                    e = Variant(f[field], genes).errors()
                    if 'invalid_gene' in e:
                        print('INVALID GENE ({}) in {}'.format(f[field], os.path.basename(f['file'].split('/')[-1])))
                # fix class
                if 'no_class' in e:
                    pass
                    print('NO CLASS ({}) in {}'.format(f[field], os.path.basename(f['file'].split('/')[-1])))
                if 'no_zygosity' in e:
                    pass
                    print('NO ZYGOSITY ({}) in {}'.format(f[field], os.path.basename(f['file'].split('/')[-1])))
                previous_gene = v._gene

    def validate(self, genes):
        '''adds statment of missing and malformed data (variants)'''
        validate_fields = ['request','result']
        error_counts = Counter()
        for f in tqdm(self.items):
            errors = []
            for field in validate_fields:
                v = Variant(f[field], genes)
                errors += list(map(lambda e: f'{field}-{e}', v.errors()))
            error_counts.update(errors)
            f['comments'] = "|".join(errors)
        for err, count in error_counts.items():
            print(f'{err}: {count}')

    def genomic(self, genes):
        '''adds genomic coordinates to variants'''
        for f in tqdm(self.items):
            for field in ['request','result']:
                v = Variant(f[field], genes)
                f[field+'_genomic'] = v.genomic()

    def unarchive(self):
        '''unarchive VCF file from DNAnexus (write ids to manifest)'''
        pass

    def extract(self):
        '''extract variants from VCF files'''
        pass

    def assess(self):
        '''assess variants for TP/TN/FP/FN'''
        pass


class Excel(dict):
    '''
    Dict of variants to confirm
    {}[]{}
    '''
    def __init__(self, file=None):
        '''Gets variants from file as dictionary'''
        self.file = file
        self.fields = None

        if file and os.path.exists(file):
            # Attempt field extraction
            try:
                raw_fields = self._parse_excel(file)
            except:
                print(f'{file} could not be parsed')
                raise
            # validate variant fields (is correct file)
            if raw_fields is not None:
                if len(raw_fields.columns):
                    self.fields = raw_fields
    
    def __len__(self):
        return len(self.fields.columns)

    def _parse_excel(self, file):
        # parse excel file
        try:
            # excel_data = pd.read_excel(file)
            # remove first line if empty
            excel_data_raw = pd.read_excel(file, header=None)
            first_row_empty = pd.isna(excel_data_raw).all()[[0]]
            if first_row_empty[0]:
                excel_data_raw.drop(index=[0],inplace=True)
            headers = excel_data_raw.iloc[0]
            excel_data  = pd.DataFrame(excel_data_raw.values[1:], columns=headers)
        except (ValueError):
            print(f' ** ValueError ** {file}')
            return
        except (PermissionError):
            print(f' ** PermissionError ** {file}')
            return
        except (xlrd.biffh.XLRDError):
            print(f' ** XLRDError ** {file}')
            return
        except (FileNotFoundError):
            print(f' ** FileNotFoundError ** {file}')
            return

        # batched result (multiple samples as a table with one column per sample)
        cols = [ i for i, col in enumerate(excel_data.columns) 
            if type(col)==str and re.search(r'NGS[^_]+_\d+_[^_]+_\w{2}',col) ]
        if cols:
            # get column that indexes the rows (identifies rows with variant result)
            index_column_index = min(cols)-1
            # aggregate request and result columns
            df = pd.DataFrame([], index=[])
            for row_type, row_names in ROWS.items():
                rows = excel_data.index[excel_data.iloc[:,index_column_index].str.match(row_names) == True]
                if not rows.empty:
                    result = None
                    # attempt unique row extraction
                    if len(rows) == 1:
                        # print('SINGLE ROW PARSING')
                        result = parser_unique_row(excel_data, rows, cols, row_type)
                    # try block extraction if consecutive rows
                    elif len(rows) == max(rows) - min(rows) + 1:
                        # print('BLOCK PARSING')
                        result = parser_block_rows(excel_data, rows, cols, row_type)
                    else:
                        # print('NO PARSER')
                        pass
                    # amend extracted fields
                    try:
                        assert result is not None
                    except AssertionError:
                        pass
                    else:
                        df = pd.concat([df, result], axis=0)

            # cleanup data
            if len(df.index):
                # remove incompatible dtype columns
                df = df.select_dtypes(exclude=['number','datetime'])
                # select columns with HGVS.c (remove samples with no results)
                hgvs_cols = [ df[col].str.contains('c\.\d').any() for col in df.columns ]
                df = df.loc[:, hgvs_cols]
                # cleanup column labels (retain sample name only)
                df = df.rename(lambda x: re.sub(r'.*\b(NGS\d+.+Pan\d+_S\d+).*$',r'\1',x), axis=1)
                # return extracted/constructed DataFrame
                return df

        # single sample result file (variant table with separate columns, sample name in filename)
        m = re.match(r'NGS[^_]+_\d+_[^_]+', os.path.basename(file))
        if m:
            # this parser should take into account files with single sample results, it's ugly, it's messy but should capture most cases...
            # extract full sample names from column header
            sample = None
            name_re = re.compile(r'(NGS[^_]+_\d+_[^_]+_[A-Z]{2}_[MFU]_[^_]+_Pan\d+)')
            # attempt to get unique sample name from header (first line)
            cols = set([ name_re.match(col).group(1) for col in excel_data.columns if type(col)==str and name_re.match(col) ])
            if len(cols) == 1:
                sample = list(cols)[0]
            # if failed, attempt to extract sample name from file name
            file_match = name_re.search(os.path.basename(file))
            if not sample and not cols and file_match:
                sample = file_match.group(1)
            # if sample determined (single sample) => extract data 
            if sample:
                # find results table (result only)
                i, j = None, None
                for i, col in enumerate(excel_data.columns):
                    j = None
                    try:
                        matched_rows = excel_data[col].str.match('(Final|Reported) Result') == True
                        matched_index = excel_data.index[matched_rows == True]
                        assert matched_index is not None
                        assert len(matched_index)==1
                    except:
                        pass
                    else:
                        j = matched_index[0]
                        break
                if i is not None and j is not None:
                    # find reported variants by line
                    combined_rows = []
                    for r in range(j+1,j+10):
                        if r < excel_data.shape[0]:
                            concatenated_row = []
                            for c in range(i,i+3):
                                if c < excel_data.shape[1]:
                                    cell = excel_data.iloc[r,c]
                                    if cell is np.nan:
                                        break  # empty cell breaks loop
                                    elif type(cell)==int:
                                        cell = f'(Class {cell})'  # integers are class number
                                    concatenated_row.append(cell)
                            joint_row = ' '.join(concatenated_row)
                            if is_variant_string(joint_row):
                                combined_rows.append(joint_row)
                    # build df
                    if combined_rows:
                        df = pd.DataFrame({sample: [';'.join(combined_rows), np.nan ]},index=['result', 'request'])
                        return df
    
    def __str__(self):
        rows = []
        if self.fields is not None:
            for col in self.fields:
                rows.append("\t".join([
                    self.file,
                    col,
                    str(self.fields.loc['request',col]),
                    str(self.fields.loc['result',col])
                ]))
        return "\n".join(rows)


class Variant(object):
    def __init__(self, cell, genes=[]):
        self.cell = cell

        # get HGVSc
        self._hgvs = re.search(r'(c\.\d+\S+)', cell)
        # get zygosity
        self._zygo = None
        zg = re.search(r'(het|hom|hemi)', cell, re.IGNORECASE)
        if zg:
            self._zygo = 2 if zg.group(1).lower() == 'hom' else 1  
        # find classification
        self._class = None
        cl = re.search(r'\bc(?:lass)?\s?([1-5])\b', cell, re.IGNORECASE)
        if cl:
            self._class = int(cl.group(1))
        # find gene (match with supplied list of valid symbols)
        self._gene = None
        for gene in genes:
            if (gene in cell or gene.lower() in cell) and re.search(f'\\b{gene}\\b', cell, re.IGNORECASE):
                self._gene = gene
                break

    def errors(self):
        errors = []
        # check non-empty
        if not self.cell:
            errors.append(f'no_data')
            return errors
        # check HGVSc
        if not self._hgvs:
            errors.append(f'no_hgvsc')
        # check gene
        if not self._gene:
            errors.append(f'invalid_gene')
        # check class
        if not self._class:
            errors.append(f'no_class')
        # check zygosity
        if not self._zygo:
            errors.append(f'no_zygosity')
        return errors

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

