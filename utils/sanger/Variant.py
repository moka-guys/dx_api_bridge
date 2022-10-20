from csv import excel
import pandas as pd
from pandas.api.types import is_string_dtype
import os
import sys
import re
from copy import deepcopy
from collections import defaultdict, Counter
from itertools import zip_longest
from tqdm import tqdm
import xlrd

ROWS = {
    'result': ['Final Result', 'Final result'],
    'request': ['SNV variant confirmation', 'Variant checks']
}

class Manifest(object):
    '''
    File to parse for variants
    '''
    FIELDS = ['file','sample','request','result','comments']
    SEP = "\t"
    
    def __init__(self, file, files=None):
        self.file = file
        self.items = []
        # parse manifest
        if os.path.exists(file):
            with open(file) as fh:
                for line in fh:
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

    def filter(self):
        '''
        filter excel files for those containing variant results
        '''
        new_manifest = []
        for f in tqdm(self.items):
            try:
                if not f['sample']:
                    v = Excel(f['file'])
                    if v.fields is not None:
                        new_manifest.append(f)
            except:
                new_manifest.append(f)
        self.items = new_manifest

    def extract(self):
        '''
        extracts variant information fromo excel files
        '''
        new_manifest = []
        for f in tqdm(self.items):
            try:
                if not f['sample']:
                    v = Excel(f['file'])
                    if v.fields is not None:
                        for sample in v.fields:
                            new_f = deepcopy(f)
                            new_f['sample'] = sample
                            new_f['request'] = v.fields.loc['request',sample].replace('\n',';')
                            new_f['result'] = v.fields.loc['result',sample].replace('\n',';')
                            new_manifest.append(new_f)
                        print(f'--- {len(new_manifest)} ---')
            except:
                new_manifest.append(f)
        self.items = new_manifest
    
    def tidy(self):
        '''
        tidy variants (split multiple)
        '''
        new_manifest = []
        split_chars = ['&',' and ',';']
        tidy_fields = ['request','result']
        for f in tqdm(self.items):
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

    def validate(self, genes):
        '''adds statment of missing and malformed data (variants)'''
        validate_fields = ['request','result']
        # only required in single
        ## gene
        ## class

        error_counts = Counter()
        for f in tqdm(self.items):
            errors = []
            for field in validate_fields:
                field_data = f[field]
                v = Variant(field_data, genes)
                errors += list(map(lambda e: f'{field}-{e}', v.errors()))
            error_counts.update(errors)
            f['comments'] = "|".join(errors)
        for err, count in error_counts.items():
            print(f'{err}: {count}')


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
                # convert into variants
    
    def __len__(self):
        return len(self.fields.columns)

    def _parse_excel(self, file):
        # parse excel file
        try:
            excel_data = pd.read_excel(file)
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

        # get sample columns
        cols = [ i for i, col in enumerate(excel_data.columns) 
            if type(col)==str and re.match(r'NGS\w+_\d+_\w+_\w{2}',col) ]
        if cols:
            # aggregate request and result columns
            df = pd.DataFrame([], index=[])
            for row_type, row_names in ROWS.items():
                index_column = excel_data.columns[min(cols)-1]
                rows = excel_data.index[excel_data[index_column].isin(row_names) == True]
                # extract relevant rows
                try:
                    result = excel_data.iloc[rows,cols]
                    result.index = [row_type]
                except (IndexError, ValueError):
                    pass  # not unique row
                else:
                    df = pd.concat([df,result],axis=0)
            
            # cleanup column labels
            df = df.rename(lambda x: re.sub(r'^(NGS.+Pan\d+_S\d+).*$',r'\1',x), axis=1)
            # ensure all requested rows were extracted
            if len(ROWS) == len(df.index):
                # remove incompatible dtype columns
                df = df.select_dtypes(exclude=['number','datetime'])
                # select columns with HGVS.c
                hgvs_cols = [ df[col].str.contains('c\.\d').any() for col in df.columns ]
                return df.loc[:, hgvs_cols]
            elif len(df.index):
                print(f' ==> Check {self.file}')

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
        cl = re.search(r'class\s?(\d+)', cell, re.IGNORECASE)
        if cl:
            self._class = int(cl.group(1))
        # find gene (match with supplied list of valid symbols)
        self._gene = None
        for gene in genes:
            if gene in cell and re.search(f'\\b{gene}\\b', cell, re.IGNORECASE):
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

