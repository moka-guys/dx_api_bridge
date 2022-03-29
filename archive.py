
#!/usr/bin/env python3

from collections import defaultdict, Counter
import sys
import os
import re
import csv
import argparse
import pandas as pd
from dxpy.exceptions import InvalidAuthentication
from app.dx import *
import pysam
import pyhgvs as hgvs
import pyhgvs.utils as hgvs_utils
from pyfaidx import Fasta
from tqdm.auto import tqdm
from functools import cache
from progress.bar import Bar

COLUMNS = ['project-name', 'project-id', 'dataUsage', 'archivedDataUsage', 'storageCost', 'billedTo']

# datafile
class DataFile(object):
    def __init__(self, file):
        self.file = file
        self.data = pd.DataFrame(columns=COLUMNS)
        for COL in COLUMNS:
          if COL not in self.data:
            self.data[COL] = ''
    
    def reload(self):
        self.data = pd.read_csv(self.file)

    def append(self,dict):
        self.data = self.data.append(dict, ignore_index=True)

    def commit(self):
        if self.file:
          return self.data.to_csv(self.file, sep="\t", index=False)
        print(self.data)


def main(args):
    """
    Main function
    """
    # read CSV file
    # df = DataFile(args.input)

    # init progress reporter for pandas operations
    tqdm.pandas()

    # conenct to DNAnexus
    try:
        dx = Dx(args.token)
    except InvalidAuthentication:
        print("Authentication token could not be validated")
        sys.exit(1)

    # output file
    df = DataFile(args.output)

    # match projects
    if args.match:
        age = f'-{args.age}' if args.age else None
        projects = dx.find_projects(f'{args.match}', mode='regexp', created_after=age)
        print(f'Found {len(projects)} projects')  

        # audit
        project_bar = Bar('Projects', max=len(projects))     
        for project in projects:
            df.append({
                'project-name': project['describe']['name'],
                'project-id': project['id'],
                'dataUsage': round(project['describe']['dataUsage'], 3),
                'archivedDataUsage': round(project['describe']['archivedDataUsage'], 3),
                'storageCost': round(project['describe']['storageCost'], 3) if 'storageCost' in project['describe'] else 0,
                'billedTo': project['describe']['billTo']
            })
            project_bar.next()
        project_bar.finish()
        df.data.sort_values(by=['storageCost'], inplace=True)
        df.commit()
    
        # report total storage cost
        sum_cost = df.data['storageCost'].sum()
        sum_data = df.data['dataUsage'].sum()
        sum_ark  = df.data['archivedDataUsage'].sum()
        sum_live = sum_data - sum_ark
        print(f'Archived size: {sum_ark}')
        print(f'Live size:     {sum_live}')
        print(f'Total storage cost: {sum_cost}')
        
        # run archival
        if args.archive:
            project_bar = Bar('Projects', max=len(projects))     
            for project in projects:
                if project['describe']['dataUsage'] != project['describe']['archivedDataUsage']:
                    # archive files
                    for file in dx.find_files('.*', 'regexp',project=project['id'], state='closed', describe=True):
                        if file['describe']['state'] == 'closed' and file['describe']['archivalState'] == 'live':
                            if not args.dryrun:
                                dx.archive(file['project'], file['id'])
                            else:
                                print(f'\nWould archive {file["describe"]["name"]}', end='')

                # rename project
                if args.rename:
                    new_project_name = re.sub(args.match, args.rename, project['describe']['name'])
                    if not args.dryrun:
                        dx.update_project(project['id'], name=new_project_name)
                    else:
                        print(f'\nWould rename project {project["describe"]["name"]} to {new_project_name}', end='')
            
                if args.dryrun:
                    print()
                project_bar.next()
            project_bar.finish()

    # compute audit
    if args.compute:
        # compute costs
        df.data.sort_values(by=['storageCost'], inplace=True)
        df.commit()
        df.data.to_csv(args.output, sep="\t", index=False)
        print(df.data)
        # compute costs
        df.data.sort_values(by=['storageCost'], inplace=True)
        df.commit()
        df.data.to_csv(args.output, sep="\t", index=False)
        print(df.data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project Archiving Tool")

    # audit (checks if archived folders have all files archived)
    parser.add_argument("-p", dest='match', help="Project name pattern (e.g. ^002_)", type=str) 
    parser.add_argument("--archive", action="store_true", help="Archives projects")
    parser.add_argument("--dryrun", action="store_true", help="Dry-run")
    parser.add_argument("--rename", help="Rename matched pattern (e.g. 802_")
    
    parser.add_argument("-c", dest='compute', action="store_true", help="Compute cost audit")

    parser.add_argument("--age", help="Minimum age of project (e.g. 12w)", type=str)
    parser.add_argument("--token", help="DNAnexus access token", required=True)
  
    # datafile format
    parser.add_argument("-o", "--output", help="Output file (updates input by default)")

    args = parser.parse_args()

    main(args)