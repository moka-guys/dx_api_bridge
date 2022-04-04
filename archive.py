
#!/usr/bin/env python3

from collections import defaultdict, Counter
import sys
import os
import re
import csv
import time
import argparse
import pandas as pd
from dxpy.exceptions import InvalidAuthentication
import dxpy
from app.dx import *
import pysam
import pyhgvs as hgvs
import pyhgvs.utils as hgvs_utils
from pyfaidx import Fasta
from tqdm.auto import tqdm
from functools import cache
from progress.bar import Bar

COST_COLUMNS = ['project-name', 'project-id', 'created', 'modified', 'dataUsage', 'archivedDataUsage', 'storageCost', 'billedTo', 'computeCost', 'estComputeCostPerSample']
COMPUTE_COLUMNS = ['job','launchedBy','workflowName','region','executableName','billTo','state','instanceType','totalPrice']
WORKSTATION_COLUMNS = ['id', 'region', 'billTo', 'state', 'launchedBy', 'instanceType', 'totalPrice']

DEPENDENCY_TAG = 'dependency'

def as_date(epoch):
    return time.strftime('%Y-%m-%dT', time.localtime(epoch/1000)),

# datafile
class DataFile(object):
    def __init__(self, file, columns=None):
        self.file = file
        self.columns = columns
        self.data = pd.DataFrame(columns=columns)
        if columns:
            for COL in columns:
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

# finds all workflows and applets
def flag_workflow_dependencies(workflow_id):
    # get workflow    


    for workflow in dxworkflow.find_executions(state='done', workflow=workflow_id):
        if workflow.get('executionContext') and workflow.get('executionContext').get('workflowDependencies'):
            for dep in workflow.get('executionContext').get('workflowDependencies'):
                if dep.get('id') == workflow_id:
                    datafile.assign(workflow.get('id'),DEPENDENCY_TAG,True)
                    break

def main(args):
    """
    Main function
    """

    # init progress reporter for pandas operations
    tqdm.pandas()

    # connect to DNAnexus
    try:
        dx = Dx(args.token)
    except InvalidAuthentication:
        print("Authentication token could not be validated")
        sys.exit(1)

    # time interval arguments
    after = f'-{args.after}' if args.after else None
    before = f'-{args.before}' if args.before else None

    # Workstation audit
    if args.workstations:
        wdf = DataFile(None, WORKSTATION_COLUMNS)
        # get workstations (workstation app executions)
        for app in dxpy.bindings.search.find_apps('cloud_workstation'):
            workstations = list(dxpy.bindings.search.find_executions(executable=app['id'],
                created_after=after,created_before=before, describe=True))
            print(f'Found {len(workstations)} workstations')
            for workstation in workstations:
                data = {}
                for col in WORKSTATION_COLUMNS:
                    data[col] = workstation['describe'][col]
                wdf.append(data)
        wdf.commit()
        sys.exit(0)

    # show orgs
    if args.orgs:
        orgs = list(dxpy.bindings.search.find_orgs({'level': 'MEMBER', 'describe': True}))
        for org in orgs:
            print(json.dumps(org, indent=2))
        sys.exit(0)

    # find data objects (files)
    if args.find:
        tags = args.tags.split(',') if args.tags else None
        fdf = DataFile(args.output)
        print(f'Looking for {args.type} {args.find}...')
        objects = []
        projects = {}
        if args.find.startswith('file-'):
            # search by id (e.g. file-G2x4bBj0Y9fvV1ZYPYbXFvqY)
            for project in dx.get_file_projects(args.find):
                file = dx.get_file(project, args.find)
                objects.append({
                    'id': file['id'],
                    'project': project,
                    'describe': file
                })
        else:
            # search by name
            objects = list(dx.find_objects(args.find, 'regexp', tags=tags, classname=args.type, describe=True, modified_after=after, modified_before=before, limit=args.limit, visibility='visible'))
        
        if args.type in ['file', 'applet', 'workflow']:
            # aggegate results
            fileids = set([])
            for f in tqdm(objects): 
                fileids.add(f['id'])
                if f["project"] not in projects.keys():
                    project = dx.get_project(f["project"])
                    projects[project['id']] = project
                p = projects[project['id']]
                fdf.append({
                    'object': f['id'],
                    'name': f['describe']['name'],
                    'state': f['describe']['state'] if 'state' in f['describe'] else None,
                    'created': as_date(f['describe']['created']),
                    'modified': as_date(f['describe']['modified']),
                    'archive': f['describe']['archivalState'] if 'archivalState' in f['describe'].keys() else None,
                    'size': f['describe']['size'] if 'size' in f['describe'].keys() else None,
                    'createdBy': f['describe']['createdBy']['user'],
                    'project': p['name'],
                    'projectCreatedBy': p['createdBy']['user'],
                    'billedTo': p['billTo'],
                    'region': p['region'],
                })
            fdf.commit()
            fdf.data.sort_values(by=['object'], inplace=True)
            print(f'There are {len(fileids)} unique in a total of {len(fdf.data)} files')

            # dependency analysis



        elif args.type == 'workflow':
            for workflow in objects:
                print(json.dumps(objects[0], indent=2))
        sys.exit(0)


    # match projects
    if args.match:
        # output file
        df = DataFile(args.output, COST_COLUMNS)
        if args.compute:
            cdf = DataFile(args.compute, COMPUTE_COLUMNS)
        
        # get projects
        projects = dx.find_projects(f'{args.match}', mode='regexp', created_after=after, created_before=before)
        print(f'Found {len(projects)} projects')  

        # audit
        project_bar = Bar('Projects', max=len(projects))     
        for project in projects:
            data = {
                'project-name': project['describe']['name'],
                'project-id': project['id'],
                'created': as_date(project['describe']['created']),
                'modified': time.strftime('%Y-%m-%d', time.localtime(project['describe']['modified']/1000)),
                'dataUsage': round(project['describe']['dataUsage'], 3),
                'archivedDataUsage': round(project['describe']['archivedDataUsage'], 3),
                'storageCost': round(project['describe']['storageCost'], 3) if 'storageCost' in project['describe'] else 0,
                'billedTo': project['describe']['billTo']
            }
            # compute cost
            if args.compute:
                # per project stats
                analyses = list(dxpy.bindings.find_executions(project=project['id'], classname='analysis', describe=True))
                workflow_counter = Counter()
                price_counter = Counter()
                for analysis in analyses:
                    name = analysis['describe']['executableName']
                    workflow_counter[name] += 1
                    price = analysis['describe']['totalPrice'] 
                    price_counter[name] += price
                    # detailed compute stats (per job)
                    for stage in analysis['describe']['stages']:
                        execution = stage['execution']
                        cdf.append({
                            'launchedBy': execution['launchedBy'],
                            'job': execution['id'],
                            'workflowName': analysis['describe']['executableName'],
                            'executableName': execution['executableName'],
                            'region': execution['region'],
                            'billTo': execution['billTo'],
                            'state': execution['state'],
                            'instanceType': execution['instanceType'],
                            'totalPrice': execution['totalPrice'],
                        })
                # summarise compute costs
                estComputeCostPerSample = 0
                computeCost = 0
                for name in workflow_counter:
                    estComputeCostPerSample += price_counter[name]/workflow_counter[name]
                    computeCost += price_counter[name]
                data['computeCost'] = round(computeCost, 3)
                data['estComputeCostPerSample'] = round(estComputeCostPerSample, 3)

            # write data
            df.append(data)
            project_bar.next()
        project_bar.finish()
        df.data.sort_values(by=['storageCost'], inplace=True)
        df.commit()
        if args.compute:
            cdf.commit()
    
        # report total storage cost
        sum_cost = df.data['storageCost'].sum()
        sum_data = df.data['dataUsage'].sum()
        sum_ark  = df.data['archivedDataUsage'].sum()
        sum_live = sum_data - sum_ark
        print(f'Archived size:      {sum_ark:12.3f} GB')
        print(f'Live size:          {sum_live:12.3f} GB\n')
        print(f'Total storage cost: ${sum_cost:10.2f}')
        if args.compute:
            sum_compute = df.data['computeCost'].sum()
            print(f'Total compute cost: ${sum_compute:10.2f}')
    
        # run archival
        if args.archive:
            project_bar = Bar('Projects', max=len(projects))     
            for project in projects:
                # archive files if any in live state
                if project['describe']['dataUsage'] != project['describe']['archivedDataUsage']:
                    closed_files = list(dx.find_files('.*', 'regexp', project=project['id'], state='closed', describe=True, modified_after=after, modified_before=before))
                    archivalstate_counter = Counter(map(lambda x: x['describe']['archivalState'], closed_files))
                    live_files = list(filter(lambda x: x['describe']['archivalState'] == 'live', closed_files))

                    # objects = list(dx.find_objects('.*', 'regexp', project=project['id'], describe=True))
                    # for object in objects:
                    #     if object['describe']['class'] == 'applet':
                    #         print(json.dumps(object,indent=2))
                    #         # print(object['describe']['class'])
                    # raise Exception("DEBUG STOP")

                    archival_files = list(filter(lambda x: x['describe']['archivalState'] == 'archival', closed_files))
                    if archival_files:
                        for archival_file in archival_files:
                            f = dxpy.bindings.dxfile.DXFile(dxid=archival_file['id'])
                            file_projects = dx.get_file_projects(archival_file['id'])
                            file_project_names = list(map(lambda x: dx.get_project(x)['name'], file_projects))
                            in_reference = any(file_project_name.startswith('001') for file_project_name in file_project_names)
                            print(f.describe()['name'], in_reference, ','.join(file_project_names))

                    # show archival state
                    if args.dryrun:
                        print(' ', archivalstate_counter)
                    elif live_files:
                        print('\nArchiving files in project {}'.format(project['describe']['name']))
                        if args.quick:
                            files = list(map(lambda x: x['id'], live_files))
                            chunks = [files[i:i + 1000] for i in range(0, len(files), 1000)]
                            for chunk in chunks:
                                dxpy.api.project_archive(project['id'], input_params={ 'files': chunk, 'allCopies': args.all }, always_retry=True)
                        else:
                            for file in tqdm(live_files):
                                dx.archive(file['project'], file['id'], args.all)

                # rename project
                if args.rename:
                    new_project_name = re.sub(args.match, args.rename, project['describe']['name'])
                    if not args.dryrun:
                        dx.update_project(project['id'], name=new_project_name)
                    else:
                        print(f'\nWould rename project {project["describe"]["name"]} to {new_project_name}', end='')
            
                project_bar.next()
            project_bar.finish()

    # compute audit
    # if args.compute:
    #     for result in dx.find_executions(state="done", project=proj_id, created_after="-2d"):
    #         print(f'Found job or analysis with object id {result["id"]}')
    #     # compute costs
    #     df.data.sort_values(by=['storageCost'], inplace=True)
    #     df.commit()
    #     df.data.to_csv(args.output, sep="\t", index=False)
    #     print(df.data)
    #     # compute costs
    #     df.data.sort_values(by=['storageCost'], inplace=True)
    #     df.commit()
    #     df.data.to_csv(args.output, sep="\t", index=False)
    #     print(df.data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project Archiving Tool")

    parser.add_argument("--token", help="DNAnexus access token", required=True)

    # audit (checks if archived folders have all files archived)
    parser.add_argument("-p", dest='match', help="Project name pattern (e.g. ^002_)", type=str) 
    parser.add_argument("--archive", action="store_true", help="Archives projects")
    parser.add_argument("--all", action="store_true", help="Forces archival of all copies of a given file (used with --archive)")
    parser.add_argument("--quick", action="store_true", help="Quick archival (used with --archive)")
    parser.add_argument("--rename", help="Rename matched pattern (e.g. 802_")
    parser.add_argument("--dryrun", action="store_true", help="Dry-run (used with --archive)")
    
    parser.add_argument("--after", help="Limit to projects created after (e.g. 12w ago)", default=None)
    parser.add_argument("--before", help="Created before (e.g. 2d ago)", default=None)
    parser.add_argument("--limit", help="Limit retuned objects", default=None, type=int)

    parser.add_argument("--output", help="Storage audit output file (defaults to STDOUT)")
    parser.add_argument('--compute', help="Compute audit output file (this takes some time)")

    # find objects
    parser.add_argument("-f", dest='find', help="Object name pattern (e.g. ^.*\.bam$)", type=str)
    parser.add_argument("--type", help='Object type ([file],applet,workflow,database,record)', default='file')
    parser.add_argument("--tags", help="Comma delimited file tags", type=str)

    # orgs
    parser.add_argument("-r", dest='orgs', action="store_true", help="List available DNAnexus orgs")

    # cloud workstations
    parser.add_argument("-w", dest='workstations', action="store_true", help="List available cloud workstations")
  
    # outputs
    args = parser.parse_args()

    main(args)