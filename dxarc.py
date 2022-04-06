
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
from pyfaidx import Fasta
from tqdm.auto import tqdm
from functools import cache
from progress.bar import Bar

WORKSTATION_COLUMNS = ['id', 'region', 'billTo', 'state', 'launchedBy', 'instanceType', 'totalPrice']

COST_COLUMNS = ['project-name', 'project-id', 'created', 'modified', 'dataUsage', 'archivedDataUsage', 'storageCost', 'billedTo', 'computeCost', 'estComputeCostPerSample']
COMPUTE_COLUMNS = ['job','launchedBy','workflowName','region','executableName','billTo','state','instanceType','totalPrice']

DEPENDENCY_TAG = 'dependency'

def as_date(epoch):
    '''
    Converts UNIX epoch to date string
    input:
        epoch: int
        output: str
    '''
    return time.strftime('%Y-%m-%dT d %H:%M:%S', time.localtime(epoch/1000))

# Pandas DataFrame bases csv output (stdout or file)
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


def remove_if_in_project(file_ids, project_regex):
    pass

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

    # argument transformations (time intervals, tag lists)
    after = f'-{args.after}' if args.after else None
    before = f'-{args.before}' if args.before else None
    tags = args.tags.split(',') if args.tags else None
    
    # workstations
    if args.workstations:
        df = DataFile(args.output, WORKSTATION_COLUMNS)
        workstations = dx.workstations(after=after, before=before)
        # get workstations (workstation app executions)
        print(f'Found {len(workstations)} workstations')
        for workstation in workstations:
            data = {}
            for col in WORKSTATION_COLUMNS:
                data[col] = workstation['describe'][col]
            df.append(data)
        df.commit()
        sys.exit(0)

    # show orgs
    if args.orgs:
        orgs = list(dxpy.bindings.search.find_orgs({'level': 'MEMBER', 'describe': True}))
        for org in orgs:
            print(json.dumps(org, indent=2))
        sys.exit(0)

    # datafile (output)
    df = DataFile(args.output)

    # find data objects (files)
    if args.find:
        # object centred search
        if args.object:
            # limit by single project
            project = None
            if args.project:
                projs = dx.find_projects(args.project, mode='regexp')
                if len(projs) != 1:
                    print(f'Found {len(projs)} projects matching {args.project}, expected 1')
                    sys.exit(1)
                project = projs[0]['id']
                print(f'Found project {projs[0]["describe"]["name"]} ({project})')
            # find objects
            objects = list(dx.find_objects(args.object, mode='regexp', project=project, describe=True, \
                visibility=args.visibility, tags=tags, classname=args.type, \
                modified_after=after, modified_before=before, limit=args.limit))
            print(f'Found {len(objects)} objects matching {args.object}')
            
            # follow objects into other projects (finds other isntances of found files) e.g. allows to find files ina project and then tag/archive etc all copies of it
            if args.follow:
                followed_objects = []
                print(f'Following {len(objects)} objects into other projects...')
                for object in tqdm(objects):
                    file_projects = dx.get_file_projects(object['id'])
                    for p in file_projects:
                        if p != object['project']:
                            f = dx.get_file(p, object['id'])
                            followed_objects.append({'id': f['id'], 'project': f['project'], 'describe': f})
                objects += followed_objects
                print(f'Added {len(followed_objects)} objects from other projects')

            # remove excluded objects
            if args.notin:
                exclude_files = dx.project_file_ids(args.notin)
                not_excluded_objects = list(filter(lambda x: x['id'] not in exclude_files, objects))
                print(f'Removed {len(objects) - len(not_excluded_objects)} objects as they are contained in {args.notin}')
                objects = not_excluded_objects

            # print found objects
            projects = {}
            fileids = set([])
            print(f'Finding projects...',file=sys.stderr)
            for f in tqdm(objects):
                fileids.add(f['id'])
                # fetch file project
                if f["project"] not in projects.keys():
                    projects[f['project']] = dx.get_project(f["project"])
                p = projects[f['project']]
                # write file info
                df.append({
                    'object': f['id'],
                    'name': f['describe']['name'],
                    'state': f['describe']['state'] if 'state' in f['describe'] else None,
                    'visibility': 'hidden' if f['describe']['hidden'] else 'visible',
                    'archive': f['describe']['archivalState'] if 'archivalState' in f['describe'].keys() else None,
                    'tags': ','.join(f['describe']['tags']) if 'tags' in f['describe'].keys() else None,
                    'folder': f['describe'].get('folder'),
                    'created': as_date(f['describe']['created']),
                    'modified': as_date(f['describe']['modified']),
                    'size': f['describe']['size'] if 'size' in f['describe'].keys() else None,
                    'createdBy': f['describe']['createdBy']['user'],
                    'project': p['name'],
                    'project-id': p['id'],
                    'projectCreatedBy': p['createdBy']['user'],
                    'billedTo': p['billTo'],
                })
            df.commit()
            df.data.sort_values(by=['object'], inplace=True)
            print(f'There are {len(fileids)} unique in a total of {len(df.data)} files')

            # archiving
            if args.archive:
                files = list(filter(lambda x: x['describe']['class'] == 'file' and x['describe']['archivalState'] == 'live', objects))
                print(f'Archiving {len(files)}...', file=sys.stderr)
                for file in tqdm(files):
                    if args.dryrun:
                        print(f'Would archive {file["id"]} in {file["project"]}...')
                    else:
                        if not dx.archive(file['project'], file['id'], args.all):
                            print(f'Failed to archive {file["id"]} in {file["project"]}. Check permissions.')
            elif args.unarchive:
                files = list(filter(lambda x: x['describe']['class'] == 'file' and x['describe']['archivalState'] != 'live', objects))
                print(f'Unarchiving {len(files)}...', file=sys.stderr)
                for file in tqdm(files):
                    if args.dryrun:
                        print(f'Would archive {file["id"]} in {file["project"]}...')
                    else:
                        if not dx.unarchive(file['project'], file['id']):
                            print(f'Failed to unarchive {file["id"]} in {file["project"]}. Check permissions.')

            # tagging
            if args.tag or args.untag:
                tags = args.tag.split(',') if args.tag else None
                untags = args.untag.split(',') if args.untag else None
                print(f'Changing tags for {len(objects)} objects (+{args.tag} -{args.untag})...', file=sys.stderr)
                for obj in tqdm(objects):
                    classname = obj['describe']['class']
                    if tags:
                        tag_fun = f'{classname}_add_tags'
                        if args.dryrun:
                            print(f'Would tag {obj["id"]} in {obj["project"]} with {args.tag}...')
                        else:
                            change_tag = getattr(dxpy.api, tag_fun)
                            change_tag(obj["id"], { 'tags': tags, 'project': obj["project"] })
                    if untags:
                        tag_fun = f'{classname}_remove_tags'
                        if args.dryrun:
                            print(f'Would untag {obj["id"]} in {obj["project"]} with {args.untag}...')
                        else:
                            change_tag = getattr(dxpy.api, tag_fun)
                            change_tag(obj["id"], { 'tags': untags, 'project': obj["project"] })
        
        # project centred (no files/objects specified)
        elif args.project:
            # get projects
            projects = dx.find_projects(f'{args.project}', mode='regexp', created_after=after, created_before=before)
            print(f'Found {len(projects)} projects')  

            # audit
            for project in tqdm(projects):
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
                # compute cost audit for projects
                if args.compute:
                    cdf = DataFile(args.compute, COMPUTE_COLUMNS)
                    # per project stats
                    analyses = list(dxpy.bindings.find_executions(project=project['id'], classname='analysis', describe=True))
                    workflow_counter = Counter()
                    price_counter = Counter()
                    for analysis in analyses:
                        name = analysis['describe']['executableName']
                        workflow_counter[name] += 1
                        price = analysis['describe']['totalPrice'] 
                        price_counter[name] += price
                        ### detailed compute stats (per job)
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
                print(f'Archiving {len(projects)} projects...', file=sys.stderr)
                # get file ids from projects
                exclude_files = dx.project_file_ids(args.notin)
                # iterate over projects
                projects_iterator = tqdm(projects)
                for project in projects_iterator:
                    # archive files if any in live state
                    if project['describe']['dataUsage'] != project['describe']['archivedDataUsage']:
                        # find closed filed for archival
                        projects_iterator.set_description('Finding files to archive...')
                        closed_files = list(dx.find_files('.*', 'regexp', project=project['id'], describe=True,
                            visibility=args.visibility, tags=tags, state='closed', 
                            modified_after=after, modified_before=before))
                        # remove files from archival list if in referenced projects
                        projects_iterator.set_description(f'Excluding files in protected projects ({len(closed_files)})...')
                        safe_files = list(filter(lambda x: x['id'] not in exclude_files, closed_files))
                        # remove non-live files
                        projects_iterator.set_description(f'Filtering for live files ({len(safe_files)})...')
                        live_files = list(filter(lambda x: x['describe']['archivalState'] == 'live', safe_files))
                        # show archival state
                        if args.dryrun:
                            print(f'\nWould archive {len(live_files)} in {project["describe"].get("name")}')
                        elif live_files:
                            # archive files in chunks (max 1000 per API call)
                            files = list(map(lambda x: x['id'], live_files))
                            chunks = [files[i:i + 1000] for i in range(0, len(files), 1000)]
                            for i, chunk in enumerate(chunks):
                                projects_iterator.set_description(f'Archiving file chunk {i+1}/{len(chunks)} of {project["describe"]["name"]}...')
                                dxpy.api.project_archive(project['id'], input_params={ 'files': chunk, 'allCopies': args.all }, always_retry=True)

                    # rename project
                    if args.rename:
                        new_project_name = re.sub(args.project, args.rename, project['describe']['name'])
                        if not args.dryrun:
                            dx.update_project(project['id'], name=new_project_name)
                        else:
                            print(f'Would rename project {project["describe"]["name"]} to {new_project_name}')
                

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Project Archiving Tool")
    
    parser_global = parser.add_argument_group('Global Options')
    parser_global.add_argument("--token", help="DNAnexus access token", required=True)
    parser_global.add_argument("--output", help="Output file (defaults to STDOUT)", default=None)

    parser_main = parser.add_argument_group('Main Options')
    parser_commands = parser_main.add_mutually_exclusive_group(required=True)
    parser_commands.add_argument('-w', dest='workstations', action='store_true', help='Get Workstation info')
    parser_commands.add_argument('-r', dest='orgs', action='store_true', help="Get Organisation info")
    parser_commands.add_argument('-f', dest='find', action='store_true', help="Find projects/objects")
    
    parser_find = parser.add_argument_group('Find options')
    parser_find.add_argument("--project", dest='project', help="Project name pattern (e.g. ^002_)", type=str, default=None) 
    parser_find.add_argument("--object", dest='object', help="Object name regex (e.g. ^.*\.bam$)", type=str, default=None)
    parser_find.add_argument("--type", help='Object type ([file],applet,workflow,database,record)', default='file')
    parser_find.add_argument("--after", help="Limit to projects/objects created after (e.g. 12w ago)", default=None)
    parser_find.add_argument("--before", help="Limit to projects/objects created before (e.g. 2d ago)", default=None)
    parser_find.add_argument("--visibility", help='Filter by object visibility ([either],hidden,visible)', default='either', choices=['either', 'hidden', 'visible'])
    parser_find.add_argument("--limit", help="Limit retuned objects", default=None, type=int)
    parser_find.add_argument("--tags", help="Require at least one tag (comma-delimited)", type=str)
    parser_find.add_argument("--notin", help="Exclude file if in project (regex)", type=str, default=None)
    parser_find.add_argument("--follow", help="Also return the matching files in all projects", action='store_true')
    
    parser_archiving = parser_find.add_argument_group('Archiving')
    parser_archiving.add_argument("--archive", action="store_true", help="Archives projects/files")
    parser_archiving.add_argument("--all", action="store_true", help="Forces archival of all copies of a given file (used with --archive)")
    parser_archiving.add_argument("--rename", help="Rename projects matched pattern (e.g. 802_). Only effective when archviing projects!", type=str, default=None)
    parser_archiving.add_argument("--dryrun", action="store_true", help="Dry-run (used with --archive)")
    
    parser_unarchiving = parser_find.add_argument_group('Unarchiving')
    parser_unarchiving.add_argument("--unarchive", action="store_true", help="Unarchives projects/files")

    parser_updating = parser_find.add_argument_group('Updating')
    parser_updating.add_argument("--tag", help="Add file tags", metavar="TAG1,TAG2,...", type=str)
    parser_updating.add_argument("--untag", help="Remove file tags", metavar="TAG1,TAG2,...", type=str)
    
    parser_audit = parser_find.add_argument_group('Audit')
    parser_audit.add_argument("--compute", metavar='FILE', help="Compute cost audit")


    # outputs
    args = parser.parse_args()

    if args.find and not (args.project or args.object):
        parser.error("--project and/or --object is required with -f")

    main(args)