#!/usr/bin/env python

import os
import re
import sys
import json
import dxpy
import datetime

'''
Class to search for project
search for project
search for sample (by regex in project)
search for files belonging to sample
return current URIs for Data objects (VCF, TBI, BAM, BAI only)
'''

#                        Lib   Cnt   DNA   ID2    INITIALS    Sex        PNm   Pan
swift_sample_regex = r'((\w+)_(\d+)_(\w+)_(\w+_)?([A-Z]{2}_)?([MFU]x]_)?(\w+)_(Pan\d+))_'
tso_sample_regex = r'(.+)_UP\d+_.+\.vcf'
# where to find output files
DATA_FOLDERS = {
    '/output': swift_sample_regex, 
    '/analysis_folder/Results': tso_sample_regex
}
# Validity of generated URLs
URL_HOURS = 12


def get_sample_name(filename):
    '''
    Get sample name from filename
    
    Args:
        filename (str): filename

    Returns:
        str: sample name
    '''
    for regex in DATA_FOLDERS.values():
        m = re.match(regex, filename)
        if m:
            return m.group(1)

class Dx(object):
    def __init__(self,token):
        '''
        Initialize dxpy object

        Args:
            token (str): authentication token

        Returns:
            None
        '''
        sec_context = '{"auth_token":"' + token + '","auth_token_type":"Bearer"}'
        os.environ['DX_SECURITY_CONTEXT'] = sec_context
        dxpy.set_security_context(json.loads(sec_context))
        self.whoami = dxpy.api.system_whoami()

    def find_objects(self, name, mode='glob', *args, **kwargs):
        '''
        Finds all objects matching the given name

        Args:
            name (str): name of the object to find
            mode (str): mode of the search, can be 'glob', 'regex', 'exact'
            *args: additional arguments to pass to the search function
            **kwargs: additional keyword arguments to pass to the search function

        Returns:
            list: list of objects matching the given name

        '''
        m = re.match(r'(record|file|applet|workflow|database)-\w{24}$', name)
        if m:
            objects = []
            classname = m.group(1)
            for project in self.get_file_projects(name):
                if classname == 'file':
                    obj = self.get_file(project, name)
                    objects.append({
                        'id': obj['id'],
                        'project': project,
                        'describe': obj
                    })
                if classname == 'applet':
                    obj = self.get_applet(project, name)
                    objects.append({
                        'id': obj['id'],
                        'project': project,
                        'describe': obj
                    })
            return objects
        return list(dxpy.bindings.search.find_data_objects(name=name, name_mode=mode, *args, **kwargs))
    
    def find_projects(self, name, mode='glob', *args, **kwargs):
        '''
        Finds all projects matching the given name

        Args:
            name (str): name of the project to find
            mode (str): mode of the search, can be 'glob', 'regex', 'exact'
            *args: additional arguments to pass to the search function
            **kwargs: additional keyword arguments to pass to the search function

        Returns:
            list: list of projects matching the given name
        '''
        return list(dxpy.bindings.search.find_projects(name=name, name_mode=mode, describe=True, *args, **kwargs))

    def find_files(self, name, mode='glob', *args, **kwargs):
        '''
        Finds all files matching the given name
        
        Args:
            name (str): name of the file to find
            mode (str): mode of the search, can be 'glob', 'regex', 'exact'
            *args: additional arguments to pass to the search function
            **kwargs: additional keyword arguments to pass to the search function

        Returns:
            list: list of files matching the given name
        '''
        return list(dxpy.bindings.search.find_data_objects(classname="file",name=name,name_mode=mode, *args, **kwargs))

    def list_outputs(self,project_id):
        '''
        Finds all output files in a given project (swift and tso)

        Args:
            project_id (str): id of the project to search

        Returns:
            list: list of output files in the project
        '''
        # get project instance
        project = dxpy.bindings.dxproject.DXProject(dxid=project_id)
        # check if output folder exists
        subfolders = dxpy.bindings.dxfile_functions.list_subfolders(project_id, '/', recurse=True)
        matched_folders = list(filter(lambda f: any(f == df for df in DATA_FOLDERS), subfolders))
        # get files from matched folder
        files = []
        for folder in matched_folders:
            # sample_regex
            found_files = dxpy.bindings.search.find_data_objects(classname='file', state='closed', visibility='visible', \
                name=DATA_FOLDERS[folder], name_mode=u'regexp', project=project_id, folder=folder, recurse=True, \
                describe=True)
            files += list(found_files)
        # return files
        return files

    def get_project(self, project_id):
        '''
        Get project

        Args:
            project_id (str): id of the project to get

        Returns:
            dict: project descriptor
        '''
        project = dxpy.bindings.dxproject.DXProject(dxid=project_id)
        return project.describe()

    def get_file(self, project_id, file_id):
        '''
        Get file from project

        Args:
            project_id (str): id of the project
            file_id (str): id of the file

        Returns:
            dict: file descriptor
        '''
        remote_handler = dxpy.bindings.dxfile.DXFile(file_id, project_id)
        return remote_handler.describe()

    def get_applet(self, project_id, applet_id):
        '''
        Get applet from project

        Args:
            project_id (str): id of the project
            applet_id (str): id of the applet

        Returns:
            dict: applet description
        '''
        remote_handler = dxpy.bindings.dxapplet.DXApplet(applet_id, project_id)
        return remote_handler.describe()

    def unarchive(self, project_id, file_id):
        '''
        Unarchives a file

        Args:
            project_id (str): id of the project of the file
            file_id (str): id of the file to unarchive

        Returns:
            (bool, str): True if the file was successfully unarchived, False if failed, None if the file is not archived
        '''
        remote_handler = dxpy.bindings.dxfile.DXFile(file_id, project_id)
        if remote_handler.describe()['archivalState'] != 'live':
            try:
                remote_handler.unarchive()
                return True
            except dxpy.exceptions.PermissionDenied:
                return False

    def archive(self, project_id, file_id, all_copies=False):
        '''
        Archives a file
        
        Args:
            project_id (str): id of the project
            file_id (str): id of the file

        Returns:
            (bool): True if the file was archived, False if failed, None if the file is already archived
        '''
        remote_handler = dxpy.bindings.dxfile.DXFile(file_id, project_id)
        if remote_handler.describe()['archivalState'] == 'live':
            try:
                remote_handler.archive(all_copies=all_copies)
                return True
            except dxpy.exceptions.PermissionDenied:
                return False

    def update_project(self, project_id, **kwargs):
        '''
        Updates a project

        Args:
            project_id (str): id of the project to update
            **kwargs: keyword arguments to pass to the update function

        Returns:
            None                    
        '''
        project = dxpy.bindings.dxproject.DXProject(dxid=project_id)
        project.update(**kwargs)

    def file_url(self, project_id, file_id, valid_hours=URL_HOURS):
        '''
        Get an ephemeral URL of a file

        Args:
            project_id (str): id of the project the file is in
            file_id (str): id of the file
            valid_hours (int): number of hours the url is valid for

        Returns:
            str: url of the file
        '''
        remote_handler = dxpy.bindings.dxfile.DXFile(file_id, project_id)
        d = remote_handler.describe()
        try:
            file_url = remote_handler.get_download_url(duration=valid_hours*3600, preauthenticated=True, filename=d['name'], project=project_id)
        except dxpy.exceptions.InvalidState:
            return { "name": d['name'], "url": None, "state": d['archivalState'] }
        except Exception as e:
            raise e

        expires = datetime.datetime.now() + datetime.timedelta(hours=URL_HOURS)
        return { "name": d['name'], "url": file_url[0], "expires": expires.isoformat() }

    def find_executions(self, name, mode='glob', *args, **kwargs):
        '''
        Finds all executions matching the given name

        Args:
            name (str): name of the execution to find
            mode (str): mode of the search, can be 'glob', 'regex', 'exact'
            *args: additional arguments to pass to the search function
            **kwargs: additional keyword arguments to pass to the search function

        Returns:
            list: list of executions matching the given name
        '''
        return list(dxpy.bindings.search.find_executions(name=name, name_mode=mode, describe=True, *args, **kwargs))

    def get_file_projects(self, object_id, *args, **kwargs):
        '''
        Finds all projects that contain the given file

        Args:
            object_id (str): id of the file to find
            *args: additional arguments to pass to the search function
            **kwargs: additional keyword arguments to pass to the search function

        Returns:
            list: list of projects that contain the given file
        '''
        return list(dxpy.api.file_list_projects(object_id, input_params={}, always_retry=True, *args, **kwargs))

    def workstations(self, *args, **kwargs):
        '''
        Returns a list of all workstations on the platform

        Args:
            *args: additional arguments to pass to the search function
            **kwargs: additional keyword arguments to pass to the search function

        Returns:
            list: list of workstations
        '''
        workstations = []
        for app in dxpy.bindings.search.find_apps('cloud_workstation'):
            workstations += list(dxpy.bindings.search.find_executions(executable=app['id'], describe=True, *args, **kwargs))
        return workstations

    def project_file_ids(self, project_regex):
        '''
        Returns a deduplicated list of file ids for all files in one or multiple projects (matched by regex name)
        
        Args:
            project_regex (str): regex to match project name

        Returns:
            list of file ids
        '''
        # find project-ids whose membership is reason for exclusion from archival
        exclude_in_project = list(map(lambda x:  x['id'], self.find_projects(project_regex, 'regexp'))) \
            if project_regex else []
        # find all file ids in those projects (faster than querying the projects for each archival candidate)
        return list(set([ file_obj['id'] for p in exclude_in_project for file_obj in \
            self.find_files('.*', 'regexp', project=p, visibility='either')]))


if __name__=="__main__":
    dx = Dx(sys.argv[1])
    objects = dx.find_objects(sys.argv[2])
    for object in objects:
        print(json.dumps(object, indent=2))
    print(f'{len(objects)} objects found')