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
DATA_FOLDERS = { '/output': swift_sample_regex, '/analysis_folder/Results': tso_sample_regex }
URL_HOURS = 12


def get_sample_name(filename):
    for regex in DATA_FOLDERS.values():
        m = re.match(regex, filename)
        if m:
            return m.group(1)

class Dx(object):
    def __init__(self,token):
        sec_context = '{"auth_token":"' + token + '","auth_token_type":"Bearer"}'
        os.environ['DX_SECURITY_CONTEXT'] = sec_context
        dxpy.set_security_context(json.loads(sec_context))
        self.whoami = dxpy.api.system_whoami()

    def find_projects(self, name, mode='glob'):
        return list(dxpy.find_projects(describe=True, name=name, name_mode=mode))

    def find_files(self, name, mode='glob'):
        return list(dxpy.bindings.search.find_data_objects(classname="file",name=name,name_mode=mode))

    def list_outputs(self,project_id):
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

    def file_url(self, project_id, file_id):
        remote_handler = dxpy.bindings.dxfile.DXFile(file_id, project_id)
        d = remote_handler.describe()
        try:
            file_url = remote_handler.get_download_url(duration=URL_HOURS*3600, preauthenticated=True, filename=d['name'], project=project_id)
        except dxpy.exceptions.InvalidState:
            return { "name": d['name'], "url": None, "state": d['archivalState'] }
        except Exception as e:
            raise e

        expires = datetime.datetime.now() + datetime.timedelta(hours=URL_HOURS)
        return { "name": d['name'], "url": file_url[0], "expires": expires.isoformat() }
