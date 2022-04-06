#!/usr/bin/env python

import re
import flask
from flask import request, jsonify, Response
from dxpy.exceptions import InvalidAuthentication
from functools import wraps
from collections import defaultdict
from .dx import Dx, get_sample_name

app = flask.Flask(__name__)
app.config["DEBUG"] = True

'''
checks token and injects Dx instance
'''
def authenticate(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        auth = request.headers.get('authorization')
        m = re.match(r'Bearer (\S+)',auth)
        if m:
            try:
                dx = Dx(m.group(1))
            except InvalidAuthentication:
                return Response('Invalid authentication token', 401, {'WWW-Authenticate': 'Basic realm="dnanexus"'})
            else:
                return f(dx, *args, **kwargs)
        return Response('No authentication token supplied', 401, {'WWW-Authenticate': 'Basic realm="dnanexus"'})
    return wrapper


'''returns user name for submitted token'''
@app.route('/whoami', methods=['GET'])
@authenticate
def status(dx):
    return jsonify(dx.whoami)


@app.route('/project', methods=['GET'])
@authenticate
def projects(dx):
    search = request.args.get('search','002_')
    mode = request.args.get('mode','glob')
    result = list(map(lambda x: x['describe'],dx.find_projects(search,mode)))
    return jsonify(result)

'''returns output files grouped by sample (according to GSTT naming scheme)'''
@app.route('/project/<string:dx_project>', methods=['GET'])
@authenticate
def project(dx, dx_project):
    files = dx.list_outputs(dx_project)
    grouped = defaultdict(list)
    for f in files:
        sample_name = get_sample_name(f['describe']['name'])
        if sample_name:
            grouped[sample_name].append(f)
    result = []
    for sample, files in grouped.items():
        result.append({ "name": sample, "files": list(map(lambda x: x['describe'], files)) })
    return jsonify(result)

'''
returns file URL
'''
@app.route('/url/<string:dx_project>/<string:dx_file>', methods=['GET'])
@authenticate
def file(dx, dx_project, dx_file):
    return jsonify(dx.file_url(dx_project, dx_file))

if __name__=="__main__":
    app.run(host="0.0.0.0", port=80)


