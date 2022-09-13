# DNAnexus file bridge and tools

This is a simple webservice that allows fast retrieval of data object URIs for use in IGV etc.
It also provides helpers to manage archival processes and perform cost audits.
## run.py
Starts the DNAnexus file service whioch provides a simple HTTP interface to get files by sample and project.
This is also the default application ran by the docker image.

### Available API routes
All API routes only support GET requests.
A token must be provided in the authentication header (Bearer XXXXXXXXX)

`/whoami` Returns the users identity based on the supplied authentication token.

`/project` Returns the project list

`/project/<string:dx_project>` Return the samples in a given project

`/url/<string:dx_project>/<string:dx_file>` Return the ephemeral URL for a given file in a project/sample.

## dxarc.py
This API native helper functions to manage file archival.
If performing archiving and/or renamin options ensure the script will have the expected effect by supplying the `--dryrun` option.

### Example use cases
#### Archive all production TSO500 runs older than 3 months, excluding files that are also in 001_ToolsReferenceData

`python dxarc.py --token XXXXXXX -f --project "^002(_.+TSO.*)$" --before 12w --visibility visible --notin "^001_Tool" --archive --rename "802\1"`

- `--token XXXX` Provide a DNAnexus access token
- `-f --project "^002_.+TSO"` Find project matichin pattern of any name in project starting with __001_Tool__
- `--before 12w` Only return projects created more than 12 weeks ago and files that have not been modified for 12 weeks
- `--visibility hidden` Only return files that are hidden
- `--notin "^001_Tool"` Excludes any files that ar also in any project matching the search regular expression
- `--archive` Archive found objects
- `--rename "802$1"` Renames projects with this pattern (used in conjunction with `--project`).

#### Unarchivng all files that are also in hidden 001_ToolsReferenceData 
THis can be used to ensure any shared resources in 001 are live of they are also on any other project.

`python dxarc.py --token XXXXXXX -f --object ".*" --type file --project "^001_Tool" --visibility hidden --follow --output reference_data.tsv --unarchive`

- `--token XXXX` Provide a DNAnexus access token
- `-f --objects '.*' --type file --project "^001_Tool"` Find files of any name in project starting with __001_Tool__
- `--visibility hidden` Only return files that are hidden
- `--follow` also return the same files in other projects
- `--output reference_data.tsv` Writes objects summarty to file (before any updates)
- `--unarchive` Unarchive found objects

#### Show storage and compute costs for all development projects and write analysis level compute cost audit

`python dxarc.py --token XXXXXXX -f --project "^003_" --compute compute_audit.tsv`

- `--token XXXX` Provide a DNAnexus access token
- `-f --objects '.*' --type file --project "^001_Tool"` Find files of any name in project starting with __001_Tool__
- `--compute compute_audit.tsv`

The output from the compute cost audit can be visualised with the included R script `compute_plot.R`.

e.g. `Rscript compute_plot.R compute_audit.tsv compute_audit.pdf`
