import re
import logging
import argparse
import pandas as pd
from datetime import datetime

from cryosparc.tools import CryoSPARC

from objects.job_import_v47 import MoviesImportJobs



def raw_files(cs, file_name='', **kwargs):
    print(f"Connection was established: {cs.test_connection()}")
    
    jobs = []
    live_jobs = []
    proj_list = cs.cli.list_projects()
    print(f"Number of projects in total - {len(proj_list)}")
    for i, p in enumerate(proj_list):
        temp_jobs = cs.cli.get_jobs_by_type(project_uid=p["uid"], types=['import_movies'])
        for j in temp_jobs:
            job = cs.cli.get_job(p["uid"], j["uid"])
            star_mark = False
            for ws in job["workspace_uids"]:
                if cs.cli.check_workspace_exists(p["uid"], ws):
                    starred = "starred_by" in cs.cli.get_workspace(p["uid"], ws)
                    star_mark = star_mark | starred
            if len(j.get("errors_run", [])) == 0 and job["params_spec"]:
                jobs.append(MoviesImportJobs.create_from_jobs(job, p, star_mark))
        
        live_temp_jobs = cs.rtp.get_all_sessions_in_project(p["uid"])
        live_jobs += [MoviesImportJobs.create_from_live_jobs(j, p, cs) for j in live_temp_jobs if len(j.get("errors", [])) == 0]
        if i % 20 == 0:
            print(f"Number of projects scanned - {i+1}")
    
    df_jobs = pd.DataFrame([i.model_dump() for i in jobs+live_jobs])

    file_dump = file_name if file_name else f"cryosparc_{datetime.now().strftime('%Y%m%d')}"
    with pd.ExcelWriter(f'{file_dump}.xlsx', engine='openpyxl') as writer:
        df_jobs.to_excel(writer, index=False)


def rejected_exposures(remote_cs, project, job, file_name='', **kwargs):
    # we need to track back the import job, since rejected_exposures location is shown in respect
    # to the import file directory

    ### Find all ancestor import jobs
    ancestor_jobs = remote_cs.cli.job_find_ancestors(project_uid=project, job_uid=job)
    print(f"Ancestor jobs | {ancestor_jobs}")
    imp_job = remote_cs.cli.get_jobs_by_type(project_uid=project, types=['import_movies', 'live_session'])
    imp_job_names = [i['uid'] for i in imp_job]
    print(f"Import jobs | {imp_job}")
    ancestor_job_name = list(set(ancestor_jobs) & set(imp_job_names))
    print(f"Jobs to inspect | {ancestor_job_name}")
    
    job_prefixes = {}
    for jb in ancestor_job_name:
        ### Find location on imported files
        job_type = imp_job[imp_job_names.index(jb)]['type']
        project_full = remote_cs.cli.get_project(project)
        if job_type == 'import_movies':
            job_full = remote_cs.cli.get_job(project, jb)
            import_params = MoviesImportJobs.create_from_jobs(job_full, project_full, False)
            name_dataset = jb
        else:
            session = remote_cs.cli.get_job(project, jb)['params_spec']['session_uid']['value']
            job_full = remote_cs.rtp.get_session(project, session)
            import_params = MoviesImportJobs.create_from_live_jobs(job_full, project_full, remote_cs)
            name_dataset = session
        prefix_name = import_params.data_project.rsplit('/', maxsplit=1).pop(0)
        job_prefixes[name_dataset] = prefix_name
        
    ### Iterate over all rejected exposure files and combine names
    job_check = remote_cs.find_job(project, job)
    curated_list = job_check.list_files()
    download_list = [i for i in curated_list if re.search(r"J\d+_passthrough_exposures_(manual_)?rejected.cs", i)]
    file_names = []
    for file in download_list:
        job_check_cs = job_check.download_dataset(file)
        for row in job_check_cs.filter_fields(['movie_blob/path', 'uid']).to_records():
            split_name = row[1].rsplit('/')
            job_attr, exposure_name = split_name[0], split_name[-1]
            if str(row[0]) in exposure_name:
                exposure_name = exposure_name.split('_', maxsplit=1).pop()
            if '.mrc' not in exposure_name:
                file_names.append(f"{job_prefixes[job_attr]}/{exposure_name}")

    file_dump = file_name if file_name else f"rejected_exposures_{datetime.now().strftime('%Y%m%d')}"
    with open(f"{file_dump}.txt", "w") as f:
        f.writelines(f"{name}\n" for name in file_names)


if __name__ == "__main__":
    shared = argparse.ArgumentParser('Logs parser', add_help=False)
    shared.add_argument(
        "--licence",
        type=str,
        required=True,
        help="CryoSPARC licence",
    )
    shared.add_argument(
        "--host",
        type=str,
        required=True,
        help="CryoSPARC web URL, e.g., http://localhost:39000",
    )
    shared.add_argument("--port", type=int, required=False, default=39000)
    shared.add_argument("--email", type=str, required=True, help="login email")
    shared.add_argument("--password", type=str, required=True, help="login password")

    parser = argparse.ArgumentParser(parents=[shared])
    subparsers = parser.add_subparsers(dest="method", required=True)

    raw_file = subparsers.add_parser("raw_files")
    raw_file.add_argument("--file_name", type=str, required=False, help="name of the file to save")

    curated = subparsers.add_parser("rejected_exposures")
    curated.add_argument("--project", type=str, required=True, help="CryoSPARC project number in format 'PX'.")
    curated.add_argument("--job", type=str, required=True, help="CryoSPARC job 'Manually curated exposures' in format 'JX'.")
    curated.add_argument("--file_name", type=str, required=False, help="Name of the file to save")

    kwargs = vars(parser.parse_args())
    method = kwargs.pop("method")
    try:
        cs = CryoSPARC(
                    license=kwargs['licence'],
                    host=kwargs['host'],
                    base_port=kwargs['port'],
                    email=kwargs['email'],
                    password=kwargs['password']
                )
        func = globals().get(method)
        if func is None:
            raise ValueError(f"Unknown method: {method}")
        func(cs, **kwargs)
    except Exception as exception:
        logging.exception("Unexpected error: {}".format(exception))