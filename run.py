import os
import re
import logging
import argparse
import subprocess
import pandas as pd
from datetime import datetime

from cryosparc.tools import CryoSPARC

from objects.job_import import MoviesImportJobs



def raw_files(remote_cs, file_name='', **kwargs):
    jobs = remote_cs.api.jobs.find(type=['import_movies', 'import_micrographs'], limit=500)
    jobs_live = remote_cs.api.jobs.find(type=['live_session'], limit=500)
    jobs_list = [MoviesImportJobs.create_from_jobs(j, remote_cs) for j in jobs if len(j.build_errors) == 0]
    jobs_live_list = [MoviesImportJobs.create_from_live_jobs(j, remote_cs) for j in jobs_live if j.spec.params.session_uid != j.workspace_uids[0]]
    df_jobs = pd.DataFrame([i.model_dump() for i in jobs_list])
    df_jobs_live = pd.DataFrame([i.model_dump() for i in jobs_live_list])
    df_all_jobs = pd.concat([df_jobs, df_jobs_live], axis=0)

    file_dump = file_name if file_name else f"cryosparc_{datetime.now().strftime('%Y%m%d')}"
    with pd.ExcelWriter(f'{file_dump}.xlsx', engine='openpyxl') as writer:
        df_all_jobs.to_excel(writer, index=False)


def rejected_exposures(remote_cs, project, job, file_name='', **kwargs):
    # we need to track back the import job, since rejected_exposures location is shown in respect
    # to the import file directory

    ### Find all ancestor import jobs
    ancestor_jobs = remote_cs.api.jobs.find_ancestor_uids(project, job)
    ancestor_jobs_details = remote_cs.api.jobs.find(
                                                    project_uid=project, 
                                                    uid=ancestor_jobs, 
                                                    type=['import_movies', 'live_session']
                                                )
    ### Find location on imported files
    if ancestor_jobs_details[0].spec.type == 'import_movies':
        import_params = MoviesImportJobs.create_from_jobs(ancestor_jobs_details[0], remote_cs)
    else:
        import_params = MoviesImportJobs.create_from_live_jobs(ancestor_jobs_details[0], remote_cs)
    prefix_name = import_params.data_project.rsplit('/', maxsplit=1).pop(0)
    ### Iterate over all rejected exposure files and combine names
    job_check = remote_cs.find_job(project, job)
    curated_list = job_check.list_files()
    download_list = [i for i in curated_list if re.search(r"J\d+_passthrough_exposures_(manual_)?rejected.cs", i)]
    file_names = []
    for file in download_list:
        job_check_cs = job_check.download_dataset(file)
        for row in job_check_cs.filter_fields(['movie_blob/path', 'uid']).to_records():
            exposure_name = row[1].rsplit('/', maxsplit=1).pop()
            if str(row[0]) in exposure_name:
                exposure_name = exposure_name.split('_', maxsplit=1).pop()
            file_names.append(f"{prefix_name}/{exposure_name}")

    file_dump = file_name if file_name else f"rejected_exposures_{datetime.now().strftime('%Y%m%d')}"
    with open(f"{file_dump}.txt", "w") as f:
        f.writelines(f"{name}\n" for name in file_names)


if __name__ == "__main__":
    shared = argparse.ArgumentParser('Logs parser', add_help=False)
    shared.add_argument(
        "--url",
        type=str,
        required=True,
        help="CryoSPARC web URL, e.g., http://localhost:39000",
    )
    shared.add_argument("--email", type=str, required=True, help="login email, prompts when unspecified")
    shared.add_argument("--ssl_cert", type=str, required=False, default="", help="where to locate the ssl certificate file")

    parser = argparse.ArgumentParser('Logs parser', parents=[shared])
    subparsers = parser.add_subparsers(dest="method", required=True)

    raw_file = subparsers.add_parser("raw_files")
    raw_file.add_argument("--file_name", type=str, required=False, help="Name of the file to save")

    curated = subparsers.add_parser("rejected_exposures")
    curated.add_argument("--project", type=str, required=True, help="CryoSPARC project number in format 'PX'.")
    curated.add_argument("--job", type=str, required=True, help="CryoSPARC job 'Manually curated exposures' in format 'JX'.")
    curated.add_argument("--file_name", type=str, required=False, help="Name of the file to save")

    args = parser.parse_args()
    kwargs = vars(args)
    method = kwargs.pop("method")
    try:
        logging.info(kwargs.get('ssl_cert', ''))
        os.environ["SSL_CERT_FILE"] = kwargs.get('ssl_cert')
        remote_cs = CryoSPARC(kwargs['url'], email=kwargs['email'])
        if not remote_cs.test_connection():
            subprocess.run(["python3", "-m", "cryosparc.tools", "login", "--url", kwargs["url"], "--email", kwargs["email"]])
        func = globals().get(method)
        if func is None:
            raise ValueError(f"Unknown method: {method}")
        func(remote_cs, **kwargs)
    except Exception as exception:
        logging.exception("Unexpected error: {}".format(exception))