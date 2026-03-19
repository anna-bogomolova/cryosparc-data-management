import logging
import argparse
import pandas as pd
import json

from cryosparc.tools import CryoSPARC

from objects.job_import_v47 import MoviesImportJobs



def main(arg):
    cs = CryoSPARC(
        license=arg.licence,
        host=arg.host,
        email=arg.email,
        password=arg.password
    )
    print(f"Connection was established: {cs.test_connection()}")
    
    jobs = []
    live_jobs = []
    proj_list = cs.cli.list_projects()
    print(f"Number of projects in total - {len(proj_list)}")
    for i, p in enumerate(proj_list):
        temp_jobs = cs.cli.get_jobs_by_type(project_uid=p["uid"], types=['import_movies'])
        for j in temp_jobs:
            job = cs.cli.get_job(p["uid"], j["uid"])
            if len(j.get("errors_run", [])) == 0 and job["params_spec"]:
                jobs.append(MoviesImportJobs.create_from_jobs(job, p))
        
        live_temp_jobs = cs.rtp.get_all_sessions_in_project(p["uid"])
        live_jobs += [MoviesImportJobs.create_from_live_jobs(j, p, cs) for j in live_temp_jobs if len(j.get("errors", [])) == 0]
        if i % 20 == 0:
            print(f"Number of projects scanned - {i+1}")
    
    df_jobs = pd.DataFrame([i.model_dump() for i in jobs+live_jobs])

    with pd.ExcelWriter('cryosparc_data_management.xlsx', engine='openpyxl') as writer:
        df_jobs.to_excel(writer, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Logs parser')
    parser.add_argument(
        "--licence",
        type=str,
        required=True,
        help="CryoSPARC licence",
    )
    parser.add_argument(
        "--host",
        type=str,
        required=False,
        help="CryoSPARC web URL, e.g., http://localhost:39000",
    )
    parser.add_argument("--email", type=str, required=True, help="login email")
    parser.add_argument("--password", type=str, required=True, help="login password")
    arg = parser.parse_args()
    try:
        main(arg)
    except Exception as exception:
        logging.exception("Unexpected error: {}".format(exception))