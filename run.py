import logging
import argparse
import subprocess
import pandas as pd
import os

from cryosparc.tools import CryoSPARC

from objects.job_import import MoviesImportJobs



def main(arg):
    remote_cs = CryoSPARC(arg.url, email=arg.email)
    jobs = remote_cs.api.jobs.find(type=['import_movies', 'import_micrographs'], limit=500)
    jobs_live = remote_cs.api.jobs.find(type=['live_session'], limit=500)
    jobs_list = [MoviesImportJobs.create_from_jobs(j, remote_cs) for j in jobs if len(j.build_errors) == 0]
    jobs_live_list = [MoviesImportJobs.create_from_live_jobs(j, remote_cs) for j in jobs_live if j.spec.params.session_uid != j.workspace_uids[0]]
    df_jobs = pd.DataFrame([i.model_dump() for i in jobs_list])
    df_jobs_live = pd.DataFrame([i.model_dump() for i in jobs_live_list])
    df_all_jobs = pd.concat([df_jobs, df_jobs_live], axis=0)

    with pd.ExcelWriter(f'cryosparc_{arg.file_name}.xlsx', engine='openpyxl') as writer:
        df_all_jobs.to_excel(writer, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser('Logs parser')
    parser.add_argument(
        "--url",
        type=str,
        required=False,
        help="CryoSPARC web URL, e.g., http://localhost:39000",
    )
    parser.add_argument("--email", type=str, required=False, help="login email, prompts when unspecified")
    parser.add_argument("--ssl_cert", type=str, required=False, help="where to locate the ssl certificate file")
    parser.add_argument("--file_name", type=str, required=True, help="name of the file to save")
    arg = parser.parse_args()
    try:
        os.environ["SSL_CERT_FILE"] = arg.ssl_cert
        result = subprocess.run(["python3", "-m", "cryosparc.tools", "login", "--url", arg.url, "--email", arg.email])
        print(result.stdout)
        main(arg)
    except Exception as exception:
        logging.exception("Unexpected error: {}".format(exception))