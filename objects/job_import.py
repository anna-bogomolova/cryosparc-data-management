from pydantic import BaseModel
from typing import Optional

from cryosparc.tools import CryoSPARC
from cryosparc.models.job import Job


class MoviesImportJobs(BaseModel):
    project_uid: str
    job_uid: str
    session_uid: Optional[str] = ''
    workspace_uid: list[str]
    verify: bool = False
    process_dir: Optional[str]
    data_group: Optional[str]
    data_project: Optional[str]
    star_mark: bool = False
    job_type: str
    num_movies: int

    @classmethod
    def create_from_jobs(cls, data: Job, cryo_agent: CryoSPARC):
        raw_path = data.spec.params.blob_paths
        group, raw_path = group_from_raw(raw_path)
        if data.spec.type == 'import_micrographs':
            type_outputs = 'imported_micrographs'
        else:
            type_outputs = 'imported_movies'

        star_mark = False
        for ws in data.workspace_uids:
            try:
                wspace = cryo_agent.api.workspaces.find_one(data.project_uid, ws)
                starred = len(wspace.starred_by) > 0
                star_mark = star_mark | starred
            except Exception as e: 
                print(f"Skipping workspace {ws}: {e}")
                continue

            starred = len(cryo_agent.api.workspaces.find_one(data.project_uid, ws).starred_by) > 0
            star_mark = star_mark | starred

        new_instance = {
            "project_uid": data.project_uid,
            "workspace_uid": data.workspace_uids,
            "job_uid": data.uid,
            "process_dir": cryo_agent.api.jobs.get_directory(data.project_uid, data.uid),
            "data_group": group,
            "data_project": raw_path,
            "star_mark": star_mark,
            "job_type": type_outputs,
            "num_movies": data.spec.outputs.root[type_outputs].num_items
        }
        return cls(**new_instance)
    
    @classmethod
    def create_from_live_jobs(cls, data: Job, cryo_agent: CryoSPARC):
        proj_uid = data.project_uid
        sess_uid = data.spec.params.session_uid
        proj_live = cryo_agent.find_project(proj_uid)
        if not proj_live.model.detached:
            live_files = proj_live.list_files(sess_uid)
            if f"{sess_uid}/import_movies" in live_files:
                process_dir = f"{proj_live.dir}/{sess_uid}/import_movies"
                verify = False
            else:
                process_dir = f"{proj_live.dir}/{sess_uid}"
                verify = True
        else:
            process_dir = f"{proj_live.dir}/{sess_uid}"
            verify = True

        sess_files = cryo_agent.api.sessions.find_exposure_groups(proj_uid, sess_uid)
        path = f"{sess_files[0].file_engine_watch_path_abs}/{sess_files[0].file_engine_filter}"
        group, raw_path = group_from_raw(path)

        new_instance = {
            "project_uid": proj_uid,
            "workspace_uid": data.workspace_uids,
            "job_uid": data.uid,
            "session_uid": sess_uid,
            "process_dir": f"{process_dir}/{sess_files[0].file_engine_filter}",
            "verify": verify,
            "data_group": group,
            "data_project": raw_path,
            "star_mark": len(data.starred_by) > 0, 
            "job_type": "live_session",
            "num_movies": sess_files[0].num_exposures_found
        }
        return cls(**new_instance)


def group_from_raw(path):
    data_dir = '/ddn/gs1/project/cryoemCore/data/'
    if data_dir in path:
        raw_start, _, raw_path = path.partition(data_dir)
        raw_split = raw_path.split('/', maxsplit=2)
        group = raw_split[0] if raw_split[0] != 'projects_niehs' else raw_split[1]
    else:
        group = path
    return group, path
