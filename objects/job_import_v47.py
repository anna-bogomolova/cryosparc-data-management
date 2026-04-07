from pydantic import BaseModel
from typing import Optional, Any

from cryosparc.tools import CryoSPARC


class MoviesImportJobs(BaseModel):
    project_uid: str
    workspace_uid: list[str]
    job_uid: str
    session_uid: Optional[str] = ''
    verify: bool = False
    process_dir: Optional[str]
    data_group: Optional[str]
    data_project: Optional[str]
    star_mark: bool = False
    num_movies: int

    @classmethod
    def create_from_jobs(cls, data: dict[str, Any], project: dict[str, Any], star_mark: bool):
        if isinstance(data['params_spec'], list):
            raw_path = [i['blob_paths']['value'] for i in data['params_spec']]
            group = group_from_raw(raw_path[0])
            data_projects = "\n".join(raw_path)
        else:
            # print(data['params_spec'])
            raw_path = data['params_spec'].get('blob_paths', {}).get('value', '')
            group = group_from_raw(raw_path)
            data_projects = raw_path
        res = {
            "project_uid": project["uid"],
            "job_uid": data["uid"],
            "process_dir": project["project_dir"],
            "data_group": group,
            "data_project": data_projects,
            "workspace_uid": data["workspace_uids"],
            "start_mark": star_mark,
            "num_movies": data['output_result_groups'][0]["num_items"]
        }
        return cls(**res)
    
    @classmethod
    def create_from_live_jobs(cls, data: dict[str, Any], project: dict[str, Any], cs: CryoSPARC):
        proj_uid = project["uid"]
        sess_uid = data['session_dir']      
        proj_live = cs.find_project(proj_uid)
        if not proj_live.doc["detached"]:
            live_files = proj_live.list_files(data["session_dir"])
            if f"{sess_uid}/import_movies" in live_files:
                process_dir = f"{project['project_dir']}/{sess_uid}/import_movies"
                verify = False
            else:
                process_dir = f"{project['project_dir']}/{sess_uid}"
                verify = True
        else:
            process_dir = f"{project['project_dir']}/{sess_uid}"
            verify = True
            
        raw_path = data["exposure_groups"][0]["file_engine_watch_path_abs"]
        file_type = data["exposure_groups"][0]['file_engine_filter']
        path = f"{raw_path}/{file_type}"
        group = group_from_raw(path)

        live_res = {
            "project_uid": proj_uid,
            "job_uid": data["live_session_job"],
            "session_uid": data["session_dir"],
            "process_dir": f"{process_dir}/{file_type}",
            "verify": verify,
            "data_group": group,
            "data_project": raw_path,
            "workspace_uid": data["uid"] if isinstance(data["uid"], list) else [data["uid"]],
            "star_mark": len(data.get("starred_by", [])) > 0, 
            "num_movies": data["exposure_groups"][0]["num_exposures_found"]
        }
        return cls(**live_res)


def group_from_raw(path):
    data_dir = '/ddn/gs1/project/cryoemCore/data/'
    if data_dir in path:
        # print(path.partition(data_dir))
        raw_start, _, raw_path = path.partition(data_dir)
        raw_split = raw_path.split('/', maxsplit=2)
        group = raw_split[0] if raw_split[0] != 'projects_niehs' else raw_split[1]
    else:
        group = path
    return group