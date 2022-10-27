import os
import sys

abspath = os.path.abspath(__file__)
os.chdir(os.path.dirname(abspath))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from api.gbq import GBQ
from config import gbq_projects, views_repo_dir

def save_views():
    for project_id in gbq_projects:
        print("Getting project: {}".format(project_id))
        project_views = get_project_views(project_id)
        print("Saving project: {}".format(project_id))
        save_project_views(project_id, project_views)


def get_project_views(project_id):
    result = {}
    my_gbq = GBQ(gbq_projects[project_id])
    datasets = my_gbq.get_datasets(project_id)
    for dataset_id in datasets:
        views = my_gbq.get_views_from_dataset(dataset_id)
        views_query = {}
        for view_id in views:
            view_query = my_gbq.get_view_query("{}.{}.{}".format(project_id, dataset_id, view_id))
            views_query.setdefault(view_id, view_query)
        result.setdefault(dataset_id, views_query)
    return result


def save_project_views(project_id, project_views):
    if (not os.path.exists(views_repo_dir)):
        print("{} not exists".format(views_repo_dir))
        return

    project_path = views_repo_dir + "/" + project_id
    if (not os.path.exists(project_path)):
        os.mkdir(project_path)

    for dataset_id in project_views:
        print("Saving project: {} dataset: {}".format(project_id, dataset_id))
        dataset_path = project_path + "/" + dataset_id
        if (not os.path.exists(dataset_path)):
            os.mkdir(dataset_path)
        views = project_views.get(dataset_id)
        for view_id in views:
            view_path = dataset_path + "/" + view_id + ".sql"
            view_file = open(view_path, "w+", encoding="utf-8")
            view_file.write(views.get(view_id))


if __name__ == '__main__':
    save_views()

