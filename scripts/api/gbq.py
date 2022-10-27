import warnings
from google.cloud import bigquery
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, '../../../bq_view')

class GBQ:

    def __init__(self, secret_path):
        warnings.filterwarnings("ignore")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = secret_path
        self.__client = bigquery.Client()

    def set_project(self, project_id):
        self.project_id = project_id

    def get_datasets(self, project_id):
        list_datasets = []
        datasets = self.__client.list_datasets(project_id)
        if datasets:
            # print("Datasets in project {}".format(project_id))
            for dataset in datasets:
                list_datasets.append(dataset.dataset_id)
        else:
            print("{} project does not contain any datasets.".format(project_id))
        return list_datasets

    def get_views_from_dataset(self, dataset_id):
        list_views = []
        dataset = self.__client.get_dataset(dataset_id)
        full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
        tables = self.__client.list_tables(full_dataset_id)
        if tables:
            # print("Views in dataset {}".format(dataset_id))
            for table in tables:
                if (table.table_type == "VIEW"):
                    list_views.append(table.table_id)
        else:
            print("{} project does not contain any views.".format(full_dataset_id))
        return list_views

    def get_view_query(self, view_id):
        view = self.__client.get_table(view_id)
        return view.view_query