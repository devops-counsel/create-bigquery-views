#!/usr/bin/python
import datetime
import sys
import os
from google.cloud import bigquery


def remove_and_create_view_datasets(bq_client, project, dataset):
        """Deletes and re-creates view dataset in destination project"""
        dataset_ref = bq_client.dataset(dataset)
        print('Deleting Dataset {} in Project {}.'.format(dataset_ref.dataset_id, project) + "\n")
        try:
                bq_client.delete_dataset(dataset_ref, delete_contents='true')
                print('Dataset {} deleted in Project {}.'.format(dataset_ref.dataset_id, project) + "\n")
        except Exception as e:
                print(e)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)
        print('Dataset {} created in Project {}.'.format(dataset.dataset_id, project) + "\n")


def list_tables_in_dataset(bq_client, dataset):
        """Lists Tables in a Dataset"""
        table_list = list(bq_client.list_tables(dataset))
        tables = []
        for table in table_list:
                tables.append(table.table_id)
        return(tables)

def create_view(bq_client, sproject, vproject, dataset, table):
        """Creates a View"""
        print("Creating a view in " + vproject + " for " + sproject + dataset + table + "\n" )
        query = "select * from `" + sproject + "." + dataset + "." + table + "`"
        dataset_ref = bq_client.dataset(dataset)
        table_ref = dataset_ref.table(table)
        table = bigquery.Table(table_ref)
        table.view_query = query
        table.view_query_legacy_sql = False
        table = bq_client.create_table(table)
        print('{} view created in Dataset {}'.format(table.table_id, dataset) + " in " + vproject + "\n")


def add_view_permissions(sbq_client, vbq_client, sproject, vproject, dataset):
        """Adds Auth View Permissions"""
        dataset_ref = sbq_client.get_dataset(sproject + "." + dataset)
        entries = list(dataset_ref.access_entries)
        tables = list_tables_in_dataset(vbq_client, dataset)
        for table in tables:
            entry = bigquery.AccessEntry(
                    None,
                    entity_type = 'view',
                    entity_id = { 'projectId': vproject,
                                  'datasetId': dataset,
                                  'tableId': table 
                                }
            )
            if entry not in dataset_ref.access_entries:
                entries.append(entry)
            else:
                print(entry)
                print('Permission already there for View {}.'.format(dataset_ref.dataset_id) + "\n")
        dataset_ref.access_entries = entries
        dataset = sbq_client.update_dataset(dataset_ref, ['access_entries'])
        print('Auth View Permission added for {}'.format(dataset.dataset_id) + " in " + sproject+ "\n")


def remove_view_permissions(bq_client, sproject, vproject, dataset):
        """Remove Stale View Permissions"""
        dataset_ref = bq_client.get_dataset(bq_client.dataset(dataset))
        newentries = []
        for entry in dataset_ref.access_entries:
                if "'projectId': '" + vproject + "', 'datasetId': '" + dataset + "'," not in str(entry):
                        newentries.append(entry)
        dataset_ref.access_entries = newentries
        dataset = bq_client.update_dataset(dataset_ref, ['access_entries'])
        print('Auth View Permissions Cleared for {}'.format(dataset.dataset_id) + " in " + sproject + "\n")


def dataset_views(sproject, vproject, dataset):
        """Creates views for all tables in a dataset"""
        source_bq_client = bigquery.Client(sproject)
        view_bq_client = bigquery.Client(vproject)
        remove_and_create_view_datasets(view_bq_client, vproject, dataset)
        remove_view_permissions(source_bq_client, sproject, vproject, dataset)
        tables = list_tables_in_dataset(source_bq_client, dataset)
        for table in tables:
                try:
                        create_view(view_bq_client, sproject, vproject, dataset, table)
                except Exception as e:
                        print(e)
        add_view_permissions(source_bq_client, view_bq_client, sproject, vproject, dataset)


def scripthelp():
        """Prints Help Info"""
        filename = os.path.basename(__file__)
        print("\nScript for creating BigQuery views\n")
        print("\nFor creating views for a dataset\n")
        print("\nUsage: " + filename + " <source_project_id> <view_project_id> <dataset>\n")

def main():
        if len(sys.argv) == 4:
                dataset_views(sys.argv[1], sys.argv[2], sys.argv[3])
        elif len(sys.argv) == 1 or sys.argv[1] == "-help" or sys.argv[1] == "help" or sys.argv[1] == "--help" or sys.argv[1] == "-h":
                scripthelp()
        else:
                scripthelp()

if __name__ == "__main__":
    main()
