import boto3
import time
import pandas as pd
import random
from string import ascii_lowercase


class Query:
    def __init__(self, client, query, catalog, db):
        self.query = query
        self.catalog = catalog
        self.db = db
        self.client = client
        self.result_df = None

    def _submit_query(self):
        # client token is used to make sure that query is executed only once. 32 =< len =< 128
        client_token = ''.join(random.choice(ascii_lowercase) for i in range(40))
        queryID = self.client.start_query_execution(
            QueryString=self.query,
            ClientRequestToken=client_token,
            WorkGroup='AthenaAdmin',
            QueryExecutionContext={
                'Database': self.db,
                'Catalog': self.catalog
            }
        )['QueryExecutionId']
        return queryID

    def _wait_for_query(self, queryID):
        status = self.client.get_query_execution(QueryExecutionId=queryID).get("QueryExecution").get("Status").get(
            "State")
        # iterator to end while if it will take more than 10 minutes
        max_iter = 600
        i = 0
        while (status == "QUEUED" or status == "RUNNING") and i < max_iter:
            time.sleep(1)
            status = self.client.get_query_execution(QueryExecutionId=queryID).get("QueryExecution").get("Status").get(
                "State")
            i += 1
        return True if status == "SUCCEEDED" else False

    def _process_query_result(self, queryID):
        result = self.client.get_query_results(QueryExecutionId=queryID)["ResultSet"]["Rows"]
        rows = []
        for data in result:
            for columns in data.values():
                row = []
                for val in columns:
                    row.append(val.get('VarCharValue', ' '))
                rows.append(row)
        df = pd.DataFrame(data=rows[1:], columns=rows[0])
        self.result_df = df
        return df

    def get_query_results(self):
        if self.result_df is not None:
            return self.result_df
        id = self._submit_query()
        self._wait_for_query(id)
        return self._process_query_result(id)


class ViewAPIMetadata:
    def __init__(self, client, catalog, db, view_name):
        self.client = client
        self.catalog = catalog
        self.db = db
        self.view_name = view_name

    def get_metadata(self):
        json_metadata = client.get_table_metadata(
            CatalogName=self.catalog,
            DatabaseName=self.db,
            TableName=self.view_name
        )
        return json_metadata

    @staticmethod
    def process_table_metadata(metadata_json):
        data = {}
        metadata_json = metadata_json.get("TableMetadata")
        data["CreateTime"] = metadata_json.get("CreateTime")
        data["LastAccessTime"] = metadata_json.get("LastAccessTime")
        data["ViewType"] = metadata_json.get("TableType")
        return data


class ViewsDataFrame:
    def __init__(self, client, catalog, db):
        file = open("../sql/views_query.sql")
        query = file.read()
        file.close()
        # query will be executed in execution_context, so will return views only for specified catalog and database
        df = Query(client, query, catalog, db).get_query_results()
        create_time = {}
        last_access = {}
        view_type = {}
        for name in df["view_name"]:
            vam = ViewAPIMetadata(client, catalog, db, name)
            metadata = vam.process_table_metadata(vam.get_metadata())
            create_time[name] = metadata["CreateTime"]
            last_access[name] = metadata["LastAccessTime"]
            view_type[name] = metadata["ViewType"]

        df["create_time"] = df["view_name"].map(create_time)
        df["last_access_time"] = df["view_name"].map(last_access)
        df["view_type"] = df["view_name"].map(view_type)

        self.full_metadata = df


if __name__ == "__main__":
    client = boto3.client('athena')
    catalog = 'AWSDataCatalog'
    db = 'mydatabase'

    vdf = ViewsDataFrame(client, catalog, db)
    vdf.full_metadata.to_csv("../output/views.csv", index=False)

    file = open("../sql/columns_query.sql")
    cquery = file.read()
    file.close()
    cols_query = Query(client,
                       query=cquery,
                       catalog=catalog,
                       db=db
                       )
    cdf = cols_query.get_query_results()
    cdf.to_csv("../output/columns.csv", index=False)

    file = open("../sql/json_query.sql")
    jquery = file.read()
    file.close()
    json_query = Query(client,
                       query=jquery,
                       catalog=catalog,
                       db=db)
    jdf = json_query.get_query_results().to_csv("../output/json.csv", index=False)
