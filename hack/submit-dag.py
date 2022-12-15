#!/usr/bin/env python
from airflow.dag_processing.processor import DagFileProcessor
from airflow.models.dagbag import DagBag
from airflow.models.dag import DAG
from airflow.serialization.serialized_objects import DagDependency, SerializedDAG
import json
import requests

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filepath")
parser.add_argument("--endpoint", default="http://localhost:8080")
parser.add_argument("--username", default="admin")
parser.add_argument("--password", default="admin")
parser.add_argument("--verbose", default=False)
parser.add_argument("--trigger", default=False)
args = parser.parse_args()

username = args.username
password = args.password
verbose = args.verbose
filepath = args.filepath
endpoint = args.endpoint
trigger = args.trigger

url = f"{endpoint}/api/v1/dags/"


class NoncollectingDagbag(DagBag):
    def collect_dags(
        self,
        dag_folder=None,
        only_if_updated: bool = False,
        include_examples=False,
        safe_mode=True,
    ):
        pass


docs = []
# if this is a python file parse it as a dag
if filepath.endswith(".py"):
  dagbag = NoncollectingDagbag()
  file_last_changed_on_disk = None
  mods = dagbag._load_modules_from_file(filepath, safe_mode=True)
  found_dags = dagbag._process_modules(filepath, mods, file_last_changed_on_disk)
  for dag in found_dags:
      serialized_dag = SerializedDAG.serialize_dag(dag)
      enveloped_dag = {"__version": 1, "dag": serialized_dag}
      encoded_json = json.dumps(enveloped_dag).encode("utf-8")
      docs.append(encoded_json)
else:
  with open(filepath) as f:
    file_contents = f.read()
  serialized_dag = json.loads(file_contents)
  enveloped_dag = {"__version": 1, "dag": serialized_dag}
  encoded_json = json.dumps(enveloped_dag).encode("utf-8")
  docs.append(encoded_json)

for encoded_json in docs:
    data = json.loads(encoded_json)
    if verbose:
      print("Submitting ", data)
    x = requests.post(url, json=data, auth=(username, password))
    dag_id = serialized_dag["_dag_id"]
    if trigger:
      if verbose:
        print("Triggering dag")
      trigger_url = f"{endpoint}/api/v1/dags/{dag_id}/dagRuns"
      trigger_data = {}
      r = requests.post(trigger_url, json=trigger_data, auth=(username, password))
