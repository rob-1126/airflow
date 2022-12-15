#!/usr/bin/env python
from airflow.dag_processing.processor import DagFileProcessor
from airflow.models.dagbag import DagBag
from airflow.models.dag import DAG
from airflow.serialization.serialized_objects import DagDependency, SerializedDAG
import json
import yaml
import requests

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("filepath")
parser.add_argument("output")
args = parser.parse_args()

filepath = args.filepath

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
      if args.output.endswith(".yaml"):
        encoded_yaml= yaml.dump(serialized_dag, indent=4).encode("utf-8")
        docs.append(encoded_yaml)
      else:
        encoded_json = json.dumps(serialized_dag, indent=4).encode("utf-8")
        docs.append(encoded_json)

with open(args.output, "wb") as f:
  for encoded_document in docs:
      f.write(encoded_document)
