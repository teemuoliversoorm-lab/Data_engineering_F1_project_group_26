"""Run inside the Airflow image to check real DAG parsing and parameter schemas."""

import importlib.util
import unittest


@unittest.skipUnless(importlib.util.find_spec("airflow"), "Requires the Airflow image")
class DagTests(unittest.TestCase):
    def test_dag_imports_and_dependencies(self):
        from airflow.models import DagBag
        from airflow.utils.task_group import MappedTaskGroup
        bag = DagBag(dag_folder="dags", include_examples=False)
        self.assertEqual(bag.import_errors, {})
        dag = bag.get_dag("f1_ingestion")
        self.assertEqual(set(dag.task_ids), {
            "plan_sessions", "ingest_session.extract", "ingest_session.check", "ingest_session.load",
        })
        self.assertEqual(dag.get_task("ingest_session.load").upstream_task_ids, {"ingest_session.check"})
        self.assertEqual(dag.get_task("ingest_session.check").upstream_task_ids, {"ingest_session.extract"})
        self.assertIsInstance(dag.task_group.children["ingest_session"], MappedTaskGroup)
        self.assertEqual(dag.params["sessions"], ["Q", "R"])
        self.assertFalse(dag.params["reuse_landed"])
        self.assertGreater(dag.default_args["retries"], 0)
