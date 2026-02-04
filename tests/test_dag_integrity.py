"""
Tests for Airflow DAG integrity and validation.
"""
import os
import sys

# Add airflow dags to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestDAGIntegrity:
    """Test DAG structure and configuration."""

    def test_dag_import(self):
        """Test that DAG can be imported without errors."""
        from velib_etl_dag import dag
        assert dag is not None

    def test_dag_id(self):
        """Test that DAG has correct ID."""
        from velib_etl_dag import dag
        assert dag.dag_id == "velib_etl"

    def test_dag_has_tasks(self):
        """Test that DAG has expected tasks."""
        from velib_etl_dag import dag
        task_ids = [task.task_id for task in dag.tasks]
        assert "extract_velib_data" in task_ids
        assert "transform_and_load" in task_ids

    def test_dag_task_count(self):
        """Test that DAG has expected number of tasks."""
        from velib_etl_dag import dag
        assert len(dag.tasks) == 2

    def test_dag_default_args(self):
        """Test that DAG has correct default arguments."""
        from velib_etl_dag import dag
        assert dag.default_args["owner"] == "airflow"
        assert dag.default_args["retries"] == 2

    def test_dag_schedule(self):
        """Test that DAG has correct schedule."""
        from velib_etl_dag import dag
        assert dag.schedule_interval == "*/5 * * * *"

    def test_dag_catchup_disabled(self):
        """Test that DAG catchup is disabled."""
        from velib_etl_dag import dag
        assert dag.catchup is False

    def test_dag_tags(self):
        """Test that DAG has expected tags."""
        from velib_etl_dag import dag
        assert "velib" in dag.tags
        assert "etl" in dag.tags

    def test_task_dependencies(self):
        """Test that tasks have correct dependencies."""
        from velib_etl_dag import dag

        extract_task = dag.get_task("extract_velib_data")
        transform_task = dag.get_task("transform_and_load")

        # transform_and_load should depend on extract_velib_data
        assert extract_task in transform_task.upstream_list or \
               transform_task in extract_task.downstream_list
