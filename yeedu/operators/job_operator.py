from typing import Optional, Union, Tuple, List
from airflow.exceptions import AirflowException
from yeedu.hooks.yeedu import YeeduHook
import time
import re


class YeeduJobRunOperator:
    template_fields: Tuple[str] = ("run_id",)

    def __init__(
        self,
        job_id: str,
        base_url: str,
        workspace_id: int,
        tenant_id: str,
        connection_id: str,
        token_variable_name: str,
        restapi_port: int,
        task_cluster_id: int = None,
        arguments: str = None,
        conf: List[str] = None,
        cluster_ids: List[int] = None,
        logger=None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.job_id: str = job_id
        self.tenant_id: str = tenant_id
        self.base_url: str = base_url
        self.workspace_id: int = workspace_id
        self.connection_id = connection_id
        self.token_variable_name = token_variable_name
        self.restapi_port = restapi_port
        self.task_cluster_id = task_cluster_id
        self.arguments = arguments
        self.conf = conf
        self.cluster_ids = cluster_ids or []
        self._BUMP_PATTERN = re.compile(
            r'exit code:?\s*(?:137|143|139|134|52)|'
            r'oomkilled|outofmemoryerror|java heap space|'
            r'gc overhead limit exceeded|killed process|'
            r'sigkill|sigterm|sigsegv|segmentation fault|sigabrt|'
            r'sparkexitcode|exceed_max_executor_failures|'
            r'driver_timeout|container killed by yarn for exceeding memory limits',
            re.IGNORECASE
        )
        self.hook: YeeduHook = YeeduHook(
            conf_id=self.job_id,
            tenant_id=self.tenant_id,
            base_url=self.base_url,
            workspace_id=self.workspace_id,
            connection_id=self.connection_id,
            token_variable_name=self.token_variable_name,
        )
        self.run_id: Optional[Union[int, None]] = None
        self.log = logger

    def _should_bump_cluster_from_logs(self, run_id: int) -> bool:
        """
        Inspect logs and workflow errors to decide if this failure qualifies for cluster bump.

        Args:
            run_id (int): The run ID to check logs for

        Returns:
            bool: True if error patterns indicate cluster bump would help, False otherwise
        """
        try:
            wf_errors = "\n".join(
                self.hook.get_job_workflow_errors(run_id) or [])

            if self._BUMP_PATTERN.search(wf_errors):
                return True

            stderr = self.hook.get_job_logs(
                run_id, "stderr", last_n_lines=1000) or ""

            if self._BUMP_PATTERN.search(stderr):
                return True

            stdout = self.hook.get_job_logs(
                run_id, "stdout", last_n_lines=1000) or ""

            if self._BUMP_PATTERN.search(stdout):
                return True

            return False
        except Exception as e:
            self.log.warning(
                f"Failed log analysis for cluster bump decision: {e}")
            return False

    def run_job(self, cluster_id=None, context=None, use_master_token_flow=False) -> tuple:
        """
        Runs a job on a specified cluster and handles the complete job lifecycle.

        Args:
            cluster_id (int, optional): The cluster ID to run the job on. 
                                       If None, uses the existing cluster.
            context (dict, optional): Airflow context containing task instance info

        Returns:
            tuple: (success, run_id, job_status, exception)
                  - success (bool): True if job completed successfully, False otherwise
                  - run_id (int): The run ID of the submitted job
                  - job_status (str): Final status of the job
                  - exception (Exception): Exception object if job failed, None otherwise
        """
        run_id = None
        job_status = None
        exception = None

        try:
            # For standalone DAGs: persist working cluster to job record for cross-run memory
            # For pipeline DAGs: cluster promotion handles persistence safely
            if cluster_id is not None and not use_master_token_flow:
                self.log.info(
                    f"Persisting cluster {cluster_id} to job {self.job_id} record for future standalone runs")
                self.hook.update_job_cluster(
                    job_id=self.job_id, cluster_id=int(cluster_id))

            # Extract context values
            task_name = None
            dag_id = None
            dag_run_id = None
            task_instance_id = None
            try_number = None
            map_index = -1
            
            if context:
                ti = context.get('ti')
                if ti:
                    task_name = ti.task_id
                    dag_id = ti.dag_id
                    dag_run_id = ti.run_id
                    task_instance_id = str(ti.id) if ti.id else None  # Convert UUID to string
                    try_number = ti.try_number
                    map_index = ti.map_index
                    self.log.info(
                        "Pipeline context for submit_job: task_id=%s dag_id=%s run_id=%s try_number=%s map_index=%s",
                        task_name,
                        dag_id,
                        dag_run_id,
                        try_number,
                        map_index,
                    )

            self.log.info(f"Submitting job {self.job_id}")
            run_id = self.hook.submit_job(
                job_id=self.job_id,
                task_cluster_id=cluster_id if cluster_id is not None else self.task_cluster_id,
                is_background=True,
                arguments=self.arguments,
                conf=self.conf,
                task_name=task_name,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_instance_id=task_instance_id,
                try_number=try_number,
                map_index=map_index
            )

            # Store the run_id for templating and later use
            self.run_id = run_id

            # Generate job URL for monitoring
            job_run_url = f"{self.base_url}tenant/{self.tenant_id}/workspace/{self.workspace_id}/run/{run_id}/run-metrics?type=spark_job".replace(
                f":{self.restapi_port}/api/v1", ":5173"
            )
            self.log.info(
                f"Job submitted with Run ID: {run_id}. Monitor at: {job_run_url}")

            # Wait for job completion
            self.log.info(
                f"Waiting for job {run_id} to complete")
            job_status = self.hook.wait_for_completion(run_id)
            self.log.info(f"Job {run_id} completed with status: {job_status}")

            # Fetch job logs
            self.log.info(
                f"Retrieving logs after 40 seconds sleep for run id: {run_id}")
            time.sleep(40)  # Ensure logs are available
            job_log_stdout = self.hook.get_job_logs(run_id, "stdout") or ""
            job_log_stderr = self.hook.get_job_logs(run_id, "stderr") or ""
            job_log = f" stdout: {job_log_stdout} stderr: {job_log_stderr}"
            self.log.info(
                f"Retrieved logs for run ID: {run_id}, logs: {job_log}")

            # Check if job was successful
            if job_status in ["ERROR", "TERMINATED", "STOPPED"]:
                self.log.error(
                    f"Job {run_id} failed with status: {job_status}")
                exception = AirflowException(
                    f"Job failed with status '{job_status}', logs: {job_log}")
                return False, run_id, job_status, exception

            # Success case
            self.log.info(f"Job {run_id} completed successfully")
            return True, run_id, job_status, None

        except Exception as e:
            self.log.error(f"Error executing job: {str(e)}")
            exception = e
            return False, run_id, job_status, exception

        finally:
            self.log.info("Stopping job in finally")

            # If run_id exists, ensure job is killed
            if run_id is not None:
                try:
                    status_response = self.hook.get_job_status(run_id)
                    status = status_response.json().get("run_status")

                    if status in ["RUNNING", "SUBMITTED"]:
                        self.log.info(
                            f"Stopping run id: {run_id}")
                        self.hook.kill_job(run_id)
                except Exception as stop_error:
                    self.log.warning(
                        f"Error during stopping the job run: {str(stop_error)}")

    def execute(self, context: dict) -> None:
        """
        Execute the Yeedu job with cluster bump logic.

        First executes the job on the current cluster. If the job fails and meets cluster bump criteria,
        it will retry on subsequent clusters in the cluster_ids list.
        """
        clusters = self.cluster_ids[:] if self.cluster_ids else []
        user_token_id = None
        use_master_token_flow = False

        try:
            # Check if connection_id is provided for traditional auth flow
            if self.connection_id:
                # Traditional flow: Use connection credentials to login
                self.log.info("Using connection-based authentication flow")
                self.hook.yeedu_login(context)
            else:
                # Pipeline flow: Use master token to create per-task user token
                self.log.info("Using master token authentication flow for pipeline")
                use_master_token_flow = True

                # 1. Extract username, dag_id from context/DAG params
                username, dag_id = self.hook._get_username_dagid_from_context(context)
                if not username:
                    raise AirflowException("Username not found in DAG context for pipeline authentication")

                # 2. Get tenant_id from workspace (workspace_id is prefix of dag_id)
                workspace_id = dag_id.split("_")[0] if dag_id else None
                if not workspace_id:
                    raise AirflowException("Could not extract workspace_id from dag_id for pipeline authentication")

                workspace_details = self.hook.get_workspace_details(int(workspace_id))
                tenant_id = workspace_details.get('tenant_id')
                if not tenant_id:
                    raise AirflowException("tenant_id not found in workspace details")

                # 3. Create user token using master token
                self.log.info(f"Creating user token for username: {username}, tenant_id: {tenant_id}")
                token_response = self.hook.create_user_token(username, tenant_id)
                user_token_id = token_response.get('token_id')
                user_token = token_response.get('token')

                if not user_token:
                    raise AirflowException("Failed to retrieve user token from response")

                # 4. Update headers with user token
                self.hook.set_headers({'Authorization': f'Bearer {user_token}'})
                self.hook.session.headers.update(self.hook.get_headers())

            if self.cluster_ids:
                self.log.info(
                    f"Cluster bump enabled, planned clusters in order: {self.cluster_ids}"
                )

            # First attempt: run using the job's current cluster binding
            self.log.info(
                f"Running job {self.job_id} on the existing cluster configuration"
            )
            success, run_id, job_status, exception = self.run_job(context=context, use_master_token_flow=use_master_token_flow)

            # If job succeeded, we're done
            if success:
                self.log.info(
                    f"Job {self.job_id} completed successfully without cluster bump"
                )
                return

            # If job failed, check if we should attempt cluster bump
            should_bump = False
            if job_status in ["ERROR", "TERMINATED", "STOPPED"]:
                # Only check for cluster bump if we have a terminal failure status
                should_bump = self._should_bump_cluster_from_logs(run_id)

            if not should_bump:
                self.log.info(
                    "Cluster bump skipped because failure logs do not match bump criteria"
                )
                raise exception or AirflowException(
                    f"Job failed with status: {job_status}")

            # Start cluster bump process using remaining clusters, if any
            available_clusters = clusters

            if not available_clusters:
                self.log.error(
                    "Cluster bump requested but no additional clusters are configured"
                )
                raise exception or AirflowException(
                    f"Job failed with status: {job_status}"
                )

            # Try on subsequent clusters
            total_attempts = len(available_clusters)
            for attempt, cluster_id in enumerate(available_clusters, start=1):
                self.log.info(
                    f"Cluster bump attempt {attempt}/{total_attempts}: switching to cluster {cluster_id}"
                )

                # Run job on this cluster
                success, run_id, job_status, exception = self.run_job(
                    cluster_id, context=context, use_master_token_flow=use_master_token_flow)

                # If job succeeded, we're done
                if success:
                    self.log.info(
                        f"Job {self.job_id} completed successfully after cluster bump to cluster {cluster_id}"
                    )
                    if use_master_token_flow and context:
                        dag_id_str = context.get('ti') and context['ti'].dag_id
                        parts = dag_id_str.split("_") if dag_id_str else []
                        pipeline_id = int(parts[1]) if len(parts) == 2 else None
                        task_key = context['ti'].task_id if context.get('ti') else None
                        if pipeline_id and task_key:
                            clusters_tried = available_clusters[:attempt]
                            try:
                                self.hook.promote_pipeline_task_cluster(
                                    self.workspace_id, pipeline_id, task_key,
                                    cluster_id, clusters_tried)
                            except Exception as promote_err:
                                self.log.warning(
                                    f"Cluster promotion failed (non-fatal, job succeeded): {promote_err}")
                    return

                # Check if we should continue bumping
                should_continue_bump = False
                if job_status in ["ERROR", "TERMINATED", "STOPPED"]:
                    # Only check for cluster bump if we have a terminal failure status
                    should_continue_bump = self._should_bump_cluster_from_logs(
                        run_id
                    )

                # If this is the last cluster or we shouldn't bump anymore, raise the exception
                if attempt == total_attempts or not should_continue_bump:
                    if attempt == total_attempts:
                        self.log.error(
                            "Cluster bump exhausted all configured clusters without success"
                        )
                    if not should_continue_bump:
                        self.log.error(
                            "Cluster bump stopped because the latest failure is not eligible for another bump"
                        )
                    raise exception or AirflowException(
                        f"Job failed with status: {job_status}"
                    )

                self.log.info(
                    f"Cluster bump will continue, preparing next cluster after failure on cluster {cluster_id}"
                )

        except Exception as e:
            self.log.error(f"Job execution failed: {str(e)}")
            raise

        finally:
            # Cleanup: handle auth cleanup based on flow type
            try:
                if use_master_token_flow and user_token_id:
                    # Pipeline flow: Delete the user token created for this task
                    self.log.info(f"Deleting user token with id: {user_token_id}")
                    self.hook.delete_user_token(user_token_id)
                else:
                    # Traditional flow: Logout if using LDAP or AAD
                    auth_type = self.hook.yeedu_auth_type
                    if auth_type in ["LDAP", "AAD"]:
                        self.hook.yeedu_logout()
            except Exception as e:
                self.log.warning(f"Auth cleanup skipped or failed: {e}")

            # Close HTTP session if it exists
            if hasattr(self, 'hook') and hasattr(self.hook, 'session'):
                try:
                    self.hook.session.close()
                    self.log.info("HTTP session closed in finally block.")
                except Exception as session_close_error:
                    self.log.warning(
                        f"Failed to close HTTP session: {session_close_error}")
