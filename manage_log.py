import os
import traceback
from datetime import date, datetime, timedelta
from src.conf_manage.config_loader import ConfigLoader
from src.audit.pyspark_logger import PysparkLogger
from src.utils.spark_session import PySparkSession
from src.audit.etl_control import ETLControl, PipilenRunLog, PipelineRunDecision
from src.utils.utility import parse_args, write_op_json_in_hdfs

# set module constant 
# ------------------------------
MARK_START = "start"
MARK_COMPLETE = "complete"
_VALID_MARK = (MARK_START, MARK_COMPLETE)


def write_to_hdfs(
    control: ETLControl, 
    pipeline_id :int, 
    pipeline_run_id: int, 
    status: str, 
    luigi_op_path: str, 
    marker_name: str
):
    """Funtion to write op file to hdfs for luigi to mark completion of the task"""

    pipeline_info = control.get_pipeline_master(pipeline_id)
    op_file_data = {
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline_info["pipeline_name"],
        "pipeline_run_id": pipeline_run_id,
        "status": status
    }
    # folder_name = f"{pipeline_info['pipeline_name']}_{pipeline_id}"
    hdfs_loc = (
        f"{cfg.get('luigi_op_file')}/{cfg.get('pipeline_name')}/{today_dt.strftime('%Y%m%d')}"
        if not luigi_op_path else luigi_op_path    
    )
    write_op_json_in_hdfs(marker_name, hdfs_loc, op_file_data)
    logger.info("--------------> Output JSON file written in HDFS location <--------------")

def start_pipeline_log(
    controller: ETLControl, 
    pipeline_id: int,
    pipeline_run_id: int,
    luigi_op_path: str, 
    marker_name: str,
    spark_user
):

    pipeline_log_entry = None
    pipeline_run = None

    # check pipeline run status
    pipeline_run = controller.check_pipeline_run(pipeline_id, business_date)
    if not pipeline_run.should_run:
        logger.info(f"Skipping [pipeline_id] ==> {pipeline_id} --> {pipeline_run.reason}")
        return
    
    # create log entry in pipeline log table for this run
    # ---------------------------------------------------
    pipeline_log_entry = PipilenRunLog(
        pipeline_run_id=pipeline_run_id,
        pipeline_id=pipeline_id,
        business_dt=business_date,
        run_dt=today_dt,
        triggered_by=spark_user,
    )

    pipeline_log_entry = controller.start_pipeline_run(pipeline_log_entry)

    # write file in hdfs with pipeline_run_id
    # ----------------------------------------
    write_to_hdfs(
        control=controller, 
        pipeline_id=pipeline_id, 
        pipeline_run_id=pipeline_log_entry.pipeline_run_id,
        status="running",
        luigi_op_path=luigi_op_path,
        marker_name=marker_name
    )

def complete_pipeline_log(
    controller: ETLControl, 
    pipeline_id: int, 
    pipeline_run_id: int,
    luigi_op_path: str, 
    marker_name: str,
):
    # check if any task has failed 
    # get all tasks run stat of this pipeline
    task_run_stat_dict = controller.get_task_run_stat(pipeline_id, business_date)
    tasks_statuses = [task["status"] for task in task_run_stat_dict]
    
    # call derive pipeline status
    pipeline_status = controller.derive_pipeline_status(tasks_statuses)
    controller.complete_pipeline_run(pipeline_id, pipeline_run_id, pipeline_status)

    # write file in hdfs with pipeline_run_id
    # ----------------------------------------
    write_to_hdfs(
        control=controller, 
        pipeline_id=pipeline_id, 
        pipeline_run_id=pipeline_run_id,
        status=pipeline_status,
        luigi_op_path=luigi_op_path,
        marker_name=marker_name
    )

# ============================================================
# MAIN FUNCTION
# ============================================================
def main():

    # step 1: parse arguments
    # =====================================================
    args = parse_args()
    pipeline_id = args.pipeline_id
    pipeline_run_id = args.pipeline_run_id 
    action = args.action
    luigi_op_path = args.output_path
    marker_name = args.marker_name
    pipeline_name = args.pipeline_name

    if action is None or action.lower() not in _VALID_MARK:
        raise ValueError(f"Invalid action !!! Please provide one of the action from ({_VALID_MARK})")
        # perform negative test

    print("="*200)
    print(f"Starting {pipeline_name} run | Pipeline Id = {pipeline_id} | Start time = {current_ts_str}")
    print(f"{'_'*100}")
    
    # step 2: build spark session
    # =====================================================
    spark_app_name = pipeline_name if pipeline_name else cfg.get("spark.app_name")
    spark_all_config = cfg.get("spark")
    try:
        sprk_obj = PySparkSession()
        spark, sc = sprk_obj.get_spark_session(spark_app_name, spark_all_config)
        logger.info("Spark Session Initialized ...")
        spark_user = spark.sparkContext.sparkUser()
    except Exception as e:
        err_msg = str(e)
        logger.error(f"Failed to create Spark Session !!! Error: {err_msg} ")
        return

    # get etl controller
    # ------------------------
    controller = ETLControl(spark)

    try:
        if action.lower() == MARK_START:
            start_pipeline_log(controller, pipeline_id, pipeline_run_id, luigi_op_path, marker_name, spark_user)

        if action.lower() == MARK_COMPLETE:
            complete_pipeline_log(controller, pipeline_id, pipeline_run_id, luigi_op_path, marker_name)
            
    except Exception as e:
        err_msg = traceback.format_exc()
        logger.error(f"Failed to log pipeline: ")
        print(err_msg)
        # update pipeline run audit table
        controller.complete_pipeline_run(pipeline_id, pipeline_run_id, "failed")

    finally:
        sprk_obj.stop_spark_session(spark)
        logger.info("SparkSession is stopped !!!")
        print("="*200)

        # upload log file to hdfs

if __name__ == "__main__":

    # resolve date-timestamp
    # =====================================================
    current_ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_dt = date.today()
    business_date = today_dt - timedelta(days=1)
    
    # 1 : Load the pipeline config file 
    # =====================================================
    cfg = ConfigLoader()
    cfg.initlz(os.path.basename("pipeline_config.yml"))  # change: put config file name here

    # 2: get logger
    # =====================================================
    logger = PysparkLogger(cfg.get("pipeline_name"), __name__)

    # call main function
    # --------------------------
    main()
