import os
from subprocess import Popen

collect_path = os.getenv("COLLECT_PATH", "/app/stat_logs")
report_path = os.getenv("REPORT_PATH", "/app/reports")
host_url = os.getenv("HOST_URL", "http://a9261bb03cf0a4b8e910c423c2296adf-113367791.us-east-2.elb.amazonaws.com")
validator_map_url = os.getenv("VALIDATOR_MAP_URL", "https://9c-dev-cluster-configs.s3.ap-northeast-2.amazonaws.com/pbft-validators.json")
query_validator_key = os.getenv("QUERY_VALIDATOR_KEY", "03461fb858bd9852ab699477a7ae7b11aefff0797ab5b0342d57b1d38f3560a23e")

collect_start_block_index = int(os.getenv("COLLECT_START_BLOCK_INDEX", "5963940"))
chunk_size_collect = int(os.getenv("CHUNK_SIZE_COLLECT", "512"))
chunk_size_recent = int(os.getenv("CHUNK_SIZE_RECENT", "30"))
interval_collect = int(os.getenv("INTERVAL_COLLECT", "1"))
interval_recent = int(os.getenv("INTERVAL_RECENT", "5"))
interval_retry = int(os.getenv("INTERVAL_RETRY", "60"))
interval_report = int(os.getenv("INTERVAL_REPORT", "60"))

if __name__ == '__main__':
    Popen([
        "python3.8", 
        "collect.py", 
        f"--out_path={collect_path}", 
        f"--host_url={host_url}", 
        f"--query_validator_key={query_validator_key}", 
        f"--start_block_index={collect_start_block_index}",
        f"--chunk_size_collect={chunk_size_collect}"
        f"--chunk_size_recent={chunk_size_recent}"
        f"--interval_collect={interval_collect}"
        f"--interval_retry={interval_retry}"
        f"--interval_recent={interval_recent}"])
    Popen([
        "python3.8", 
        "report.py"
        f"--log_path={collect_path}",
        f"--report_path={report_path}",
        f"--interval={interval_report}"])
