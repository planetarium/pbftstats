import os
from subprocess import Popen

collect_path = os.getenv("COLLECT_PATH", "/app/stat_logs")
report_path = os.getenv("REPORT_PATH", "/app/reports")
host_url = os.getenv("HOST_URL", "http://a9261bb03cf0a4b8e910c423c2296adf-113367791.us-east-2.elb.amazonaws.com")
validator_map_url = os.getenv("VALIDATOR_MAP_URL", "https://9c-dev-cluster-configs.s3.ap-northeast-2.amazonaws.com/pbft-validators.json")
collect_start_block_index = int(os.getenv("COLLECT_START_BLOCK_INDEX", "5963940"))
collect_chunk_size = int(os.getenv("COLLECT_CHUNK_SIZE", "1024"))
report_interval = int(os.getenv("REPORT_INTERVAL", "60"))

if __name__ == '__main__':
    Popen([
        "python3.8", 
        "collect.py", 
        f"--out_path={collect_path}", 
        f"--host_url={host_url}", 
        f"--start_block_index={collect_start_block_index}",
        f"--chunk_size={collect_chunk_size}"])
    Popen([
        "python3.8", 
        "report.py"
        f"--log_path={collect_path}",
        f"--report_path={report_path}",
        f"--report_interval={report_interval}"])
