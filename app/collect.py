import argparse
import time
from multiprocessing import Process
from libs.collector import Collector

def collect(out_path, host_url, start_block_index, chunk_size_collect, chunk_size_recent, interval_collect, interval_recent, interval_retry, query_validator_key):
    collector = Collector(out_path, host_url, start_block_index)
    try:
        Process(target=collector.run_tx_signer, args=(chunk_size_collect, interval_collect)).start()
    except (ConnectionError, IndexError) as error:
        print(error)
        time.sleep(interval_retry)
        Process(target=collector.run_tx_signer, args=(chunk_size_collect, interval_collect)).start()

    try:
        Process(target=collector.run_lastcommit_vote, args=(chunk_size_collect, interval_collect)).start()
    except (ConnectionError, IndexError) as error:
        print(error)
        time.sleep(interval_retry)
        Process(target=collector.run_lastcommit_vote, args=(chunk_size_collect, interval_collect)).start()
    try:
        Process(target=collector.run_recent_status, args=(query_validator_key, chunk_size_recent, interval_recent)).start()
    except Exception as e:
        print(e)
        Process(target=collector.run_recent_status, args=(query_validator_key, chunk_size_recent, interval_recent)).start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_path", type=str, default="./stat_logs")
    parser.add_argument("--host_url", type=str, default="http://a2a14a139dbc243ba9182849af6e2c77-231856034.us-east-2.elb.amazonaws.com")
    parser.add_argument("--query_validator_key", type=str, default="03461fb858bd9852ab699477a7ae7b11aefff0797ab5b0342d57b1d38f3560a23e")
    parser.add_argument("--start_block_index", type=int, default=5963940)
    parser.add_argument("--chunk_size_collect", type=int, default=1024)
    parser.add_argument("--chunk_size_recent", type=int, default=30)
    parser.add_argument("--interval_collect", type=int, default=1)
    parser.add_argument("--interval_retry", type=int, default=60)
    parser.add_argument("--interval_recent", type=int, default=1)
    args = parser.parse_args()
    collect(args.out_path, args.host_url, args.start_block_index, args.chunk_size_collect, args.chunk_size_recent, args.interval_collect, args.interval_recent, args.interval_retry, args.query_validator_key)
