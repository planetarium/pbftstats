import argparse
from multiprocessing import Process
from libs.collector import Collector

def collect(out_path, host_url, start_block_index, chunk_size):
    collector = Collector(out_path, host_url, start_block_index)
    try:
        Process(target=collector.run_tx_signer, args=(chunk_size, )).start()
    except (ConnectionError, IndexError) as error:
        print(error)
        Process(target=collector.run_tx_signer, args=(chunk_size, )).start()

    try:
        Process(target=collector.run_lastcommit_vote, args=(chunk_size, )).start()
    except (ConnectionError, IndexError) as error:
        print(error)
        Process(target=collector.run_lastcommit_vote, args=(chunk_size, )).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_path", type=str, default="/app/stat_logs")
    parser.add_argument("--host_url", type=str, default="http://a2a14a139dbc243ba9182849af6e2c77-231856034.us-east-2.elb.amazonaws.com")
    parser.add_argument("--start_block_index", type=int, default=5963940)
    parser.add_argument("--chunk_size", type=int, default=1024)
    args = parser.parse_args()
    collect(args.out_path, args.host_url, args.start_block_index, args.chunk_size)
