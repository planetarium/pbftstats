import argparse
from multiprocessing import Process
from pbftstats.libs.stats import Stats

def main(out_path, host_url, start_block_index, chunk_size):
    stats = Stats(out_path, host_url, start_block_index)
    Process(target=stats.run_tx_signer, args=(chunk_size, )).start()
    Process(target=stats.run_lastcommit_vote, args=(chunk_size, )).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_path", type=str, default="./stat_logs")
    parser.add_argument("--host_url", type=str, default="http://a2a14a139dbc243ba9182849af6e2c77-231856034.us-east-2.elb.amazonaws.com")
    parser.add_argument("--start_block_index", type=int, default=5963940)
    parser.add_argument("--chunk_size", type=int, default=1024)
    args = parser.parse_args()
    main(args.out_path, args.host_url, args.start_block_index, args.chunk_size)
