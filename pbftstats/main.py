import argparse

from pbftstats.libs.stats import Stats

def main(out_path, host_url, start_block_index):
    Stats(out_path, host_url, start_block_index).run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out_path", type=str, default="./stat_logs")
    parser.add_argument("--host_url", type=str, default="http://a2a14a139dbc243ba9182849af6e2c77-231856034.us-east-2.elb.amazonaws.com")
    parser.add_argument("--start_block_index", type=int, default=5719259)
    args = parser.parse_args()
    main(args.out_path, args.host_url, args.start_block_index)
