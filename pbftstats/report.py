import argparse
import time

from pbftstats.libs.reporter import Reporter

def report(log_path, report_path, interval):
    reporter = Reporter(log_path, report_path)

    if interval < 0:
        print("Reporting start...")
        reporter.run_tx_signer()
        reporter.run_lastcommit_vote()
    else:
        while(True):
            print("Reporting start...")
            reporter.run_tx_signer()
            reporter.run_lastcommit_vote()
            time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_path", type=str, default="./stat_logs")
    parser.add_argument("--report_path", type=str, default="./reports")
    parser.add_argument("--interval", type=int, default=-1)
    args = parser.parse_args()
    report(args.log_path, args.report_path, args.interval)
