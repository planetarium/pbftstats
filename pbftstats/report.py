import argparse

from pbftstats.libs.reports import Reports

def report(log_path, report_path):
    reports = Reports(log_path, report_path)
    reports.run_tx_signer()
    reports.run_lastcommit_vote()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_path", type=str, default="./stat_logs")
    parser.add_argument("--report_path", type=str, default="./reports")
    args = parser.parse_args()
    report(args.log_path, args.report_path)
