import os
import argparse
from dash import Dash, html, dcc
from multiprocessing import Process
from .collect import collect 
from .report import report

try:
    collect_path = os.environ["COLLECT_PATH"]
except KeyError:
    collect_path = "./stat_logs"

try:
    report_path = os.environ["REPORT_PATH"]
except KeyError:
    report_path = "./reports"

try:
    host_url = os.environ["HOST_URL"]
except KeyError:
    host_url = "http://a2a14a139dbc243ba9182849af6e2c77-231856034.us-east-2.elb.amazonaws.com"


app = Dash(__name__)

app.layout = html.Div([], style={'display': 'flex', 'flex-direction': 'row'})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_block_index", type=int, default=5963940)
    parser.add_argument("--chunk_size", type=int, default=1024)
    parser.add_argument("--report_interval", type=int, default=3600)
    args = parser.parse_args()
    collect(collect_path, host_url, args.start_block_index, args.chunk_size)
    Process(target=report, args=(collect_path, report_path, args.report_interval)).start()
    app.run_server(debug=True)