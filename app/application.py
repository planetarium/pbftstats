import os
import argparse
import json
import requests
import urllib
from dash import Dash, html, dcc, dash_table, Input, Output, ctx
from multiprocessing import Process
import pandas as pd
from collect import collect 
from report import report


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


app = Dash(__name__, title="PBFT Status")
server = app.server

mem = {}
mem["node_status"] = pd.DataFrame()

@app.callback(
    Output("report_lastcommit_vote", "data"), 
    Output("report_lastcommit_vote", "columns"),
    Input("refresh_report_interval", "n_intervals")
)
def read_report_lastcommit_vote(n):
    df = pd.read_csv(os.path.join(report_path, "report_lastcommit_vote.csv"))
    val_key_name = {x["publicKey"]: x["name"] for x in json.loads(requests.get(url=validator_map_url).content)["validators"]}
    df["validator"] = df["validator"].apply(lambda x: val_key_name[x] if x in val_key_name.keys() else x)
    return df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]

@app.callback(
    Output("report_tx_signer", "data"), 
    Output("report_tx_signer", "columns"),
    Input("refresh_report_interval", "n_intervals")
)
def read_report_tx_signer(n):
    df= pd.read_csv(os.path.join(report_path, "report_tx_signer.csv"))
    return df.to_dict("records"), [{"name": i, "id": i} for i in df.columns]

@app.callback(
    Output("validator_status", "data"),
    Output("validator_status", "columns"),
    Input("refresh_status_interval", "n_intervals")
)
def get_validator_status(n):
    path = os.path.join(collect_path, "recent.csv")
    try:
        df_recent = pd.read_csv(path, index_col=0)
    except FileNotFoundError:
        df_recent = pd.DataFrame()

    val_key_name = {x["publicKey"]: x["name"] for x in json.loads(requests.get(url=validator_map_url).content)["validators"]}
    val_addr_name = {x["address"]: x["name"] for x in json.loads(requests.get(url=validator_map_url).content)["validators"]}
    
    try:
        df_recent["proposer"] = df_recent["proposer"].apply(lambda v: val_addr_name[v] if v in val_addr_name else v)
    except Exception as e:
        print(e)
    df_recent.rename(columns=val_key_name, inplace=True)
    df_recent = df_recent.sort_index(ascending=False)
    df_recent.reset_index(inplace=True)

    return df_recent.to_dict("records"), [{"name": i, "id": i} for i in df_recent.columns]

@app.callback(
    Output("latest_ill_cases", "data"),
    Output("latest_ill_cases", "columns"),
    Input("refresh_status_interval", "n_intervals")
)
def get_latest_ill_cases(n):
    path = os.path.join(collect_path, "problem.csv")
    try:
        df_problem = pd.read_csv(path, index_col=0)
    except FileNotFoundError:
        df_problem = pd.DataFrame()

    val_key_name = {x["publicKey"]: x["name"] for x in json.loads(requests.get(url=validator_map_url).content)["validators"]}    
    try:
        df_problem["validators"] = df_problem["validators"].apply(lambda v: ";".join([val_key_name[x] if x in val_key_name else x for x in v.split(";")]))
    except Exception as e:
        print(e)
    df_problem = df_problem.sort_index(ascending=False)
    df_problem.reset_index(inplace=True)

    return df_problem.to_dict("records"), [{"name": i, "id": i} for i in df_problem.columns]

@app.callback(
    Output("status", "style"),
    Output("report", "style"),
    Input("btn_status", "n_clicks"),
    Input("btn_report", "n_clicks"),
)
def update_body(btn_status, btn_report):
    if ctx.triggered_id == "btn_status":
        return {"display": ""}, {"display": "none"}
    elif ctx.triggered_id == "btn_report":
        return {"display": "none"}, {"display": ""}
    return {"display": ""}, {"display": "none"}

app.layout = html.Div([
    html.Header(id="header", children=[
        html.Nav(id="nav", children=[
            html.Ul([
                html.Img(src=app.get_asset_url("logo.svg"), style={"float": "left"}),               
                html.Li(children=[
                    html.A("Validator Status", id="btn_status", href="#validator-status", style={
                        "display": "block",
                        "color": "white",
                        "text-align": "center",
                        "padding": "14px 16px",
                        "text-decoration": "none"})
                ], style={
                    "float": "left",
                }),
                html.Li(children=[
                    html.A("Daily Report", id="btn_report", href="#daily-report", style={
                        "display": "block",
                        "color": "white",
                        "text-align": "center",
                        "padding": "14px 16px",
                        "text-decoration": "none"})
                ], style={
                    "float": "left",
                }),
            ], style={
                "list-style-type": "none",
                "margin": "0",
                "padding": "0",
                "overflow": "hidden",
                "background-color": "#333",
            })
        ])
    ]),

    html.Div(id="body", children=[
        html.Div(id="status", children=[
            html.H1(
                children="Node Status of Validators (Online | Vote)",
                style={
                    "textAlign": "center",
                }
            ),
            html.Legend(
                children="\U0001F7E2 : Present, \U0001F534 : Absent, \U000026AA : Unknown"
            ),
            dash_table.DataTable(
                id="validator_status",
                data=[],
                style_cell={
                    "minWidth": 70, "maxWidth": 95, "width": 70, "textAlign": "center"
                },
                style_header={
                    "backgroundColor": "black",
                    "color": "white",
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(220, 220, 220)',
                    }
                ],
            ),
            html.H1(
                children="Latest Ill Cases",
                style={
                    "textAlign": "center",
                }
            ),
            html.Legend(
                children="Validator was online, but couldn't vote"
            ),
            dash_table.DataTable(
                id="latest_ill_cases",
                data=[],
                style_cell={
                    "minWidth": 70, "maxWidth": 95, "width": 70, "textAlign": "center"
                },
                style_header={
                    "backgroundColor": "black",
                    "color": "white",
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(220, 220, 220)',
                    }
                ],
            ),
            dcc.Interval(id="refresh_status_interval", interval=3*1000, n_intervals=0)
        ]),
        html.Div(id="report", children=[
            html.H1(
                children="Number of PreCommits per each Validator",
                style={
                    "textAlign": "center",
                }
            ),
            dash_table.DataTable(
                id="report_lastcommit_vote",
                data=[],
                style_cell={
                    "minWidth": 95, "maxWidth": 95, "width": 95
                },
                style_header={
                    "backgroundColor": "black",
                    "color": "white",
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(220, 220, 220)',
                    }
                ],
            ),
            html.H1(
                children="Number of Transactions per each Player",
                style={
                    "textAlign": "center",
                }
            ),
            dash_table.DataTable(
                id="report_tx_signer",
                data=[],
                style_cell={
                    "minWidth": 95, "maxWidth": 95, "width": 95
                },
                style_header={
                    "backgroundColor": "black",
                    "color": "white",
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(220, 220, 220)',
                    }
                ],
            ),
            dcc.Interval(id="refresh_report_interval", interval=10*1000, n_intervals=0)
        ], style={"display": "none"}),
    ]) 
], style={"display": "flex", "flex-direction": "column"})

if __name__ == '__main__':
    Process(target=collect, args=(collect_path, host_url, collect_start_block_index, chunk_size_collect, chunk_size_recent, interval_collect, interval_recent, interval_retry, query_validator_key)).start()
    Process(target=report, args=(collect_path, report_path, interval_report)).start()
    app.run()
