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
collect_start_block_index = os.getenv("COLLECT_START_BLOCK_INDEX", 5963940)
collect_chunk_size = os.getenv("COLLECT_CHUNK_SIZE", 1024)
report_interval = os.getenv("REPORT_INTERVAL", 60)
status_size = os.getenv("STATUS_SIZE", 30)

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
    val_key_name = {x["publicKey"]: x["name"] for x in json.loads(requests.get(url=validator_map_url).content)["validators"]}
    val_addr_name = {x["address"]: x["name"] for x in json.loads(requests.get(url=validator_map_url).content)["validators"]}
    url = urllib.parse.urljoin(host_url, url="graphql/explorer", allow_fragments=True)
    body = f"query{{blockQuery{{blocks(desc:true limit:{status_size}){{index miner timestamp lastCommit{{round votes{{validatorPublicKey flag}}}}}}}}nodeState{{validators{{publicKey}}}}}}"
    response = requests.post(url=url, json={"query": body})
    if response.status_code != 200:
        raise ConnectionError("Failed to get response")
    data = json.loads(response.content)

    df_vote = pd.DataFrame()
    for block in data["data"]["blockQuery"]["blocks"]:
        for vote in block["lastCommit"]["votes"]:
            df_vote.loc[block["index"] - 1, vote["validatorPublicKey"]] = "\U000026AA" + " | " + "\U0001F7E2" if vote["flag"] == "PreCommit" else "\U000026AA" + " | " + "\U0001F534"
    
    for col in df_vote.columns:
        if col not in mem["node_status"].columns:
            mem["node_status"][col] = "\U0001F534"

    df_vote = df_vote.fillna("\U000026AA" + " | " + "\U0001F534")
    

    online_validators = [x["publicKey"] for x in data["data"]["nodeState"]["validators"]] + ["03461fb858bd9852ab699477a7ae7b11aefff0797ab5b0342d57b1d38f3560a23e"]
    for online_validator in online_validators:
        if online_validator in mem["node_status"].columns:
            mem["node_status"].loc[data["data"]["blockQuery"]["blocks"][0]["index"] + 1, online_validator] = "\U0001F7E2"
    mem["node_status"] = mem["node_status"].fillna("\U0001F534").iloc[-(status_size + 2):, :]
    df_status = mem["node_status"].rename(columns=val_key_name)
    df_status = df_status.reset_index()[::-1]

    for idx in mem["node_status"].index:
        for col in mem["node_status"].columns:
            try:
                df_vote.loc[idx, col] = mem["node_status"].loc[idx, col] + " | " + df_vote.loc[idx, col][-1]
            except:
                df_vote.loc[idx, col] = mem["node_status"].loc[idx, col] + " | " + "\U000026AA"

    df_vote.insert(0, "proposer", "")
    for block in data["data"]["blockQuery"]["blocks"]:
        df_vote.loc[block["index"], "proposer"] = val_addr_name[block["miner"]]

    df_vote.insert(0, "round", "")
    for block in data["data"]["blockQuery"]["blocks"]:
        df_vote.loc[block["index"] - 1, "round"] = block["lastCommit"]["round"]

    df_vote.rename(columns=val_key_name, inplace=True)
    df_vote = df_vote.sort_index(ascending=False)
    df_vote.reset_index(inplace=True)
    
    return df_vote.to_dict("records"), [{"name": i, "id": i} for i in df_vote.columns]

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
    # collect(collect_path, host_url, collect_start_block_index, collect_chunk_size)
    # Process(target=report, args=(collect_path, report_path, report_interval)).start()
    app.run()
