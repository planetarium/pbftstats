from __future__ import annotations
from typing import Optional
import os
import re
import time
import datetime
import dateutil
import urllib
import requests
import json
import numpy as np
import pandas as pd
from functools import reduce

class Collector:

    tx_signer_regex = re.compile(r"^tx-signer_\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])_\d{9}_\d{9}_\d{5}_wip.csv$")
    lastcommit_vote_regex = re.compile(r"^lastcommit-vote_\d{4}\-(0[1-9]|1[012])\-(0[1-9]|[12][0-9]|3[01])_\d{9}_\d{9}_\d{5}_wip.csv$")
    
    def __init__(self, out_path: str, host_url: str, start_block_index: int):
        self.__out_path = out_path
        self.__host_url = urllib.parse.urljoin(host_url, url="graphql/explorer", allow_fragments=True)
        self.__start_block_index = start_block_index
    
    def run_tx_signer(self, chunk_size: int, interval: int):
        print("Gathering transaction signers...")
        while(True):
            wip = self.lookup_wip(self.tx_signer_regex)
            self.update_tx_signer(wip, chunk_size)
            time.sleep(interval)

    def run_lastcommit_vote(self, chunk_size: int, interval: int):
        print("Gathering lastcommit votes...")
        while(True):
            wip = self.lookup_wip(self.lastcommit_vote_regex)
            self.update_lastcommit_vote(wip, chunk_size)
            time.sleep(interval)

    def run_recent_status(self, query_validator_key: str, chunk_size: int, interval: int):
        print("Gathering recent status...")
        while(True):
            self.update_recent_status(query_validator_key, chunk_size)
            time.sleep(interval)

    def lookup_wip(self, regex_wip: re.Pattern) -> Optional[os.DirEntry]:
        os.makedirs(self.__out_path, exist_ok=True)
        for file in os.scandir(self.__out_path):
            if regex_wip.match(file.name):
                return file
        return None

    def update_recent_status(self, query_validator_key: str, chunk_size: int):

        recent_status_path = os.path.join(self.__out_path, "recent.csv")
        problem_gather_path = os.path.join(self.__out_path, "problem.csv")
        
        os.makedirs(self.__out_path, exist_ok=True)

        try:
            df_recent = pd.read_csv(recent_status_path, index_col=0)
        except FileNotFoundError:
            df_recent = pd.DataFrame()

        try:
            df_problem = pd.read_csv(problem_gather_path, index_col=0)
        except FileNotFoundError:
            df_problem = pd.DataFrame(["publicKey"])
        
        unknown = "\U000026AA"
        present = "\U0001F7E2"
        absent = "\U0001F534"

        data = self.parse_response(self.query(self.__host_url, True, 0, chunk_size))

        for block in data["data"]["blockQuery"]["blocks"]:
            block_idx = block["index"]
            df_recent.loc[block_idx, "proposer"] = block["miner"]
            for vote in block["lastCommit"]["votes"]:
                lastcommit_idx = block["index"] - 1
                df_recent.loc[lastcommit_idx, "round"] = block["lastCommit"]["round"]
                try:
                    last_val = df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]]
                except KeyError:
                    last_val = np.nan
                if pd.isnull(last_val):
                    if vote["flag"] == "PreCommit": 
                        df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]] = " | ".join([unknown, present])
                    else:
                        df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]] = " | ".join([unknown, absent])
                else:
                    if vote["flag"] == "PreCommit": 
                        df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]] = df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]][:-1] + present
                    else:
                        df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]] = df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]][:-1] + absent
                        if df_recent.loc[lastcommit_idx, vote["validatorPublicKey"]] == " | ".join([present, absent]):
                            try:
                                prob_val = df_problem.loc[lastcommit_idx, "publicKey"]
                            except KeyError:
                                prob_val = np.nan
                            if pd.isnull(df_problem.loc[lastcommit_idx, "publicKey"]):
                                df_problem.loc[lastcommit_idx, "publicKey"] = vote["validatorPublicKey"]
                            else:
                                df_problem.loc[lastcommit_idx, "publicKey"] = ", ".join(prob_val, vote["validatorPublicKey"])
        
        online_validators = [x["publicKey"] for x in data["data"]["nodeState"]["validators"]] + [query_validator_key]

        online_idx = data["data"]["blockQuery"]["blocks"][0]["index"] + 1
        for col in df_recent.columns[2:]:
            if col in online_validators:
                try:
                    online_val = df_recent.loc[online_idx, col]
                except KeyError:
                    online_val = np.nan
                if pd.isnull(online_val):
                    df_recent.loc[online_idx, col] = " | ".join([present, unknown])
                else:
                    df_recent.loc[online_idx, col] = present + online_val[1:]
            else:
                try:
                    online_val = df_recent.loc[online_idx, col]
                except KeyError:
                    online_val = np.nan
                if pd.isnull(online_val):
                    df_recent.loc[online_idx, col] = " | ".join([absent, unknown])
                else:
                    df_recent.loc[online_idx, col] = absent + online_val[1:]

        df_recent = df_recent.sort_index(ascending=True)
        df_recent = df_recent.iloc[-(chunk_size + 2):]

        df_recent.to_csv(recent_status_path)

        df_problem = df_problem.sort_index(ascending=True)
        df_problem.to_csv(problem_gather_path)

    def update_tx_signer(self, file: Optional[os.DirEntry], chunk_size: int):

        def update_tx_signer_df(df, tx_signers):
            for tx_signer in tx_signers:
                txn = df.loc[df["signer"] == tx_signer, "n_tx"]
                if txn.count() == 0:
                    df.loc[len(df.index)] = [tx_signer, 1]
                else:
                    df.loc[df["signer"] == tx_signer, "n_tx"] = txn + 1
            return df

        if file:
            wip = self.parse_wip_name(file)
        else:
            wip = { "flag": "tx-signer", "date": datetime.date.min, "start": self.__start_block_index, "end": self.__start_block_index - 1, "summary": [] }
        data = self.get_data_from_wip(wip["end"], chunk_size)

        blocks_current, blocks_next_nested = self.get_block_slices(wip["date"], self.get_blocks(data))

        if file:
            df_current = pd.read_csv(file.path)
        
        for block in blocks_current:
            tx_signers = self.get_tx_signers(block)
            df_current = update_tx_signer_df(df_current, tx_signers)

        if file:
            fpath = file.path
        else:
            fpath = None

        if len(blocks_current) > 0:
            wip["end"] = self.get_height(blocks_current[-1])
            wip["summary"] = [f"{len(df_current):05}"]
            file_name = self.construct_wip_name(wip)
            fpath = os.path.join(self.__out_path, file_name)
            df_current.to_csv(fpath, index=False)
            os.remove(file.path)
            
        if len(blocks_next_nested) > 0 and fpath:
            os.rename(fpath, fpath.replace("_wip", ""))

        for i, blocks_next in enumerate(blocks_next_nested):
            df_next = pd.DataFrame(columns=["signer", "n_tx"])
            for block in blocks_next:
                tx_signers = self.get_tx_signers(block)
                df_next = update_tx_signer_df(df_next, tx_signers)

            if len(blocks_next) > 0:
                wip["date"] = self.get_timestamp(blocks_next[-1])
                print(f"Gathering transaction signers on {wip['date']}")
                wip["start"] = self.get_height(blocks_next[0])
                wip["end"] = self.get_height(blocks_next[-1])
                wip["summary"] = [f"{len(df_next):05}"]
                file_name = self.construct_wip_name(wip)
                fpath = os.path.join(self.__out_path, file_name)
                if i < (len(blocks_next_nested) - 1):
                    fpath = fpath.replace("_wip", "")
                df_next.to_csv(fpath, index=False)
                
    def update_lastcommit_vote(self, file: Optional[os.DirEntry], chunk_size: int):

        def update_lastcommit_vote_df(df, votes):
            for vote in votes:
                if vote["flag"] == "PreCommit":
                    n_precommit = df.loc[df["validator"] == vote["validatorPublicKey"], "n_precommit"]
                    if n_precommit.count() == 0:
                        df.loc[len(df.index)] = [vote["validatorPublicKey"], 1]
                    else:
                        df.loc[df["validator"] == vote["validatorPublicKey"], "n_precommit"] = n_precommit + 1
                else:
                    n_precommit = df.loc[df["validator"] == vote["validatorPublicKey"], "n_precommit"]
                    if n_precommit.count() == 0:
                        df.loc[len(df.index)] = [vote["validatorPublicKey"], 0]
            return df

        if file:
            wip = self.parse_wip_name(file)
        else:
            wip = { "flag": "lastcommit-vote", "date": datetime.date.min, "start": self.__start_block_index, "end": self.__start_block_index - 1, "summary": [] }
        data = self.get_data_from_wip(wip["end"], chunk_size)

        blocks_current, blocks_next_nested = self.get_block_slices(wip["date"], self.get_blocks(data))

        if file:
            df_current = pd.read_csv(file.path)
        
        for block in blocks_current:
            votes = self.get_votes(block)
            df_current = update_lastcommit_vote_df(df_current, votes)

        if file:
            fpath = file.path
        else:
            fpath = None
            
        if len(blocks_current) > 0:
            wip["end"] = self.get_height(blocks_current[-1])
            wip["summary"] = [f"{len(df_current):05}"]
            file_name = self.construct_wip_name(wip)
            fpath = os.path.join(self.__out_path, file_name)
            df_current.to_csv(fpath, index=False)
            os.remove(file.path)
            
        if len(blocks_next_nested) > 0 and fpath:
            os.rename(fpath, fpath.replace("_wip", ""))

        for i, blocks_next in enumerate(blocks_next_nested):
            df_next = pd.DataFrame(columns=["validator", "n_precommit"])
            for block in blocks_next:
                votes = self.get_votes(block)
                df_next = update_lastcommit_vote_df(df_next, votes)

            if len(blocks_next) > 0:
                wip["date"] = self.get_timestamp(blocks_next[-1])
                print(f"Gathering lastcommit votes on {wip['date']}")
                wip["start"] = self.get_height(blocks_next[0])
                wip["end"] = self.get_height(blocks_next[-1])
                wip["summary"] = [f"{len(df_next):05}"]
                file_name = self.construct_wip_name(wip)
                fpath = os.path.join(self.__out_path, file_name)
                if i < (len(blocks_next_nested) - 1):
                    fpath = fpath.replace("_wip", "")
                df_next.to_csv(fpath, index=False)

    def get_data_from_wip(self, end: int, chunk_size: int) -> dict:
        response = self.query(self.__host_url, False, end + 1, chunk_size)
        data = self.parse_response(response)
        return data
    
    @staticmethod
    def construct_wip_name(parsed: dict) -> str:
        return f"{parsed['flag']}_{parsed['date'].strftime('%Y-%m-%d')}_{parsed['start']:09}_{parsed['end']:09}_{'_'.join(parsed['summary'])}_wip.csv"

    @staticmethod
    def parse_wip_name(file: Optional[os.DirEntry]) -> dict:
        splitted = file.name.split("_")
        return {
            "flag": splitted[0],
            "date": datetime.datetime.strptime(splitted[1], "%Y-%m-%d").date(), 
            "start": int(splitted[2]), 
            "end": int(splitted[3]),
            "summary": splitted[4:-1]
        }

    @staticmethod
    def query(url: str, desc: bool, start_index: int, chunk_size: int) -> requests.Response:
        body = (f"query{{blockQuery{{blocks(desc:{str(desc).lower()} offset:{start_index} limit:{chunk_size})"
        f"{{index miner transactions{{publicKey}} timestamp lastCommit{{round votes{{validatorPublicKey flag}}}}}}}}nodeState{{validators{{publicKey}}}}}}")
        return requests.post(url=url, json={"query": body})

    @staticmethod
    def parse_response(response: requests.Response) -> dict:
        if response.status_code != 200:
            raise ConnectionError("Failed to get response")
        data = json.loads(response.content)
        if "errors" in data:
            raise IndexError("The block has not been generated yet")
        return data

    @staticmethod
    def get_block_slices(wip_date: datetime.datetime, blocks: dict) -> tuple:
        try:
            slice_idx = next(i for i, block in enumerate(blocks) if Collector.get_timestamp(block) > wip_date)
        except StopIteration:
            return blocks, []

        def subslicing(acc:list, cur:list) -> list:
            if Collector.get_timestamp(cur) > Collector.get_timestamp(acc[-1][-1]):
                acc.append([cur])
            else:
                acc[-1].append(cur)
            return acc

        return blocks[:slice_idx], reduce(subslicing, blocks[(slice_idx + 1):], [[blocks[slice_idx]]])
    
    @staticmethod
    def get_blocks(data: dict) -> list:
        return data["data"]["blockQuery"]["blocks"]

    @staticmethod
    def get_tx_signers(block: dict) -> list:
        return [tx["publicKey"] for tx in block["transactions"]]

    @staticmethod
    def get_timestamp(block: dict) -> datetime.datetime:
        return dateutil.parser.isoparse(block["timestamp"]).date()

    @staticmethod
    def get_votes(block: dict) -> list:
        return block["lastCommit"]["votes"]

    @staticmethod
    def get_height(block: dict) -> int:
        return block["index"]
        