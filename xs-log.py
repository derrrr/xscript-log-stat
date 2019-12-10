import os
import re
import codecs
import ntpath
import shutil
import configparser
import numpy as np
import pandas as pd
from chardet import detect
from datetime import datetime, timedelta

def _load_config():
    config_path = "./config.ini"
    with open(config_path, "rb") as ef:
        config_encoding = detect(ef.read())["encoding"]
    config = configparser.ConfigParser()
    config.read_file(codecs.open(config_path, "r", config_encoding))
    return config

def mkdirs(path):
    if not os.path.exists(path):
        os.makedirs(path)

def absoluteFilePaths(directory):
   for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
           yield os.path.abspath(os.path.join(dirpath, f))

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def get_encoding(path):
    with open(path, "rb") as ef:
        file_encoding = detect(ef.read())["encoding"]
        if file_encoding.lower() == "big5":
            file_encoding = "cp950"
    return file_encoding

def convert_encoding(file_path, output_encoding):
    input_encoding = get_encoding(file_path)
    temp_path = "{}.tmp".format(file_path)
    with open(file_path, "r", encoding=input_encoding) as sourceFile, \
        open(temp_path, "w", encoding=output_encoding) as targetFile:
        contents = sourceFile.read()
        targetFile.write(contents)
    os.remove(file_path)
    shutil.move(temp_path, file_path)

class XS_stat:
    def __init__(self):
        self.config = _load_config()
        self.raw_dir = self.config["XScript"]["raw_log_dir"]
        self.fixed_dir = self.config["XScript"]["fixed_log_dir"]
        self.concat_dir = self.config["XScript"]["stat_dir"]
        self.xq_path = self.get_xq_path()

    def del_empty(self, input_path, output_path):
        convert_encoding(input_path, "utf-8-sig")

        with open(input_path, "r", encoding="utf-8-sig") as inFile, \
            open(output_path, "w", encoding="utf-8-sig") as outFile:
            for line in inFile:
                line = line.replace(" ", ",")
                line = re.sub(",\n", "\n", line)
                if line.strip():
                    outFile.write(line)

    def df_script(self, path, script_name):
        df = pd.read_csv(path, encoding="utf-8-sig", header=None, engine="python")
        df = df.drop(df.columns[3:], axis=1)
        cols = ["Ticker", "Name", "Date"]
        df.columns = cols
        df["Date"] = df["Date"].astype("int64").astype("str").fillna("")
        df["Date"] = df["Date"].apply(lambda x: datetime.strptime(x, "%Y%m%d").date()).reset_index(drop=True)
        df[script_name] = 1
        return df

    def df_summation(self, df_con, days):
        date_filter = datetime.strptime(self.date_last, "%Y%m%d") - timedelta(days=days)
        df_con = df_con[df_con["Date"] > date_filter.date()].reset_index(drop=True).drop(["Date"], axis=1)
        df_period = df_con.groupby(["Ticker", "Name"]).sum().reset_index()
        df_period["Sum"] = df_period.sum(axis=1)
        df_period[df_period.eq(0)] = np.nan
        df_period = df_period.sort_values(["Sum", "Ticker"], ascending=[False, True])

        df_period_xq = self.xq_merge(df_period)
        new_cols = df_period.columns[:2].tolist() + ["產業地位"] + df_period.columns[2:].tolist()
        df_period_xq = df_period_xq[new_cols]
        return df_period_xq

    def get_xq_path(self):
        xq_dir = "./xq"
        if not os.path.exists(xq_dir):
            print("沒有xq公司簡介")
            sys.exit()
        elif not os.listdir(xq_dir):
            print("沒有xq公司簡介")
            sys.exit()
        else:
            xq_list = os.listdir(xq_dir)
            self.xq_date = datetime.strptime(re.search("\d{4}-\d{2}\d{2}", xq_list[-1])[0], "%Y-%m%d").date()
        xq_list.sort(reverse=False)
        return "{}/{}".format(xq_dir, xq_list[-1])

    def xq_merge(self, df):
        convert_encoding(self.xq_path, "utf-8-sig")
        xq_encoding = get_encoding(self.xq_path)
        dfxq = pd.read_csv(self.xq_path, encoding=xq_encoding, engine="python")
        dfxq["代碼"] = dfxq["代碼"].apply(lambda x: "{}{}".format(x, ".TW"))
        dfxq.drop(labels=["商品"], axis=1, inplace=True)
        dfx = df.merge(dfxq, left_on="Ticker", right_on="代碼", how="left", indicator=False)
        dfx.drop(labels=["代碼"], axis=1, inplace=True)
        return dfx

    def process(self):
        start_time = datetime.now().replace(microsecond=0)
        mkdirs(self.raw_dir)
        mkdirs(self.fixed_dir)

        print("\n--{} 裡面請只留最新要統計的log--".format(self.raw_dir))
        print("--請先刪除所有log，再執行XS去print最新的log--\n")

        raw_paths = absoluteFilePaths(self.raw_dir)
        raw_files = [path_leaf(path) for path in raw_paths]
        print("-找到{}個log-".format(len(raw_files)))

        for raw_file in raw_files:
            in_path = "{}/{}".format(self.raw_dir, raw_file)
            out_path = "{}/{}".format(self.fixed_dir, raw_file)
            self.del_empty(in_path, out_path)
        print("-log前處理完成-")

        fixed_paths = absoluteFilePaths(self.fixed_dir)
        self.date_list = []
        dfs = []
        for fixed_path in fixed_paths:
            fixed_base = os.path.splitext(path_leaf(fixed_path))[0].replace("-QL", "")
            self.date_list.append(int(fixed_base.split("_")[0]))
            self.date_last = str(max(self.date_list))
            script_name = fixed_base.split("_")[-1]
            dfs.append(self.df_script(fixed_path, script_name))

        df_con = pd.concat(dfs, sort=False, ignore_index=True)
        df_con = df_con.sort_values(["Ticker", "Date"], ascending=[True, True])

        periods = [1, 5, 20, 60]

        mkdirs(self.concat_dir)
        self.concat_path = "{}/{}_xs_stat.xlsx".format(self.concat_dir, self.date_last)
        with pd.ExcelWriter(self.concat_path, engine="xlsxwriter", options={"in_memory": True}) as writer:
            df_con.to_excel(writer, sheet_name=self.date_last, index=False, encoding="utf-8-sig")
            for period in periods:
                df_period = self.df_summation(df_con, period)
                df_period.to_excel(writer, sheet_name=str("{}天".format(period)), index=False, encoding="utf-8-sig")

        shutil.rmtree(self.fixed_dir)
        print("--log合併完成--")
        print("==花費時間: {}==".format(str(datetime.now().replace(microsecond=0) - start_time)))

XS = XS_stat()
XS.process()