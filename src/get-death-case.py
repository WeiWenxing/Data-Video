# -*- coding: utf-8 -*-

import csv
import sys
import pandas as pd
from datetime import datetime


def getArea(input):
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')  # 本例的time格式
    df = pd.read_csv(input, usecols=['countryEnglishName', 'provinceEnglishName', 'province_deadCount', 'updateTime'],
                     parse_dates=['updateTime'], date_parser=dateparse)
    result1 = df[df["countryEnglishName"] == df["provinceEnglishName"]]
    result1.rename(columns={'countryEnglishName': 'type', 'province_deadCount': 'value'}, inplace=True)
    result1 = result1.drop(columns=['provinceEnglishName'])
    result1['date'] = result1['updateTime'].dt.date
    result1 = result1.drop(columns=['updateTime'])
    result1 = result1[~result1['type'].str.contains('China')]  # 删除中国
    result1 = result1.drop_duplicates(subset=['type', 'date'], keep='first')  # 去重
    #    result1.drop(result1[result1.type < 50].index, inplace=True)
    col_name = result1.columns.tolist()  # 将数据框的列名全部提取出来存放在列表里
    col_name.insert(0, 'name')  # 增加一列name
    result2 = result1.reindex(columns=col_name)
    result2['type'].replace('United States of America', "U.S", inplace=True)
    result2['name'] = result2['type']

    #    result2.to_csv(output, index=False)
    return result2


def getOverall(input):
    #    df = pd.read_csv(input, encoding="gbk", usecols=['type', 'value', 'date'])
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    result1 = pd.read_csv(input, usecols=['deadCount', 'updateTime'], parse_dates=['updateTime'],
                          date_parser=dateparse)
    result1.rename(columns={'deadCount': 'value'}, inplace=True)
    result1['date'] = result1['updateTime'].dt.date
    result1 = result1.drop(columns=['updateTime'])
    result1 = result1.drop_duplicates(subset=['date'], keep='first')
    col_name = result1.columns.tolist()  # 将数据框的列名全部提取出来存放在列表里
    col_name.insert(0, 'type')
    col_name.insert(0, 'name')
    result2 = result1.reindex(columns=col_name)
    result2['type'] = 'China'
    result2['name'] = 'China'

    #    result2.to_csv(output, index=False)
    return result2


def insert_table(index, org_df, insert_df):
    # if index == 0:
    #     return insert_df.append(org_df, ignore_index=True)
    df1 = org_df.loc[:index - 1]
    df2 = org_df.loc[index:]
    df = df1.append(insert_df, ignore_index=True).append(df2, ignore_index=True)
    return df


def comp(df1, df2):
    result1 = pd.concat([df1, df2], ignore_index=True)
    result2 = result1.drop_duplicates(subset=['type', 'date'], keep='first')
    return result2


def insertLostDate(sample, lost):
    exp_date_list = sample['date'].unique()
    out = sample
    for country in lost['name'].drop_duplicates().unique():
        country_item = lost[lost['name'] == country]
        country_item = country_item.sort_values('date', ascending=False)

        data_list = country_item['date'].unique()
        value_list = country_item['value'].tolist()
        i = 0
        item = country_item
        for j in range(0, len(exp_date_list)):
            # print("i = " + str(i) + "; j = " + str(j))
            if i >= len(value_list):
                break
            exp_date = exp_date_list[j]
            act_date = data_list[i]
            act_value = value_list[i]
            # print("exp_date = " + str(exp_date) + "; act_date = " + str(act_date) + "; act_value = " + str(act_value))
            if exp_date == act_date:
                i = i + 1
                continue
            # print(country + ":" + str(exp_date))
            insert = pd.DataFrame({'name': [country], 'type': [country], 'value': [act_value], 'date': [exp_date]})
            item = insert_table(j, item, insert)
        # dateStart = sortItem.iat[sortItem.shape[0] - 1, 3]
        # print((dateEnd))
        # print(dateStart)

        item = item.sort_values('date', ascending=False)
        # path = "country/" + country + ".csv"
        # item.to_csv(path, index=False)
        out = pd.concat([out, item], ignore_index=True)
    return out


world = getArea("DXYArea.csv")
china = getOverall("DXYOverall.csv")
result = insertLostDate(china, world)

result = result.drop_duplicates(subset=['type', 'date'], keep='first')
result.to_csv(sys.argv[1], index=False)
