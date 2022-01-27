# -*- coding: utf-8 -*-
import pyspark
from pyspark import StorageLevel
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,DoubleType,DateType,StructType,StructField
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit
import re
from datetime import datetime, timedelta
#import numpy as np
import math
import time

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('input', type=str)
parser.add_argument('output', type=str)
parser.add_argument('--date_from', type=str)
parser.add_argument('--date_to', type=str)
parser.add_argument('--time_from', type=int)
parser.add_argument('--time_to', type=int)
parser.add_argument('--num_reducers', type=int)
parser.add_argument('--securities', type=str)
parser.add_argument('--widths', type=str)
parser.add_argument('--shifts', type=str)
parser.add_argument('--parquet_folder', type=str)
args = parser.parse_args()

input = args.input
output = args.output
if args.time_from == None:
    time_from = 1000
else:
    time_from = args.time_from
if args.time_to == None:
    time_to = 1800
else:
    time_to = args.time_to
if args.date_from == None:
    date_from = 20110111
else:
    date_from = args.date_from
if args.date_to == None:
    date_to = 20110112
else:
    date_to = args.date_to
if args.widths == None:
    widths = [1, 5, 10]
else:
    widths = [int(width) for width in args.widths.split(',')]
if args.shifts == None:
    shifts = [0, 1, 2, 3, 4, 5]
else:
    shifts = [int(shift) for shift in args.shifts.split(',')]
if args.securities == None:
    securities = 'SVH1|EDH1'
else:
    securities = args.securities
if args.num_reducers == None:
    reducers = 1
else:
    reducers = args.num_reducers
    
spark = SparkSession.builder.appName('Job').getOrCreate()
sc = spark.sparkContext
conf = pyspark.SparkConf()
conf.set("spark.default.parallelism", reducers)


lines = sc.textFile(input)
begin = str(date_from)+str(time_from)+'00000'
s = str(date_to)
date = datetime(year=int(s[0:4]), month=int(s[4:6]), day=int(s[6:8])) - timedelta(days=1)
end = date.strftime("%Y%m%d")+str(time_to)+'00000'

fin_data_lines = lines.map(lambda l: l.split(','))
fin_data_lines = fin_data_lines.filter(lambda tokens: re.match(securities,tokens[0]) != None).filter(lambda tokens: tokens[0] != "#SYMBOL")

def moment_to_unix_with_ms(moment):
  return int(math.ceil(time.mktime(time.strptime(moment, '%Y%m%d%H%M%S%f'))))

def moment_to_candle_begin(moment, candle_width):
  unix_begin = moment_to_unix_with_ms(moment)
  return(datetime.utcfromtimestamp(unix_begin - unix_begin%candle_width).strftime('%Y%m%d%H%M%S'))

pairs = sc.parallelize([])
for width in widths:
  pairs = pairs.union(fin_data_lines.map(lambda deal: ((deal[0],moment_to_candle_begin(deal[2], width), width), float(deal[4]))))
df_pairs = spark.createDataFrame(pairs)
#df_pairs = df_pairs.groupby("_1").agg(f.first("_2").alias("OPEN"),f.max("_2").alias("HIGH"), f.min("_2").alias("LOW"),
#                                       f.last("_2").alias("CLOSE")).sort("_1")
df_pairs = df_pairs.groupby("_1").agg(((f.last("_2") - f.first("_2"))/f.first("_2")).alias("INCREASE")).sort("_1")

def rdd_to_tup(row):
  return(((row[0][0], moment_to_unix_with_ms(row[0]["_2"]+"000"), row[0]["_3"]),row["INCREASE"]))

candle_widths = df_pairs.rdd.map(rdd_to_tup)

candle_widths_shifts = sc.parallelize([])
for shift in shifts:
  if shift != 0:
    candle_widths_shifts = candle_widths_shifts.union(candle_widths.map(lambda row: ((row[0][0], row[0][1]-shift*row[0][2],row[0][2],shift, "b", row[1]))))
    candle_widths_shifts = candle_widths_shifts.union(candle_widths.map(lambda row: ((row[0][0], row[0][1]+shift*row[0][2],row[0][2],shift, "f", row[1]))))
  else:
    candle_widths_shifts = candle_widths_shifts.union(candle_widths.map(lambda row: ((row[0][0], row[0][1]-shift*row[0][2],row[0][2],shift, "n", row[1]))))

candle_widths_shifts = spark.createDataFrame(candle_widths_shifts, ["SYMBOL", "MOMENT", "CANWIDTH", "SHIFT", "SHIFTDIR","INCREASE"])

df_ws = candle_widths_shifts.selectExpr("SYMBOL as SYMBOL2", "MOMENT as MOMENT2", "SHIFT as SHIFT2", "SHIFTDIR as SHIFTDIR2", "INCREASE as INCREASE2")
candles_to_corr = candle_widths_shifts.join(df_ws, on = [candle_widths_shifts.SYMBOL != df_ws.SYMBOL2,
                                       candle_widths_shifts.MOMENT == df_ws.MOMENT2]) \
                                       .filter(candle_widths_shifts.MOMENT>=moment_to_unix_with_ms(begin)) \
                                       .filter(candle_widths_shifts.MOMENT<moment_to_unix_with_ms(end))

candles_to_corr = candles_to_corr.select("SYMBOL", "SYMBOL2", "CANWIDTH", "SHIFT", "SHIFT2", "SHIFTDIR","INCREASE", "INCREASE2")\
.filter(candles_to_corr.SHIFT2 == 0)

def make_data_to_pre_corr(row):
  if row[1] > row[0]:
    return ((row[0], row[1], row[2], row[3], row[5]), (row[6], row[7]))
  else:
    return ((row[1], row[0], row[2], row[3], row[5]), (row[7], row[6]))

one_action_left = candles_to_corr.rdd.map(make_data_to_pre_corr)

def tup_to_list(x):
  x1 = []
  x2 = []
  for i in x:
    x1.append(i[0])
    x2.append(i[1])
  return x1, x2

def correlation(x,y):
  sum_x = 0
  sum_y = 0
  sum_x2 = 0
  sum_y2 = 0
  sum_xy = 0
  n = len(x)
  for i in range(n):
    sum_x += x[i]
    sum_y += y[i]
    sum_x2 += x[i]**2
    sum_y2 += y[i]**2
    sum_xy += x[i]*y[i]
  try:
    corr =  ( (sum_xy - sum_x*sum_y/n)/n ) / ( math.sqrt((sum_x2/n - (sum_x/n)**2)*(sum_y2/n - (sum_y/n)**2)) )
    return corr
  except ZeroDivisionError:
    return math.nan

result = one_action_left.groupByKey().mapValues(tup_to_list).map(lambda row: (row[0], correlation(row[1][0], row[1][1])))\
.filter(lambda row: math.isnan(row[1]) != True).map(lambda row: row[0][0]+" "+row[0][1]+" "+str(row[0][2])+" "+str(row[0][3])+" "+str(row[1]))

                                                    
result.saveAsTextFile(output)

#df_pairs.persist(pyspark.StorageLevel.MEMORY_ONLY)



#result.write.save('dataset.parquet', format='parquet')