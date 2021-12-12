#!/usr/bin/env python

import argparse
import logging
import requests
from datetime import datetime
import pytz
import json

url = "https://api.exchange.coinbase.com/products/{}/candles"
gran = 60

def to_utc_epoch(timestamp):
  utc = pytz.timezone("UTC")
  dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
  t = int(utc.localize(dt).timestamp())
  return t

def to_timestamp(epoch):
  return datetime.utcfromtimestamp(epoch).strftime("%Y-%m-%dT%H:%M:%S")

def candle_extraction(pid, s_timestamp, e_timestamp, outfile):
  session = requests.Session()
  start_epoch = to_utc_epoch(s_timestamp)
  end_epoch = to_utc_epoch(e_timestamp)
  output = []
  for epoch in range(start_epoch, end_epoch, gran * 300):
    start_timestamp = to_timestamp(epoch)
    end_timestamp = to_timestamp(epoch + gran * 299)
    params = {'granularity' : gran, 'start' : start_timestamp, 'end' : end_timestamp}
    resp = session.request("GET", url.format(pid), params=params, timeout=30)
    data = resp.json()
    output.extend(data)
  with open(outfile, 'w') as df:
    df.write(json.dumps(output))

def setup_logging(args):
  logger = logging.getLogger()
  logger.setLevel(logging.DEBUG)
  fhandler = logging.FileHandler(args.logfile, mode='w')
  formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-5s %(message)s', datefmt='%H:%M:%s')
  fhandler.setFormatter(formatter)
  logger.addHandler(fhandler)
  chandler = logging.StreamHandler()
  chandler.setLevel(logging.INFO)
  chandler.setFormatter(formatter)
  logger.addHandler(chandler)

def build_parser():
  parser = argparse.ArgumentParser(description="parameters")
  parser.add_argument("--pid", type=str, help="product id", required=True)
  parser.add_argument("--start", type=str, help="start timestamp in format YYYY-mm-ddTHH:MM:SS", required=True)
  parser.add_argument("--end", type=str, help="end timestamp in format YYYY-mm-ddTHH:MM:SS", required=True)
  parser.add_argument("--outfile", type=str, help="output filename", required=True)
  parser.add_argument("--logfile", type=str, help="log filename", required=True)
  return parser

def main():
  parser = build_parser()
  args = parser.parse_args()
  setup_logging(args)
  logging.info("Extraction from {} to {} to output file {}".format(args.start, args.end, args.outfile))
  candle_extraction(args.pid, args.start, args.end, args.outfile)
  logging.info("Extraction Complete")

if __name__ == '__main__':
  main()
