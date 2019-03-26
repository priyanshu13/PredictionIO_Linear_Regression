"""
Import sample data for classification engine
"""

import predictionio
import argparse
import csv 
import pandas as pd


def import_events(client, file):
    #df = pd.read_csv(path)
    #read = df.to_dict()
    f = open(file, 'r')
    df = pd.DataFrame(list(f))
    read = df.to_dict()
    #read = csv.DictReader(f)
    cleanup_nums={'MONDAY':0,'TUESDAY':1,'WEDNESDAY':2, 'THURSDAY':3, 'FRIDAY':4, 'SATURDAY':5,'SUNDAY':6}

    print("Importing data...")
    for row in read:
        no = float(row['no'])
        atm_name = row['atm_name']
        weekday = row['weekday']
        festival_religion = row['festival_religion']
        working_day = row['working_day']
        holiday_sequence = row['holiday_sequence']
        trans_date_set = float(row['trans_date_set'])
        trans_month = float(row['trans_month'])
        trans_year = float(row['trans_year'])
        prevweek_mean = float(row['prevweek_mean'])
        total_amount_withdrawn = float(row['total_amount_withdrawn'])
            
 
        client.create_event(
        event="Atm_regression",
        entity_type="Regression_atm",
        entity_id= no, 
        properties= {
        "no"  : no ,
        "atm_name" : atm_name,
        "weekday" : week,
        "festival_religion" : festival_religion,
        "working_day" : working_day,
        "holiday_sequence" : holiday_sequence,
        "trans_date_set" : trans_date_set, 
        "trans_month" : trans_month, 
        "trans_year" : trans_year, 
        "prevweek_mean" : prevweek_mean, 
        "total_amount_withdrawn" : total_amount_withdrawn
      
      }
    )
    f.close()
  

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for Regression engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/sample_data.txt")

  args = parser.parse_args()
  print (args)

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
import_events(client, args.file)

