#!/usr/bin/env python
# File Processing Pipeline
# Date : 26-Aug-2021
# Initial version v0.1

#Include the modules required
import pandas as pd
import datetime as dt
import numpy as np
import time
import pycountry
import hashlib
import fastparquet
import pyarrow
import openpyxl


# Define the class for Deals
class Deals:
    def __init__(self): pass

    # method to read values from the csv file
    def read_csv_df(self, filename):
        df_csv = pd.read_csv(filename)
        # Change the column value/type to boolean
        df_csv['Active'] =  df_csv['Active'].map({'Yes': True, 'No': False})
        return df_csv

    # method to read values from the excel file
    def read_excel_df(self, filename):
        df_excel = pd.read_excel(filename, sheet_name='deals')
        # Change the column value/type to boolean
        df_excel['Active'] =  df_excel['Active'].map({'Yes': True, 'No': False})
        return df_excel

    # method to write values to the csv file
    def write_csv_df(self, df_in, filename):
        df_in.to_csv(filename, index = False)

    # method to write values to the excel file
    def write_excel_df(self, df_in, filename):
        df_in.to_excel(filename, sheet_name='deals_out', index = False)

    # method to write dataframe to the parquet format file
    def write_df_to_parquet(self, df_in, filename):
        df_in.to_parquet(filename, index = False)

    # method to add the columns into the dataframe
    def add_cols_df(self, df_in):
        # Adding the new column RowHash with hash value of the data columns
        df_in['RowHash'] = pd.util.hash_pandas_object(df_in, index = False)

        # Adding the new column AsOfDate with current date
        df_in['AsOfDate'] = dt.datetime.now().date()

        # Adding the new column ProcessIdentifier with value
        df_in['ProcessIdentifier'] = 10000 + np.arange(len(df_in)) + 1

        # Adding the new column RowNo with value
        df_in.insert(loc=0, column='RowNo', value=np.arange(len(df_in)) + 1)

        return df_in

# code flow starts from here __main__
if __name__ == '__main__':

    # Create an instance of the Deals
    deal_inst = Deals()

    # Call the method to read csv file by passing the filename as argument
    csv_deal_df = deal_inst.read_csv_df('Deals.csv')

    # Call the method to update the dataframe by adding columns by passing the dataframe name as argument
    csv_deal_upd_df = deal_inst.add_cols_df(csv_deal_df)

    # Call the method to write the output csv file by passing the dataframe and filename as arguments
    deal_inst.write_csv_df(csv_deal_upd_df,'out_Deals.csv')

    # Call the method to write the updated dataframe into parquet format by passing the dataframe and filename as arguments
    deal_inst.write_df_to_parquet(csv_deal_upd_df,'out_Deals_csv.parquet')


    # Call the method to read excel file by passing the filename as argument
    excel_deal_df = deal_inst.read_excel_df('Deals.xlsx')

    # Call the method to update the dataframe by adding columns by passing the dataframe name as argument
    excel_deal_upd_df = deal_inst.add_cols_df(excel_deal_df)

    # Call the method to write the output excel file by passing the dataframe and filename as arguments
    deal_inst.write_excel_df(excel_deal_upd_df, 'out_Deals.xlsx')

    # Call the method to write the updated dataframe into parquet format by passing the dataframe and filename as arguments
    deal_inst.write_df_to_parquet(excel_deal_upd_df, 'out_Deals_excel.parquet')
