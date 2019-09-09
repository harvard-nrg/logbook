import pandas as pd
from pandas.tseries.offsets import MonthBegin

# Bin monthly
def bin_df(df):
    months = df['$date_to'] - MonthBegin(1)
    df.index = pd.DatetimeIndex(months, ambiguous='infer').floor('D')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('MS').sum().fillna(0)
