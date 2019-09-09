import pandas as pd
from pandas.tseries.offsets import YearBegin

# Bin yearly
def bin_df(df):
    years = df['$date_to'] - YearBegin(1)
    df.index = pd.DatetimeIndex(years, ambiguous='infer').floor('D')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('YS').sum().fillna(0)
