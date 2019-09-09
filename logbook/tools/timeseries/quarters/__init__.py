import pandas as pd
from pandas.tseries.offsets import QuarterBegin

# Bin quarterly
def bin_df(df):
    quarters = df['$date_to'] - QuarterBegin(startingMonth=1)
    df.index = pd.DatetimeIndex(quarters, ambiguous='infer').floor('D')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('QS').sum().fillna(0)
