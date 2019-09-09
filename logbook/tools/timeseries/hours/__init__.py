import pandas as pd

# Bin hourly
def bin_df(df):
    df.index = pd.DatetimeIndex(df['$date_to'], ambiguous='infer').floor('H')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('H').sum().fillna(0)
