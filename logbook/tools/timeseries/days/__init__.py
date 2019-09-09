import pandas as pd

# Bin daily
def bin_df(df):
    df.index = pd.DatetimeIndex(df['$date_to'], ambiguous='infer').floor('D')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('D').sum().fillna(0)
