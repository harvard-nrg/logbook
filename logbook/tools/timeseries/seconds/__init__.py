import pandas as pd

# Bin by seconds
def bin_df(df):
    #df = df.set_index('$date_to')
    #df.index = pd.Series(df['$date_to']).dt.floor(freq='S',ambiguous='infer')
    df.index = pd.DatetimeIndex(data=df['$date_to']).floor('S', ambiguous='NaT')
    df = df.groupby(level=0).count()
    return df[~df.index.duplicated(keep='first')]

def resample_df(df):
    return df.resample('S').sum().fillna(0)
