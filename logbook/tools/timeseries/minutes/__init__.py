import pandas as pd

# Bin by minutes
def bin_df(df):
    df.index = pd.DatetimeIndex(df['$date_to'], ambiguous='infer').floor('T')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('T').sum().fillna(0)
