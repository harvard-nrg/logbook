import pandas as pd

# Bin weekly
def bin_df(df):
    weeks = df['$date_to'] - pd.offsets.Week(weekday=5)
    df.index = pd.DatetimeIndex(weeks, ambiguous='infer').floor('D')
    return df.groupby(level=0).count()

def resample_df(df):
    return df.resample('W-SAT').sum().fillna(0)
