import pandas as pd

EVENT_MAP = {
    'beer_festival_2024' : {
        'start' : pd.Timestamp(year=2024, month=9, day=27, tz='UTC'),
        'end' : pd.Timestamp(year=2024, month=9, day=28, tz='UTC'),
        'label' : 'beer_festival'
    },
    'beer_festival_2023' : {
        'start' : pd.Timestamp(year=2023, month=9, day=18, tz='UTC'),
        'end' : pd.Timestamp(year=2023, month=9, day=24, tz='UTC'),
        'label' : 'beer_festival',
    },
    'beer_festival_2022' : {
        'start' : pd.Timestamp(year=2022, month=9, day=5, tz='UTC'),
        'end' : pd.Timestamp(year=2022, month=9, day=30, tz='UTC'),
        'label' : 'beer_festival',
    },
}
