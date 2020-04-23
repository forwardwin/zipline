from datetime import datetime
import pandas as pd
import pytz
from zipline.examples.momentum_pipeline import before_trading_start

from zipline.examples.buyapple import *

from zipline import run_algorithm

start = pd.Timestamp(datetime(2018, 1, 1, tzinfo=pytz.UTC))
end = pd.Timestamp(datetime(2018, 7, 25, tzinfo=pytz.UTC))

run_algorithm(start=start,
              end=end,
              initialize=initialize,
              capital_base=100000,
              handle_data=handle_data,
              before_trading_start=before_trading_start,
              data_frequency='daily')