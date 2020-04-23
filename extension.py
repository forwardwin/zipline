from zipline.data.bundles import register
from zipline.data.bundles.viadb import viadb
import pandas as pd
from cn_stock_holidays.zipline.default_calendar import shsz_calendar

equities1 = {
}  # 没有则是代表全部加载

register(
    'astock',  # name this whatever you like
    viadb(equities1),
    calendar_name='SHSZ'
)
