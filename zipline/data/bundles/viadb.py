#
# Ingest stock csv files to create a zipline data bundle

import os

import numpy  as np
import pandas as pd
import datetime
from cn_stock_holidays.zipline.default_calendar import shsz_calendar

boDebug = False  # Set True to get trace messages

from zipline.utils.cli import maybe_show_progress

def viadb(symbols, start=None, end=None):
    # strict this in memory so that we can reiterate over it.
    # (Because it could be a generator and they live only once)
    tuSymbols = tuple(symbols)

    if boDebug:
        print "entering viacsv.  tuSymbols=", tuSymbols

    # Define our custom ingest function
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):
        if boDebug:
            print "entering ingest and creating blank dfMetadata"
        import sqlite3
        IFIL = "History.db"
        conn = sqlite3.connect(IFIL, check_same_thread=False)

        dfMetadata = pd.DataFrame(np.empty(len(tuSymbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

        if boDebug:
            print "dfMetadata", type(dfMetadata)
            print  dfMetadata.describe
            print

        # We need to feed something that is iterable - like a list or a generator -
        # that is a tuple with an integer for sid and a DataFrame for the data to
        # daily_bar_writer

        liData = []
        iSid = 0
        for S in tuSymbols:
            if boDebug:
                print "S=", S
            # dfData=pd.read_sql(IFIL,index_col='Date',parse_dates=True).sort_index()
            query = "select * from '%s' order by date desc" % S
            dfData = pd.read_sql(sql=query, con=conn, index_col='date', parse_dates=['date']).sort_index()
            # dfData = dfData.set_index('date')

            if boDebug:
                print "read_sqllite dfData", type(dfData), "length", len(dfData)

            # the start date is the date of the first trade and
            start_date = dfData.index[0]
            if boDebug:
                print "start_date", type(start_date), start_date

            # the end date is the date of the last trade
            end_date = dfData.index[-1]
            if boDebug:
                print "end_date", type(end_date), end_date

            # The auto_close date is the day after the last trade.
            ac_date = end_date + pd.Timedelta(days=1)
            if boDebug:
                print "ac_date", type(ac_date), ac_date

            # Update our meta data
            dfMetadata.iloc[iSid] = start_date, end_date, ac_date, S
            new_index = ['open', 'high', 'low', 'close', 'volume']
            dfData.reindex(new_index, copy=False)
            # FIX IT
            sessions = calendar.sessions_in_range(start_date, end_date)
            print sessions.tz_localize(None)
            dfData = dfData.reindex(
                sessions.tz_localize(None),
                copy=False,
            ).fillna(0.0)

            liData.append((iSid, dfData))
            iSid += 1

        if boDebug:
            print "liData", type(liData), "length", len(liData)
            print "Now calling daily_bar_writer"

        daily_bar_writer.write(liData, show_progress=False)

        # Hardcode the exchange to "YAHOO" for all assets and (elsewhere)
        # register "YAHOO" to resolve to the NYSE calendar, because these are
        # all equities and thus can use the NYSE calendar.
        dfMetadata['exchange'] = "YAHOO"

        if boDebug:
            print "returned from daily_bar_writer"
            print "calling asset_db_writer"
            print "dfMetadata", type(dfMetadata)
            print

        # Not sure why symbol_map is needed
        symbol_map = pd.Series(dfMetadata.symbol.index, dfMetadata.symbol)
        if boDebug:
            print "symbol_map", type(symbol_map)
            print symbol_map

        asset_db_writer.write(equities=dfMetadata)

        if boDebug:
            print "returned from asset_db_writer"
            print "calling adjustment_writer"

        adjustment_writer.write()
        if boDebug:
            print "returned from adjustment_writer"
            print "now leaving ingest function"
        conn.close()
        return

    if boDebug:
        print "about to return ingest function"
    return ingest