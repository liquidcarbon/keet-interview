import logging
import numpy as np
import os
import pandas as pd
import sqlite3
import sys
from pathlib import Path

# setup logging
_l = logging.getLogger('Keet')
logging.basicConfig(
    format='%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s %(message)s',
    datefmt='%y%m%d@%H:%M:%S',
    stream=sys.stdout,
)
_l.setLevel(logging.INFO)
_l.info('Logging is enabled')


class Keet:
    """Driver class for this task"""
    def __init__(self, path, file):
        try:
            assert (Path(path) / file).is_file() is True
        except AssertionError:
            _l.fatal('data file not found')
            sys.exit(0)
        self.data = pd.read_csv(file)
        _l.info('read %s rows x %s columns' % self.data.shape)

    @staticmethod
    def interpolate(s: pd.Series, days: int, poly_n: int) -> pd.Series:
        """Extrapolates date series using polynomial fit"""

        # ensure that datetime index is sorted
        s.sort_index(inplace=True)
        x = (s.index - s.index[0]).days
        y = s.values

        # polynomial fit
        z = np.polyfit(x, y, poly_n)
        f = np.poly1d(z)

        # apply prediction
        for i in range(1, days + 1):
            v = f(x[-1] + i)
            s[s.index[-1] + pd.DateOffset(i)] = int(round(v))
        return s

    def prep_data(self):
        """Prepares data for loading into SQL database"""

        dau = self.data.groupby('visit_date').id.nunique()
        try:
            dau.index = pd.DatetimeIndex(dau.index)
            observed_dates = list(dau.index)
            dau = Keet.interpolate(dau, 1, 3)
        except ValueError as e:
            _l.error(f'date format error: {e}')
        self.data_prepared = pd.DataFrame({
            'year': dau.index.year,
            'month': dau.index.month,
            'day': dau.index.day,
            'observed': dau.index.isin(observed_dates),
            'count': dau.values,
        })
        _l.info('data processing complete')
        return

    def init_db(self, url):
        """Connect to database"""

        self.conn = sqlite3.connect(url)
        _l.info(f'database created: {url}')

    @staticmethod
    def make_table(conn):
        """Executes CREATE TABLE DDL."""

        q = '''
        CREATE TABLE IF NOT EXISTS daily_user_counts (
            year integer NOT NULL,
            month integer NOT NULL,
            day integer NOT NULL,
            observed boolean NOT NULL,
            count integer NOT NULL
        )
        '''
        conn.execute(q)
        _l.info('table daily_user_counts created')
        return

    def load_to_db(self):
        """Loads to SQL"""
        try:
            self.data_prepared.to_sql(
                'daily_user_counts',
                self.conn,
                if_exists='replace',
                index=False,
            )
            _l.info('data loaded to SQLite \o/ ')
        except Exception as e:
            _l.error(f':( {e}')
        return


if __name__ == '__main__':
    k = Keet(path='.', file='Generated_Data_modified.csv')
    k.prep_data()
    k.init_db('users.sqlite')
    k.make_table(k.conn)
    k.load_to_db()
