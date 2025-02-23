"""
Collector Base Class
"""
import os
from abc import ABC
from typing import List, Optional, Tuple, Union

import pandas as pd

from secfsdstools.a_utils.constants import NUM_TXT, PRE_TXT, SUB_TXT
from secfsdstools.d_container.databagmodel import RawDataBag, get_pre_num_filters


class BaseCollector(ABC):
    """
    Base class for Collector implementations
    """

    def __init__(self, datapath: str,
                 stmt_filter: Optional[List[str]] = None,
                 tag_filter: Optional[List[str]] = None):
        self.datapath = datapath
        self.stmt_filter = stmt_filter
        self.tag_filter = tag_filter

    def _read_df_from_raw_parquet(self,
                                  file: str,
                                  filters=None) -> pd.DataFrame:
        try:
            return pd.read_parquet(os.path.join(self.datapath, f'{file}.parquet'),
                                   filters=filters)
        except Exception as ex:
            print("Error reading file:", self.datapath, file, ex)
            raise ex

    def basecollect(self, sub_df_filter: Tuple[str, str, Union[str, List[str]]]) -> RawDataBag:
        """
        basic implementation of the collect method

        Args:
            sub_df_filter: pathfilter that applies directly on the sub.txt dataframe.

        Returns:
            RawDataBag: the loaded instance of RawDataBag

        """

        sub_df = self._read_df_from_raw_parquet(file=SUB_TXT,
                                                filters=[sub_df_filter] if sub_df_filter else None)
        adshs = sub_df.adsh.to_list()
        pre_filter, num_filter = get_pre_num_filters(adshs=adshs,
                                                     stmts=self.stmt_filter,
                                                     tags=self.tag_filter)

        pre_df = self._read_df_from_raw_parquet(
            file=PRE_TXT, filters=pre_filter if pre_filter else None
        )

        num_df = self._read_df_from_raw_parquet(
            file=NUM_TXT, filters=num_filter if num_filter else None
        )

        # pandas pivot works better if coreg and segments are not nan, so we set None values of
        # them to empty strings
        num_df.loc[num_df.coreg.isna(), 'coreg'] = ''
        num_df.loc[num_df.segments.isna(), 'segments'] = ''

        return RawDataBag.create(sub_df=sub_df, pre_df=pre_df, num_df=num_df)

    def collect(self) -> RawDataBag:
        """
        collects the data and returns a Databag. Overwritten by subclasses

        Returns:
            RawDataBag: the collected Data

        """
