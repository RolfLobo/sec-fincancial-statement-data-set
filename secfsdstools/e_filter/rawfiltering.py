"""
This module contains some basic pathfilter implementations on the RawDataBag.

Note: the filters don't create new copies of the pandas dataset
"""
from typing import List

from secfsdstools.a_utils.basic import calculate_previous_period
from secfsdstools.d_container.databagmodel import RawDataBag
from secfsdstools.d_container.filter import FilterBase


class AdshRawFilter(FilterBase[RawDataBag]):
    """
    Filters the data by a list of adshs. This filter operates on the sub, pre_df and the num_df.
    """

    def __init__(self, adshs: List[str]):
        self.adshs = adshs

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that only datapoints of reports defined by the adshs list
        are contained.

        Args:
            databag(RawDataBag) : rawdatabag to apply the pathfilter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        sub_filtered_for_adshs = databag.sub_df[databag.sub_df.adsh.isin(self.adshs)]
        pre_filtered_for_adshs = databag.pre_df[databag.pre_df.adsh.isin(self.adshs)]
        num_filtered_for_adshs = databag.num_df[databag.num_df.adsh.isin(self.adshs)]

        return RawDataBag.create(sub_df=sub_filtered_for_adshs,
                                 pre_df=pre_filtered_for_adshs,
                                 num_df=num_filtered_for_adshs)


class StmtRawFilter(FilterBase[RawDataBag]):
    """
    Filters the data by a list of statement type (BS, IS, CF, ...).
    This filter operates on the pre_df.
    """

    def __init__(self, stmts: List[str]):
        self.stmts = stmts

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that only datapoints of reports defined by the adshs list
        are contained.

        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        pre_filtered_for_stmts = databag.pre_df[databag.pre_df.stmt.isin(self.stmts)]

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=pre_filtered_for_stmts,
                                 num_df=databag.num_df)


class ReportPeriodRawFilter(FilterBase[RawDataBag]):
    """
    Filters the data so that only datapoints are contained which ddate-attribute equals the
    period date of the report. Therefore, the filter operates on the num_df dataframe.
    """

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filter the databag so that only datapoints are contained which have a ddate-attribute
        that equals the period-attribute of the report.
        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """

        adsh_period_map = \
            databag.sub_df[['adsh', 'period']].set_index('adsh').to_dict()['period']

        mask = databag.num_df['adsh'].map(adsh_period_map) == databag.num_df['ddate']
        num_filtered_for_ddates = databag.num_df[mask]

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=databag.pre_df,
                                 num_df=num_filtered_for_ddates)


class ReportPeriodAndPreviousPeriodRawFilter(FilterBase[RawDataBag]):
    """
    Filters the data so that only datapoints are contained which ddate-attribute equals the
    period date of the report or the period date of the previous (a year ago) report.
    Therefore, the filter operates on the num_df dataframe.
    """

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filter the databag so that only datapoints are contained which have a ddate-attribute
        that equals the period-attribute of the report or the period of the previous (a year ago)
        report.
        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """

        adsh_period_map = \
            databag.sub_df[['adsh', 'period']].set_index('adsh').to_dict()['period']

        # caculate the dates for the previous year
        adsh_previous_period_map = {adsh: calculate_previous_period(period)
                                    for adsh, period in adsh_period_map.items()}

        mask = (databag.num_df['adsh'].map(adsh_period_map) == databag.num_df['ddate']) | \
               (databag.num_df['adsh'].map(adsh_previous_period_map) == databag.num_df['ddate'])

        num_filtered_for_ddates = databag.num_df[mask]

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=databag.pre_df,
                                 num_df=num_filtered_for_ddates)


class TagRawFilter(FilterBase[RawDataBag]):
    """
    Filters the data by a list of tags. This filter operates on the pre_df and the num_df.
    """

    def __init__(self, tags: List[str]):
        self.tags = tags

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that only datapoints are contained which have a tag-attribute
        that is in the provided list.
        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        pre_filtered_for_tags = databag.pre_df[databag.pre_df.tag.isin(self.tags)]
        num_filtered_for_tags = databag.num_df[databag.num_df.tag.isin(self.tags)]

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=pre_filtered_for_tags,
                                 num_df=num_filtered_for_tags)


class MainCoregRawFilter(FilterBase[RawDataBag]):
    """
    Filters only for the main coreg entries (coreg == '')
    """

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that only the main coreg entries are contained
        (no data subsidiaries).
        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        num_filtered_for_main_coreg = databag.num_df[databag.num_df.coreg == '']

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=databag.pre_df,
                                 num_df=num_filtered_for_main_coreg)


class OfficialTagsOnlyRawFilter(FilterBase[RawDataBag]):
    """
    Filters only the official tags. These are the tags that contain an official XBRL version
    within the version column. "inofficial" (resp. company specific) tags are identified with
    the version column containing the value of the adsh.
    """

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that official tags are contained.

        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        # using isin is performant, so we just make sure to filter the rows
        # which do not have an adsh as version
        pre_filtered_for_tags = databag.pre_df[~databag.pre_df.version.isin(databag.sub_df.adsh)]
        num_filtered_for_tags = databag.num_df[~databag.num_df.version.isin(databag.sub_df.adsh)]

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=pre_filtered_for_tags,
                                 num_df=num_filtered_for_tags)


class USDOnlyRawFilter(FilterBase[RawDataBag]):
    """
    Removes all entries which have a currency in the column uom that is not USD.
    """

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        Removes all currency entries in the uom colum of the num_df that are not USD.

        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data

        """

        # currency is always in uppercase, so if it is not all uppercase, it is not a currency
        mask_has_lower = ~databag.num_df.uom.str.isupper()

        # currency is always 3 letters
        mask_is_none_currency = databag.num_df.uom.str.len() != 3

        # keep USD
        mask_usd_only = databag.num_df.uom == "USD"

        num_filtered_for_usd = \
            databag.num_df[mask_has_lower | mask_is_none_currency | mask_usd_only]

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=databag.pre_df,
                                 num_df=num_filtered_for_usd)


class NoSegmentInfoRawFilter(FilterBase[RawDataBag]):
    """
    Filters only for the entries in num.txt that don't have a value in the segments column.)
    """

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that only entries are contained that don't have segments info.

        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        num_filtered_for_main_coreg = databag.num_df[databag.num_df.segments == '']

        return RawDataBag.create(sub_df=databag.sub_df,
                                 pre_df=databag.pre_df,
                                 num_df=num_filtered_for_main_coreg)


class CIKRawFilter(FilterBase[RawDataBag]):
    """
    Filters the data by a list of ciks. This filter operates on the sub, pre_df and the num_df.
    """

    def __init__(self, ciks: List[int]):
        self.ciks = ciks

    def filter(self, databag: RawDataBag) -> RawDataBag:
        """
        filters the databag so that only datapoints belonging to the provided ciks
        are contained.

        Args:
            databag(RawDataBag) : rawdatabag to apply the filter to

        Returns:
            RawDataBag: the databag with the filtered data
        """
        sub_filtered_for_adshs = databag.sub_df[databag.sub_df.cik.isin(self.ciks)]
        adshs = sub_filtered_for_adshs.adsh.tolist()

        pre_filtered_for_adshs = databag.pre_df[databag.pre_df.adsh.isin(adshs)]
        num_filtered_for_adshs = databag.num_df[databag.num_df.adsh.isin(adshs)]

        return RawDataBag.create(sub_df=sub_filtered_for_adshs,
                                 pre_df=pre_filtered_for_adshs,
                                 num_df=num_filtered_for_adshs)
