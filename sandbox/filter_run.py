from pathlib import Path

from secfsdstools.d_container.databagmodel import JoinedDataBag
from secfsdstools.e_filter.joinedfiltering import StmtJoinedFilter


def filter_all_BS():
    file_path = Path("c:/data/sec/automated/_4_single_bag/all")

    joined = JoinedDataBag.load(str(file_path))
    joined_filtered = joined[StmtJoinedFilter(stmts=['BS'])]
    print(joined.pre_num_df.shape)
    print(joined_filtered.pre_num_df.shape)


def filter_load_all_BS():
    file_path = Path("c:/data/sec/automated/_4_single_bag/all")

    joined = JoinedDataBag.load(target_path=str(file_path), stmt_filter=['BS'])
    print(joined.pre_num_df.shape)


def filter_load_all_10Q():
    file_path = Path("c:/data/sec/automated/_4_single_bag/all")

    joined = JoinedDataBag.load(target_path=str(file_path), forms_filter=['10-Q'])
    print(joined.pre_num_df.shape)


def filter_load_all_BS_10Q():
    file_path = Path("c:/data/sec/automated/_4_single_bag/all")

    joined = JoinedDataBag.load(target_path=str(file_path),
                                stmt_filter=['BS'],
                                forms_filter=['10-Q'])
    print(joined.pre_num_df.shape)


if __name__ == '__main__':
    """
    total :                                          (62_187_005, 17)

    filter_all_BS:           1. 20 GB / 45 seconds / (19_657_047, 17)
    filter_load_all_BS:      1.  7 GB / 15 seconds / (19_657_047, 17)
    filter_load_all_10Q:     1. 15 GB / 35 seconds / (46_603_383, 17)
                             2. 15 GB / 35 seconds / (46_603_383, 17)
    
    filter_load_all_BS_10Q:  1.  5 GB / 13 seconds / (14_486_608, 17) (stmt then form) 
                             2.  5 GB / 13 seconds / (14_486_608, 17) (form then stmt)
    """
    import pyarrow.parquet as pq

    file_path = Path("c:/data/sec/automated/old_4_single_bag/all/sub.txt.parquet")
    parquet_file = pq.ParquetFile(file_path)

    print(parquet_file.schema)
    # import timeit
    #
    # execution_time = timeit.timeit(filter_all_BS, number=1)
    # print(execution_time)
