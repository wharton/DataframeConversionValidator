"""

Provides a way to quickly validate column conversions didn't result in null across large dataframes.
Can also be used to find problem columns and rows.

Will need to specify or create a primary key column for matching before and after rows.

Use case: Converting StringType columns to TimestampType can cause null values in poorly formed data. It's not possible
to check by hand. This works as an early warning system.

Example:

$ dfcv = DataframeConversionValidator(_before_df=unmodified_df, _after_df=converted_df, _primary_key_column='pk')

---------------
Original Shape:
    rows    - 469221
    columns - 582
Problem Shape:
    rows    - 1
    columns - 3
Details:
    ['ImproperDate (1)', 'ImproperTimestamp (1)', 'BadUpdateTime (1)']
---------------

"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from typing import Dict, List
from collections import namedtuple
from operator import add

ColumnDifference = namedtuple('ColumnDifference', ['column_name', 'difference'])


def count_nulls(df: DataFrame) -> DataFrame:
    """
    Count null values in columns

    :param df: Dataframe to count
    :return: Dataframe of same schema with one row, each column has the count of nulls in all rows.
    """
    return df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])


class DataframeConversionValidator:
    """Class for comparing transformed dataframes to their original form.

    A class for comparing two dataframes. One before a transformation takes place and one after. This provides helper
    methods for finding problems when transforming Spark dataframes. Makes it quicker to discover poorly formed data,
    specification errors and datetime mismatches.

    Attributes:
        before_df : Dataframe
            Dataframe before conversion.
        after_df : Dataframe
            Dataframe after conversion.
        nulls_before_df : Dict[str, int]
            Dictionary of column names and sum of nulls in each column for `before_df`.
        nulls_after_df : Dict[str, int]
            Dictionary of column names and sum of nulls in each column for `after_df`.
        differing_columns : List[ColumnDifference]
            List of Tuple[column_name, difference]. Difference is the subtraction of `after_df` by 'before_df`.
        column_names : List[str]
            List of only column names from `differing_columns`.
        primary_key_column : str
            Column name for existing primary key column. TODO: allow multiple columns
        bad_row_column_comparison : Dataframe bad row counts for each dataframe joined on primary key column.

    """
    before_df: DataFrame
    after_df: DataFrame
    nulls_before_df: Dict[str, int]
    nulls_after_df: Dict[str, int]
    differing_columns: List[ColumnDifference]
    column_names: List[str]
    primary_key_column: str
    bad_row_column_comparison: DataFrame

    def __init__(self, _before_df: DataFrame, _after_df: DataFrame, _primary_key_column: str, quiet: bool = False):
        """
        :param _before_df: Dataframe before conversion, includes a PK column for matching
        :param _after_df: Dataframe after conversion, includes a PK column for matching
        :param _primary_key_column: Columnn name used to compare matching rows between dataframes
        :param quiet: True suppresses the summary information on creation.

        To add a PK column before conversion do this::

            df = df.withColumn('pk', F.monotonically_increasing_id())

        """
        self.column_names = list(_before_df.columns)

        if _primary_key_column not in self.column_names:
            raise LookupError("%s not found in '_before_df'" % _primary_key_column)

        self.before_df = _before_df
        self.after_df = _after_df
        self.nulls_before_df = count_nulls(_before_df).collect()[0].asDict()
        self.nulls_after_df = count_nulls(_after_df).collect()[0].asDict()
        self.differing_columns = list([ColumnDifference(column_name=colname, difference=self.nulls_after_df[colname] - self.nulls_before_df[colname]) for colname in self.column_names if self.nulls_after_df[colname] - self.nulls_before_df[colname] != 0])
        self.primary_key_column = _primary_key_column

        select_left = list(map(lambda x: "left." + x, self.different_row_columns()))
        select_right = list(map(lambda x: "right." + x, self.different_row_columns()))
        merged = self.before_df.alias("left").join(self.after_df.alias('right'), on=self.primary_key_column, how='inner')
        merged = merged.withColumn('leftNulls',
                                   reduce(add, [F.col(colname).isNull().cast('int') for colname in select_left]))
        merged = merged.withColumn('rightNulls',
                                   reduce(add, [F.col(colname).isNull().cast('int') for colname in select_right]))

        self.bad_row_column_comparison = merged.where(F.col('leftNulls') != F.col('rightNulls'))\
            .select([self.primary_key_column] +
                    select_left +
                    select_right +
                    ['leftNulls', 'rightNulls'])
        if not quiet:
            self.summary()

    def summary(self) -> None:
        """
        Prints a summary of original dataframe shape, problem shape, problem columns and the difference in null counts per.
        :return: None
        TODO: allow redirection.
        """
        column_summary = repr([f"""{column} ({difference})""" for column, difference in self.differing_columns])
        print(f"""---------------
Original Shape:
    rows    - {self.before_df.count()}
    columns - {len(self.before_df.columns)}
Problem Shape:
    rows    - {self.bad_row_count()}
    columns - {self.bad_column_count()}
Details:
    {column_summary}
---------------""")

    def different_row_columns(self) -> List[str]:
        """
        :return: List of column names where null counts do not match between dataframes.
        """
        return list(map(lambda x: x.column_name, self.differing_columns))

    def bad_row_count(self) -> int:
        """
        :return: Count of rows where null counts do not match between dataframes.
        """
        return self.bad_row_column_comparison.count()

    def bad_column_count(self) -> int:
        """
        :return: Count of columns where null counts do not match between dataframes.
        """
        return len(self.different_row_columns())

    def original_problem_rows(self, full_row=False) -> DataFrame:
        """
        :param full_row: True if you want to see all the original columns. Defaults to just problem columns.
        :return: Dataframe of rows from original dataframe where counts do not match between dataframes.
        """
        return self._get_dataframe_by_pk(df=self.before_df, pks=self._get_pks_of_bad_rows(), full_row=full_row)

    def converted_problem_rows(self, full_row=False) -> DataFrame:
        """
        :param full_row: True if you want to see all the after columns. Defaults to just problem columns.
        :return: Dataframe of rows from after dataframe where counts do not match between dataframes.
        """
        return self._get_dataframe_by_pk(df=self.after_df, pks=self._get_pks_of_bad_rows(), full_row=full_row)

    def _get_dataframe_by_pk(self, df: DataFrame, pks: List, full_row=False) -> DataFrame:
        """
        :param df: Dataframe to select by primary key column
        :param pks: List of primary key values
        :param full_row: True if you want to see all columns from specified dataframe. Defaults to just problem columns.
        :return: Dataframe of rows matching primary key values.
        """
        if full_row:
            return df.where(F.col(self.primary_key_column).isin(pks))
        else:
            return df.where(F.col(self.primary_key_column).isin(pks)).select([self.primary_key_column] + self.different_row_columns())

    def _get_pks_of_bad_rows(self) -> List:
        """
        INTERNAL
        :return: List of primary key values for problem rows.
        """
        return [row[self.primary_key_column] for row in self.bad_row_column_comparison.select(self.primary_key_column).collect()]


__all__ = ["DataframeConversionValidator", "count_nulls"]
