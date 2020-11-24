# DataframeConversionValidator
Module for checking PySpark Dataframes before and after conversion.

Provides a way to quickly validate column conversions didn't result in null across large dataframes.
Can also be used to find problem columns and rows.

Will need to specify or create a primary key column for matching before and after rows.

## Use case
Converting StringType columns to TimestampType can cause null values in poorly formed data. It's not possible
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