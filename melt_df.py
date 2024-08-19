#!/usr/bin/env python3

###import pandas as pd No LooseVersion in python 3.12

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()
#    #.config("spark.some.config.option", "some-value") \


import pyspark.sql.functions as F


def melt_df(df, id_vars, value_vars, var_name, value_name):
    """Convert :class:`DataFrame` from wide to long format.
        FOR ONE "COLUMN"

        This takes advantage of advanced data types in the "SQL" table. Specifically
        the Spark feature of having a column whose value is a structure of more than one field.
        (Can Oracle, PostgreSQL or MSQL do this?)
        It makes an array of struct values, from  the named columns. One field is the name of
        the column and the other its value. Then it takes the array and makes a coumn out the 
        values for each name.
    """

    # Create array<struct<variable: str, value: ...>>
    # in Python, not C++ templates, and using dictionaries instead of structs [  {A:A_1, A_val:1}, {A:A_2, A_val:99}, {A:A_3, A_val:47} ]
    _vars_and_vals = F.array(*(
        F.struct( F.lit(c).alias(var_name), F.col(c).alias(value_name) )
        for c in value_vars))

    # Add to the DataFrame and explode. 
    # The columns are now named after the aliases given the field names 
    # in the previous line. So you end up with a  column called A and  another called A_val
    # The array of structs becomes a pair of  columns A and A_val
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals)) 

    cols = id_vars + [
            F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)



def main():
    # Direct to Spark: rows
    column_names = ['id', 'A_1', 'A_2', 'A_3', 'B_1', 'B_2', 'B_3']
    rows = [
        [ 1, 'a',  'b', 'c', 't', 'u', 'v'],
        [ 2, 'b',  'b', None, 'u', 'u', 'u'],
        [ 3, 'a',  None,None, 't', 'u', 't'],
        [ 4, None, None,None, 't', 'u', 't']
    ]
    s_df = spark.createDataFrame(rows,column_names)
    s_df.show()

    # melt
    #                     df, id_vars,  value_vars,            var_name,  value_name
    #                                                          literal,   col
    new_sdf_A = melt_df(s_df,  ['id'],  ['A_1', 'A_2', 'A_3'], 'A',  'A_val')
    new_sdf_B = melt_df(s_df,  ['id'],  ['B_1', 'B_2', 'B_3'], 'B',  'B_val')

    new_sdf_A.show()
    new_sdf_B.show()


    #new_sdf_both = melt_df(s_df, ['id'], 
    #    [ ['A_1', 'A_2', 'A_3'], ['B_1', 'B_2', 'B_3'] ], 
    #    ['Acol', 'Bcol'],  ['A', 'B'])
    #new_sdf_both.show()

if __name__ == '__main__':
    main()
