#!/usr/bin/env python3


from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()
#    #.config("spark.some.config.option", "some-value") \


import pyspark.sql.functions as F

def melt_array( df, pkey_cols, cols_to_melt, var_name, value_name,
    num_start_idx, remove_nulls=True
):
    """
    Many of the variables in CMS datasets are arrays that have been exploded
    into many columns - e.g. DGNSCD01, DGNSCD02, ..., DGNSCDXX

    This function 'melts' the array into a dataset of the form:

    PRIMARY KEY COLUMNS, DGNSCD_col, DGCNSCD, col_num
                          DGNSCD01 ,  XXXX. ,   01

    The col_num is useful if you want to join two such arrays (e.g. when
    combining diagnosis/procedure information with dates)
    """
    df = df.select(pkey_cols + cols_to_melt)

    df_long = melt_df( df, id_vars=pkey_cols, value_vars=cols_to_melt,
        var_name=var_name, value_name=value_name
    )
    if remove_nulls:
        df_long = df_long.filter(
            (F.col(value_name).isNotNull()) & (F.col(value_name) != "")
        )
    df_long = df_long.withColumn("col_num", F.substring(var_name, num_start_idx, 2))
    return df_long


def melt_df(df, id_vars, value_vars, var_name, value_name):
    """Convert :class:`DataFrame` from wide to long format.
        FOR ONE COLUMN
    """

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(*(
        F.struct( F.lit(c).alias(var_name), F.col(c).alias(value_name) )
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [
            F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)



def main():
    # Direct to Spark: rows
    column_names = ['id', 'A_1', 'A_2', 'A_3', 'B_1', 'B_2', 'B_3', 'C_1', 'C_2']
    rows = [
        [ 1, 'a',  'b', 'c', 't', 'u', 'v', 'h', 'i'],
        [ 2, 'b',  'b', None, 'u', 'u', 'u', 'h', None],
        [ 3, 'a',  None,None, 't', 'u', 't', None, None],
        [ 4, None, None,None, 't', 'u', 't', None, None]
    ]
    s_df = spark.createDataFrame(rows,column_names)
    s_df.show()

    # melt
    #                     df, id_vars,  value_vars,            var_name,  value_name
    #                                                          literal,   col
    new_sdf_A = melt_array(s_df,  ['id'],  ['A_1', 'A_2', 'A_3'], 'A_var',  'A', 3)
    new_sdf_B = melt_array(s_df,  ['id'],  ['B_1', 'B_2', 'B_3'], 'B_var',  'B', 3)
    new_sdf_C = melt_array(s_df,  ['id'],  ['C_1', 'C_2'], 'C_var',  'C', 3)

    new_sdf_A.show()
    new_sdf_B.show()
    new_sdf_C.show()

    join_on = ['id', 'col_num']
    # these are inner joins
    print("PROBLEM for just A and B")
    problem_A = new_sdf_A.join(new_sdf_B, on = join_on)
    problem_A.show()

    print("PROBLEM for A , B and C")
    problem_B = new_sdf_A.join(new_sdf_B, on = join_on).join(new_sdf_C, on = join_on)
    problem_B.show()

    print("SOLUTION for just A and B")
    problem_A = new_sdf_A.join(new_sdf_B, on = join_on, how="outer")
    problem_A.show()
    print("SOLUTION for A , B and C")
    solution = new_sdf_A.join(new_sdf_B, on = join_on, how="outer").join(new_sdf_C, on = join_on, how="outer")
    solution.show()

if __name__ == '__main__':
    main()
