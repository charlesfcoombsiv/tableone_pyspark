import sys


def tableone_pyspark(df, col_to_strat="", cols_to_analyze_list=[], beautify=False, p_values=False):
    """
    tableone
    tableone creates the “Table 1” summary statistics for a patient population.
    It was modeled after the R package of the same name by Yoshida and Bohn.
    
    Parameters:
            df: (dataframe) The dataframe to analyze.
            
            col_to_strat: (string) (not required) Categorical variable to stratify analysis by.

            cols_to_analyze_list: (list of strings) Names of the columns to analyze.
            
            beautify: (string: 'Yes', ' No') Make the table presentation ready by:
                      Removing "Pivoted_column" and "Category_type"
                      Removing redundant entries in "Characteristics"
                      Replacing "_" in "Characteristics" with " ".
            
            p_values: (string: 'Yes', ' No') Return p values and test statistics for stratified analysis
                      For continuous variables with 2 distinct stratification values, t-test returns t statistics.
                      For continuous variables with >2 distinct stratification values, ANOVA returns F-value.
                      For categorical variables with >5 values, Chi-Squared returns Chi-square.
    
    Returns:
            final_df: (dataframe) Summary dataframe of the p values and test statistics for all the columns analyzed.
            
    Author: Charles Coombs - 2019/08/15
    
    Update History:
            2019-11-12 - Charles
                Broadcast initial_df to speed up analysis.
                Imported function from SQL library individually so don't need "F." notation.
                Repartitioned to 1 after any joins or aggregates (".agg()").

            2019-11-14 - Charles
                Changed Yes/No for p_values and beautify to True/False


    """
    # BROADCAST SHOULD BE FOR READ ONLY DATAFRAMES

    ##################################################################################
    # Override p_value indicator if no stratification variable so doesnt mess up run.
    ##################################################################################

    if col_to_strat == "" and p_values is True:
        p_values = False
        print("p_values indicator overridden to False because no stratification variable")

    #######################################################################
    # Calculate general statistics (regardless to cols_to_analyze_list)
    #######################################################################

    # Start output row order counter
    idx = 0

    output_column_names = ["All_Patients"]

    column_order = ["Index", "Characteristics", "Variable_type", "Values"]

    if col_to_strat == "":
        # If there is NOT a stratification variable

        # Find row count by selecting first variable to analyze and broadcasting
        count_all = broadcast(df.select(cols_to_analyze_list[0])).count()

        df_all = spark.createDataFrame(pd.DataFrame({"Characteristics": "Total", "Values": "ALL", "Variable_type": "",
                                                     "All_Patients": count_all, "All_Patients_%": 1,
                                                     "Index": idx}, index=[0]))

        column_order += ["All_Patients", "All_Patients_%"]

    else:
        # If there is a stratification variable

        # Change col_to_strat values to be valid as column's name.
        strat_df = broadcast(df.select(col_to_strat)
                             .fillna("MISSING", subset=[col_to_strat])
                             .withColumn(col_to_strat, regexp_replace(col_to_strat, ' ', '_'))
                             .withColumn(col_to_strat, regexp_replace(col_to_strat, r'[^\x00-\x7F]+','?')))

        # Get count for each stratification value
        df_all = strat_df.groupBy().pivot(col_to_strat).count()
        
        # Prepare column names with "_%"
        output_column_names.extend(df_all.columns)

        output_columns_with_percent_names = []

        for c in output_column_names:
            output_columns_with_percent_names += [c, c + "_%"]

        column_order.extend(output_columns_with_percent_names)

        # Create dataframe of individual category counts, total count, and percents.
        # "add" function here was imported from operations so SQL "add" function doesnt override it
        df_all = df_all.withColumn("Characteristics", lit("Total"))\
                       .withColumn("All_Patients",reduce(add, [col(x) for x in output_column_names if x != "All_Patients"]))\
                       .withColumn("Values", lit("ALL"))\
                       .withColumn("Variable_type", lit(None))\
                       .withColumn("Index", lit(idx))
            
        # Add 1 as 100% for each category percent
        for c in output_column_names:
            df_all = df_all.withColumn(c + "_%", lit(1))
            
        # Change column order and add columns if finding p values
        if p_values is True:
            column_order += ["p_value", "test_value", "test_name"]

            df_all = df_all.withColumn("p_value", lit(None))\
                           .withColumn("test_name", lit(None))\
                           .withColumn("test_value",lit(None))

    # Add to summary list
    dfs_to_union = [df_all.select(column_order)]

    # Iterate index +1
    idx += 1

    #######################################################################
    # Calculate statistics for cols_to_analyze_list
    #######################################################################

    # Find the number of patients for "All Patients" and in each strat value (used to derive percents later)
    counts_dict = df_all.select(output_column_names).toPandas().to_dict(orient='list')

    # calculate statistics for each cols_to_analyze_list 
    for col_i in cols_to_analyze_list:

        # Figure out column type
        col_type = df.select(col_i).dtypes[0][1]

        # Prepare dataframe to be analyzed
        if col_to_strat == '':
            initial_df = broadcast(df.select(col_i))
        else:
            initial_df = broadcast(df.select(col_i, col_to_strat))

        # conduct categorical analysis
        if col_type == 'string':

            df_stat = analysis_categorical(col_i, initial_df, col_to_strat, idx)

            # Find percent for each cols_to_analyze_list
            for cat_col in output_column_names:
                df_stat = df_stat.withColumn(cat_col + "_%", col(cat_col)/counts_dict[cat_col][0])

            # calculate the p value
            if p_values is True:
                df_p_values = p_values_categorical(col_i, initial_df, col_to_strat)

                # Add 2 columns for a cleaner join
                df_p_values = df_p_values.withColumn("Index", lit(idx + 0.01))\
                                         .withColumn("Characteristics",lit(col_i))

                # Something is wrong with Darwin so need to convert back and forth
                df_p_values = spark.createDataFrame(df_p_values.toPandas())

                df_stat = df_stat.join(df_p_values, ["Index", "Characteristics"], "left_outer").repartition(1)

        # conduct continuous analysis
        elif col_type in ['int', 'double', 'float', 'short', 'long']:

            df_stat = analysis_continuous(col_i, initial_df, col_to_strat, idx)

            # calculate the p value
            if p_values is True:
                df_p_values = p_values_continous(col_i, initial_df, col_to_strat)

                # Add 2 columns for a cleaner join
                df_p_values = df_p_values.withColumn("Index", lit(idx + 0.1))\
                                         .withColumn("Characteristics", lit(col_i))

                # Something is wrong with Darwin so need to convert back and forth
                df_p_values = spark.createDataFrame(df_p_values.toPandas())

                df_stat = df_stat.join(df_p_values, ["Index", "Characteristics"], "left_outer").repartition(1)
            
            # Add Null so can be stacked with the categorical analysis
            for cat_col in output_column_names:
                df_stat = df_stat.withColumn(cat_col + "_%", lit(None))
        else:
            print("Warning: Not supported column's type {}:{}".format(col_i, c_type))
            continue

        # Counter +1
        idx = idx+1

        # Add to summary list
        dfs_to_union.append(df_stat.select(column_order))

    # Create one dataframe from list of dataframes
    final_df = reduce(lambda x, y: x.union(y), dfs_to_union)

    # If no variable to pivot, then pivoted column will be an empty string
    final_df = final_df.withColumn("Pivoted_column", lit(col_to_strat))\
                       .select(["Pivoted_column"] + column_order)

    #####################
    # Beautify
    #####################
    # Make the table presentation ready by:
    # 1. Remove "Pivoted_column" and "Variable_type".
    # 2. Remove redundant entries in "Characteristics".
    # 3. Replace "_" in "Characteristics" with " "
    # . Multiply percent column by 100 <-- maybe add this in

    if beautify is True:
        final_df = final_df.drop("Pivoted_column", "Variable_type")

        # Blank out headings on non heading rows of "Characteristics" and Replace "_" in "Characteristics" with " "
        window = W.partitionBy("Characteristics").orderBy("Index", "Values")

        final_df = final_df.withColumn("rank", row_number().over(window))

        final_df = final_df.withColumn("Characteristics", when(final_df.rank == 1,
                                                               regexp_replace(final_df.Characteristics, "_", " "))
                                                          .otherwise(lit(None)))

        final_df = final_df.drop("rank")
    
    final_df = final_df.repartition(1)

    return final_df
         

#################################################
# Global imports and functions #
#################################################
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, when, row_number, count, min, max, avg, stddev, lower, regexp_replace, broadcast, expr
from pyspark.sql.window import Window as W

from operator import add
from functools import reduce

import pandas as pd
import numpy as np

from scipy import stats


def analysis_categorical(col_i, initial_df, col_to_strat, idx):
    """
    Calculate count by categorical col_i by col_to_strat.
    """

    # if exist null values in col_i change it to string "MISSING"
    initial_groups = initial_df.fillna("MISSING", subset=[col_i]).groupBy(col_i)

    # count patients by the col_i (regardless to the pivoted column)
    df_cat_stat = initial_groups.count().withColumnRenamed("count", "All_Patients")
    df_cat_stat = df_cat_stat.withColumnRenamed(col_i, "Values")

    if col_to_strat != "":
        # If there is a stratification variable

        # get the count of each sub-category per each sub category of col_to_strat
        df_pivot_stat = initial_groups.pivot(col_to_strat).count().fillna(0)
        df_pivot_stat = df_pivot_stat.withColumnRenamed(col_i, "Values")

        # join all to get final result
        df_cat_stat = df_cat_stat.join(df_pivot_stat, "Values", "outer").repartition(1)

    df_cat_stat = df_cat_stat.withColumn("Variable_type", lit("category"))\
                             .withColumn("Characteristics", lit(col_i))

    # Add counter based on alphabetic order of col_i,
    # EXCEPT "Yes" goes before "No"
    # "Missing" or "Unknown" or "Other" goes last

    window_index = W.partitionBy("Characteristics").orderBy("order","Values")

    df_cat_stat = df_cat_stat.withColumn("order", when(col("Values") == "Yes", lit(1))
                                                 .when(col("Values") == "No", lit(2))
                                                 .when((lower(col("Values")).rlike("missing|unknown|other")), 5)
                                                 .otherwise(lit(3)))

    df_cat_stat = df_cat_stat.withColumn("Index", idx + row_number().over(window_index) * 0.01)\
                             .drop("order")\
                             .repartition(1)

    return df_cat_stat


def analysis_continuous(col_i,initial_df,col_to_strat,idx):
    """
    Calculate for continous col_i by col_to_strat: 
    [n, mean, std, min, max, q25, q50, q75].

    Create idx here so that can create proper order for each stat.
    """
    
    initial_groups = initial_df.groupBy()

    if col_to_strat != "":
        initial_pivoted = initial_df.groupBy().pivot(col_to_strat)

    ###########################
    # calculate n
    ###########################

    df_n = initial_groups.agg(count(col_i).alias('All_Patients')).withColumn("Values", lit('n'))

    if col_to_strat != "":
        # per pivoted column values 
        df_n = df_n.join(initial_pivoted.agg({col_i:'count'}).withColumn("Values", lit('n')), "Values", "inner")
                          
    df_n = df_n.withColumn("Index", lit(idx + 0.1)).repartition(1)

    ###########################
    # calculate min
    ###########################

    df_min = initial_groups.agg(min(col_i).alias('All_Patients')).withColumn("Values", lit('min'))

    if col_to_strat != "":
        # per pivoted column values 
        df_min = df_min.join(initial_pivoted.agg({col_i:'min'}).withColumn("Values", lit('min')), "Values", "inner")

    df_min = df_min.withColumn("Index", lit(idx + 0.2)).repartition(1)

    ###########################
    # calculate max
    ###########################
    df_max = initial_groups.agg(max(col_i).alias('All_Patients')).withColumn("Values", lit('max'))

    if col_to_strat != "":
        # per pivoted column values 
        df_max = df_max.join(initial_pivoted.agg({col_i:'max'}).withColumn("Values", lit('max')), "Values", "inner")

    df_max = df_max.withColumn("Index", lit(idx + 0.3)).repartition(1)

    ###########################
    # calculate mean
    ###########################
    df_mean = initial_groups.agg(avg(col_i).alias('All_Patients')).withColumn("Values", lit('mean'))

    if col_to_strat != "":
        # per pivoted column values 
        df_mean = df_mean.join(initial_pivoted.agg({col_i:'avg'}).withColumn("Values", lit('mean')), "Values", "inner")

    df_mean = df_mean.withColumn("Index", lit(idx + 0.4)).repartition(1)

    ###########################
    # calculate std
    ###########################
    df_std = initial_groups.agg(stddev(col_i).alias('All_Patients')).withColumn("Values", lit('stddev'))

    if col_to_strat != "":
        # per pivoted column values 
        df_std = df_std.join(initial_pivoted.agg({col_i:'stddev'}).withColumn("Values", lit('stddev')), "Values", "inner")

    df_std = df_std.withColumn("Index", lit(idx + 0.5)).repartition(1)

    ###################################################
    # stack n, mean, std, min, and max statistics
    ###################################################
    out_df = df_n.union(df_mean).union(df_min).union(df_max).union(df_std)

    # Find number of records and add 1 so percentile_aprox finds exact values
    # Default accuracy is 10,000 so only change value if count > 10,000
    ct_plus_one = initial_df.count() + 1
    if ct_plus_one < 10000:
        ct_plus_one = 10000

    ###########################
    # calculating 25th percentile
    ###########################
    df_25quar = initial_groups.agg(expr('percentile_approx({}, 0.25,  {})'.format(col_i,ct_plus_one)).alias('All_Patients')).withColumn("Values", lit('25th percentile'))

    if col_to_strat != "":
        # per pivoted column values 
        df_25quar = df_25quar.join(initial_pivoted.agg(F.expr('percentile_approx({}, 0.25,  {})'.format(col_i,ct_plus_one))).withColumn("Values", lit('25th percentile')), "Values","inner")
    
    df_25quar = df_25quar.withColumn("Index",lit(idx + 0.6)).repartition(1)

    ###########################
    # calculating 50th percentile
    ###########################
    df_50quar = initial_groups.agg(expr('percentile_approx({}, 0.50,  {})'.format(col_i,ct_plus_one)).alias('All_Patients')).withColumn("Values", it('50th percentile'))

    if col_to_strat != "":
        # per pivoted column values 
        df_50quar = df_50quar.join(initial_pivoted.agg(expr('percentile_approx({}, 0.50,  {})'.format(col_i,ct_plus_one))).withColumn("Values", lit('50th percentile')), "Values","inner")
    
    df_50quar = df_50quar.withColumn("Index",lit(idx + 0.7)).repartition(1)

    ###########################
    # calculating 75th percentile
    ###########################
    # per pivoted column values
    df_75quar = initial_groups.agg(expr('percentile_approx({}, 0.75,  {})'.format(col_i,ct_plus_one)).alias('All_Patients')).withColumn("Values", lit('75th percentile'))

    if col_to_strat != "":
        # per pivoted column values 
        df_75quar = df_75quar.join(initial_pivoted.agg(expr('percentile_approx({}, 0.75,  {})'.format(col_i,ct_plus_one))).withColumn("Values", lit('75th percentile')), "Values","inner")
    
    df_75quar = df_75quar.withColumn("Index",lit(idx + 0.8)).repartition(1)

    # stack all statistics
    df = out_df.union(df_25quar).union(df_50quar).union(df_75quar)
    
    df = df.withColumn("Characteristics", lit(col_i))\
           .withColumn("Variable_type", lit("continous"))

    return df


def p_values_continous(col_i,initial_df,col_to_strat):
    """
        Find the p value for the continous variable.
        If the stratified column has 2 distinct values than use t-test,
        >=2 use ANOVA, otherwise print message and return nothing.
    """

    # Convert to Pandas dataframe but only select relevant columns
    df_pan = initial_df.select(col_to_strat, col_i).toPandas()

    # If distinct count of col_to_strat is 2 than use t-test else use ANOVA
    unique_list = df_pan[col_to_strat].unique().tolist()
    # print unique_list
    unique_ct = len(unique_list)

    if unique_ct == 2:
        # Perform t test
        a = df_pan.loc[df_pan[col_to_strat] == unique_list[0]][col_i]
        b = df_pan.loc[df_pan[col_to_strat] == unique_list[1]][col_i]
        t, p_value = stats.ttest_ind(a,b)

        df = pd.DataFrame({"test_name":"t-test","p_value":p_value,"test_value":t},index=[0])

    elif unique_ct > 2:
        # Perform ANOVA
        sample_list = [(df_pan.loc[df_pan[col_to_strat] == value][col_i]) for value in unique_list]
        
        F_value, p_value = stats.f_oneway(*sample_list)
        
        df = pd.DataFrame({"test_name":"ANOVA","p_value":p_value,"test_value":F_value},index=[0])

    else:
        print("Notice: <2 distinct values for {}. p value not returned".format(col_to_strat))
        df = pd.DataFrame({"test_name":"NOT DONE","p_value":np.nan,"test_value":np.nan},index=[0])

    df_spark = spark.createDataFrame(df)

    return df_spark


def p_values_categorical(col_i,initial_df,col_to_strat):
    """
        Find the p value for the categorical variable.
        If the stratified column has 2 distinct values than use t-test,
        >=2 use ANOVA, otherwise print message and return nothing.
    """

    # Convert to Pandas dataframe but only select relevant columns
    df_pan = initial_df.select(col_to_strat, col_i).toPandas()

    # If count of col_i is <5 than dont run test
    col_i_ct = len(df_pan[col_i].dropna())

    if col_i_ct >=5:
        # Make the cross tab
        crtb = pd.crosstab(df_pan[col_i],df_pan[col_to_strat])

        # Do the chi-square on the cross tab
        chi2, p_value, dof, expected = stats.chi2_contingency(crtb)

        df = pd.DataFrame({"test_name":"Chi-Square","p_value":p_value,"test_value":chi2},index=[0])

    else:
        print("Notice: <5 values for {}. p value not returned".format(col_i))
        df = pd.DataFrame({"test_name":"NOT DONE","p_value":np.nan,"test_value":np.nan},index=[0])

    df_spark = spark.createDataFrame(df)

    return df_spark


# Allow function file to be run as a script
if __name__ == "__main__":
    tableone_pyspark(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])