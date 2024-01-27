"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description: 
"""
import pandas as pd
import ray
import typing
import util.judge_df_equal
import tempfile


def pandas_q2(timediff:int, lineitem:pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])

    cutoff_date = pd.to_datetime("1998-12-01") - pd.DateOffset(days=timediff)
    filtered_data = lineitem[lineitem['l_shipdate'] <= cutoff_date]

    result = filtered_data.groupby(['l_returnflag', 'l_linestatus']).agg(
        sum_qty=pd.NamedAgg(column='l_quantity', aggfunc='sum'),
        sum_base_price=pd.NamedAgg(column='l_extendedprice', aggfunc='sum'),
        sum_disc_price=pd.NamedAgg(column='l_extendedprice', aggfunc=lambda x: (x * (1 - filtered_data['l_discount'])).sum()),
        sum_charge=pd.NamedAgg(column='l_extendedprice', aggfunc=lambda x: (x * (1 - filtered_data['l_discount']) * (1 + filtered_data['l_tax'])).sum()),
        avg_qty=pd.NamedAgg(column='l_quantity', aggfunc='mean'),
        avg_price=pd.NamedAgg(column='l_extendedprice', aggfunc='mean'),
        avg_disc=pd.NamedAgg(column='l_discount', aggfunc='mean'),
        count_order=pd.NamedAgg(column='l_quantity', aggfunc='count')
    ).reset_index()

    result = result.sort_values(by=['l_returnflag', 'l_linestatus'])

    return result
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()

    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    # run the test
    result = pandas_q2(90, lineitem)
    # result.to_csv("correct_results/pandas_q2.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/pandas_q2.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")


