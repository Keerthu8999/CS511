"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-10 21:45:33
 	 FilePath: /codes/pandas_q2.py
 	 Description:
"""
import tempfile

import pandas as pd
import ray
import typing
import util.judge_df_equal


def pandas_q4(time: str, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    lineitem['l_commitdate'] = pd.to_datetime(lineitem['l_commitdate'])
    lineitem['l_receiptdate'] = pd.to_datetime(lineitem['l_receiptdate'])
    
    quarter_start = pd.to_datetime(time)
    quarter_end = pd.to_datetime(time) + pd.DateOffset(months=3)

    filtered_orders = orders[
        (orders['o_orderdate'] >= quarter_start) &
        (orders['o_orderdate'] < quarter_end) &
        (orders['o_orderkey'].isin(
            lineitem[(lineitem['l_commitdate'] < lineitem['l_receiptdate'])]['l_orderkey'])
        )
    ]

    # df = pd.DataFrame([])
    # for index, row in filtered_data.iterrows():
    #     if(lineitem_check['l_orderkey']==orders['o_orderkey']):
    #         df = df.append(row)
    # lineitem_check = lineitem[
    #     lineitem['l_commitdate'] < lineitem['l_receiptdate']]
    # df = filtered_orders.merge(lineitem_check, how="inner", left_on='o_orderkey', right_on='l_orderkey')

    result = filtered_orders.groupby('o_orderpriority').agg(
        order_count=pd.NamedAgg(column='o_orderpriority', aggfunc='count')
    ).reset_index()

    # result = df.groupby('o_orderpriority').size().reset_index(name='order_count')
    
    result = result.sort_values(by='o_orderpriority')
    return result
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = pandas_q4("1993-7-01",orders,lineitem)
    # result.to_csv("correct_results/pandas_q4.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/pandas_q4.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
