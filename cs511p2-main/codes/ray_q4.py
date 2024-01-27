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
import numpy as np
import math

ray.init(ignore_reinit_error=True)

@ray.remote
def check_order_priority(time: int, orders_chunk: pd.DataFrame, lineitem_chunk: pd.DataFrame) -> pd.DataFrame:
    orders_chunk['o_orderdate'] = pd.to_datetime(orders_chunk['o_orderdate'])
    lineitem_chunk['l_commitdate'] = pd.to_datetime(lineitem_chunk['l_commitdate'])
    lineitem_chunk['l_receiptdate'] = pd.to_datetime(lineitem_chunk['l_receiptdate'])
    
    quarter_start = pd.to_datetime(time)
    quarter_end = pd.to_datetime(time) + pd.DateOffset(months=3)

    filtered_orders = orders_chunk[
        (orders_chunk['o_orderdate'] >= quarter_start) &
        (orders_chunk['o_orderdate'] < quarter_end)
    ]

    filtered_lineitem = lineitem_chunk[lineitem_chunk['l_commitdate'] < lineitem_chunk['l_receiptdate']]
    
    merged_df = pd.merge(filtered_orders, filtered_lineitem, how='inner', left_on='o_orderkey', right_on='l_orderkey')

    grouped_df = merged_df.groupby('o_orderpriority').agg(
        order_count=pd.NamedAgg(column='o_orderpriority', aggfunc='count')
    ).reset_index()

    return grouped_df

def ray_q4(time: str, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    
    # print(len(orders)) 1500000
    # print(len(lineitem)) 6001215
    num_chunks = 4 
    orders_chunks = np.array_split(orders, num_chunks)
    lineitem_chunks = np.array_split(lineitem, num_chunks)
    
    futures = [check_order_priority.remote(time, orders_chunk, lineitem_chunk) for orders_chunk, lineitem_chunk in zip(orders_chunks, lineitem_chunks)]
    results = ray.get(futures)

    result = pd.concat(results).groupby('o_orderpriority').size().reset_index(name='order_count').sort_values(by='o_orderpriority')

    ray.shutdown()
    
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
    result = ray_q4("1993-7-01",orders,lineitem)
    # result.to_csv("correct_results/pandas_q4.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/ray_q4.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
