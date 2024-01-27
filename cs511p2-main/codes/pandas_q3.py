"""
 	 Author: University of Illinois at Urbana Champaign
 	 Date: 2023-09-10 20:19:52
 	 LastEditTime: 2023-09-11 10:03:13
 	 FilePath: /codes/pandas_q3.py
 	 Description: 
"""
import tempfile

import pandas as pd
import ray
import typing

import util.judge_df_equal


def pandas_q3(segment: str, customer: pd.DataFrame, orders: pd.DataFrame, lineitem: pd.DataFrame) -> pd.DataFrame:
    #TODO: your codes begin
    # print(customer.head)
    # print(orders.head)
    # print(lineitem.head)

    lineitem['l_shipdate'] = pd.to_datetime(lineitem['l_shipdate'])
    orders['o_orderdate'] = pd.to_datetime(orders['o_orderdate'])
    comp_date = pd.to_datetime("1995-03-15")

    # filtered = customer['c_mktsegment'] = segment & orders['o_orderdate'] < comp_date & lineitem['l_shipdate'] > comp_date

    # result = customer.merge(orders, how='cross', left_on='c_custkey', right_on='o_custkey').merge(lineitem, how='cross', left_on='o_orderkey', right_on='l_orderkey') pandas.errors.MergeError: Can not pass on, right_on, left_on or set right_index=True or left_index=True
    # result = customer.merge(orders, how='cross').merge(lineitem, how='cross') numpy.core._exceptions._ArrayMemoryError: Unable to allocate 1.64 TiB for an array with shape (225000000000,) and data type int64
    result = customer.merge(orders, how='left', left_on='c_custkey', right_on='o_custkey').merge(lineitem, how='left', left_on='o_orderkey', right_on='l_orderkey') 
    filtered = result[
        (result['c_mktsegment'] == segment) & 
        (result['o_orderdate'] < comp_date) & 
        (result['l_shipdate'] > comp_date)
    ]
    
    filtered = filtered.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority']).agg(
        revenue=pd.NamedAgg(column='l_extendedprice', aggfunc=lambda x: (x * (1 - filtered['l_discount'])).sum())).reset_index()
    
    filtered = filtered.sort_values(by=['revenue', 'o_orderdate'], ascending = [False, True])
    #print(filtered.head)

    #concatenation
    lst = [customer, orders, lineitem]
    df_result= pd.concat(lst, axis=1, ignore_index=True)
    #print('\n Concatenated Output  \n' ,df_result.head)
    
    return filtered.head(10)
    #end of your codes



if __name__ == "__main__":
    # import the logger to output message
    import logging
    logger = logging.getLogger()
    # read the data
    lineitem = pd.read_csv("tables/lineitem.csv", header=None, delimiter="|")
    orders = pd.read_csv("tables/orders.csv", header=None, delimiter="|")
    customer = pd.read_csv("tables/customer.csv", header=None, delimiter="|")


    lineitem.columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                        'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
    customer.columns = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                        'c_comment']
    orders.columns = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority',
                      'o_clerk', 'o_shippriority', 'o_comment']

    # run the test
    result = pandas_q3('BUILDING', customer, orders, lineitem)
    # result.to_csv("correct_results/pandas_q3.csv", float_format='%.3f')
    with tempfile.NamedTemporaryFile(mode='w') as f:
        result.to_csv(f.name, float_format='%.3f',index=False)
        result = pd.read_csv(f.name)
        correct_result = pd.read_csv("correct_results/pandas_q3.csv")
        try:
            assert util.judge_df_equal.judge_df_equal(result, correct_result)
            print("*******************pass**********************")
        except Exception as e:
            logger.error("Exception Occurred:" + str(e))
            print(f"*******************failed, your incorrect result is {result}**************")
