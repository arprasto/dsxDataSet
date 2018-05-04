import os
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
df_cust_txns = SQLContext(sc).read.csv(os.environ['DSX_PROJECT_DIR']+'/datasets/cust_txns.csv', header='true', inferSchema = 'true')
df_products_weigt = SQLContext(sc).read.csv(os.environ['DSX_PROJECT_DIR']+'/datasets/products_weigt.csv', header='true', inferSchema = 'true')
sparkSql = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()



def getCustomerDeltaWithPurchasedItemsWithQtyWeightVector(cust_id):
    a = df_cust_txns.select(df_cust_txns.cust_id,df_cust_txns.product_id,df_cust_txns.purchased_qty).where(df_cust_txns.cust_id==cust_id)
    return a.join(df_products_weigt,a.product_id==df_products_weigt.product_id)


def getCustomerDeltaWithNonPurchasedItems(cust_id):
    return (df_products_weigt.select(df_products_weigt.product_id)).subtract((df_cust_txns.select(df_cust_txns.product_id).where(df_cust_txns.cust_id==cust_id)))


def getCustomerDeltaIncludingAllItems(cust_id):
    delta_purchased_items=getCustomerDeltaWithPurchasedItemsWithQtyWeightVector(cust_id)
    not_purchased_list=getCustomerDeltaWithNonPurchasedItems(cust_id).collect()
    i=1
    df={}
    for item in not_purchased_list:
        if(i==1):
            df = sparkSql.createDataFrame([[cust_id, item.product_id, 0,item.product_id,0]], ["cust_id", "product_id", "purchased_qty","product_id","weight"])
            i=i+1
        else:
            df = df.union(sparkSql.createDataFrame([[cust_id, item.product_id, 0,item.product_id,0]], ["cust_id", "product_id", "purchased_qty","product_id","weight"]))

    delta_including_non_purchased_items=delta_purchased_items.union(df)
    return delta_including_non_purchased_items



import math
    
def getEucladianFactForCustomer(cust_id):
    cust_txn_delta_with_weight_vectors=getCustomerDeltaIncludingAllItems(cust_id)
    #cust_txn_delta_with_weight_vectors.show(1000)
    L2VectorSum=0
    cust_txn_delta_with_weight_vectors.foreach(lambda rec:
                                               L2VectorSum=L2VectorSum+pow((rec.weight-rec.purchased_qty/100),2)
                                              )
    return sparkSql.createDataFrame([[L2VectorSum]],[cust_id])


getCustomerDeltaIncludingAllItems('cust1273767').show()
