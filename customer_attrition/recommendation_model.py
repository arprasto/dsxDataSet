import os
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
df_cust_txns = SQLContext(sc).read.csv(os.environ['DSX_PROJECT_DIR']+'/datasets/cust_txns.csv', header='true', inferSchema = 'true')
df_products_weigt = SQLContext(sc).read.csv(os.environ['DSX_PROJECT_DIR']+'/datasets/products_weigt.csv', header='true', inferSchema = 'true')
sparkSql = SparkSession.builder.config(conf=sc.getConf()).enableHiveSupport().getOrCreate()


all_products=df_products_weigt.select('product_id').distinct()
all_customers_ids=df_cust_txns.select('cust_id').distinct()


delta1=all_customers_ids.join(all_products).rdd.map(lambda x:(x.cust_id+"-"+x.product_id,0))


delta2=df_cust_txns.rdd.map(lambda x:(x.cust_id+"-"+x.product_id,x.purchased_qty))
delta2.collect()


from operator import add

cust_id_prod_map=delta1.union(delta2).reduceByKey(add).collectAsMap()
#cust_id_prod_map


all_customers_ids_lst=all_customers_ids.collect()
all_products_lst=all_products.collect()
cust_prod_delta = [(x,y,z) for x in all_customers_ids_lst for y in all_customers_ids_lst for z in all_products_lst if x!=y]
cust_prod_delta


cust_prod_comparison_delta=sc.parallelize(cust_prod_delta,).map(lambda x:(x[0].cust_id+"-"+x[2].product_id,x[1].cust_id+"-"+x[2].product_id))
cust_prod_comparison_delta.collect()


temp=cust_prod_comparison_delta.map(lambda x:(x[0],x[1],pow((cust_id_prod_map[x[0]]-cust_id_prod_map[x[1]]),2)))
temp.collect()


import math
L2Vector_map=temp.map(lambda x:(x[0].split("-")[0]+"-"+x[1].split("-")[0],x[2])).reduceByKey(add).map(lambda x:(x[0],math.sqrt(x[1])))
L2Vector_map.collectAsMap()


L2Vector_sorted_map=L2Vector_map.map(lambda x:(x[0].split("-")[0],x[0].split("-")[1],x[1])).sortBy(lambda x: x[2],ascending=False).map(lambda x:(x[0],(x[1],x[2]))).groupByKey().map(lambda x:(x[0],list(x[1]))).collectAsMap()
L2Vector_sorted_map



cust_prod_comparison_mat=all_customers_ids.rdd.map(lambda x:x.cust_id+"-"+L2Vector_sorted_map[x.cust_id][0][0]+"-"+L2Vector_sorted_map[x.cust_id][len(L2Vector_sorted_map[x.cust_id])-1][0])
cust_prod_comparison_mat.collect()



cust_txns_prods_map=df_cust_txns.rdd.map(lambda x:(x.cust_id,x.product_id)).groupByKey().map(lambda x:(x[0],list(x[1]))).collectAsMap()


recom_rdd=cust_prod_comparison_mat.map(lambda x:(x.split("-")[0],((set(cust_txns_prods_map[x.split("-")[1]])|set(cust_txns_prods_map[x.split("-")[2]]))-set(cust_txns_prods_map[x.split("-")[0]]) )))
recom_rdd.collect()
