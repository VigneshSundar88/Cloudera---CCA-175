# Databricks notebook source
#Create RDDs
orderItems = sc.textFile("/FileStore/tables/cca175/test_data/retail_db/order_items/part_00000-6a99e")
#type(orderItems)
#help(orderItems)
#orderItems.first()
#orderItems.take(10)
orderItemMaps = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
#orderItemMaps.take(5)
revenuePerOrder = orderItemMaps.reduceByKey(lambda key, val: key+val)
#revenuePerOrder.take(5)
revenuePerOrder.toDebugString()

# COMMAND ----------

l = range(1, 10000)
l_rdd = sc.parallelize(l)
l_rdd.take(5)

# COMMAND ----------

#help(spark.read.json)
#help(spark.read.text)
#help(spark.read.parquet)
#help(spark.read.orc)
#spark.read.json("/FileStore/tables/retail_db_json/order_items/part_r_00000_6b83977e_3f20_404b_9b5f_29376ab1419e-9d0b8").show()
orders = sc.textFile("/FileStore/tables/cca175/test_data/retail_db/orders/part_00000-6a99e")
orderItems = sc.textFile("/FileStore/tables/cca175/test_data/retail_db/order_items/part_00000-6a99e")
#first_str = orders.first()
#print(len(first_str))
#split_str = first_str.split(",")
#print(split_str)
#map_func = orders.map(lambda split_word: (split_word.split(",")[3], int(1)))
#map_func_1 = orders.flatMap(lambda split_word: (split_word.split(",")[3], int(1)))
#map_func.take(10)
#map_func_1.take(10)
#group_func = map_func.groupByKey()
#group_func.take(5)

# COMMAND ----------

#ordersComplete = orders.filter(lambda val: (val.split(",")[3] == "COMPLETE" or val.split(",")[3] == "CLOSED") and val.split(",")[1].split(" ").split("-")[0] == "2014")
#ordersComplete = orders.filter(lambda val: (val.split(",")[3] == "COMPLETE" or val.split(",")[3] == "CLOSED") and "2014-02" in val.split(",")[1][:7])
ordersComplete = orders.filter(lambda val: val.split(",")[3] in ["COMPLETE", "CLOSED"] and "2014-02" in val.split(",")[1][:7])
#ordersMap_1 = orders.map(lambda val_1: val_1.split(",")[1])
#ordersMap_2 = ordersMap_1.map(lambda val_2: val_2.split(" ")[0])
#ordersMap_3 = ordersMap_2.map(lambda val_3: val_3.split("-")[0])
ordersComplete.take(20)
#ordersMap_3.take(5)
#orderMap = orders.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderMap = orders.map(lambda o: (int(o.split(",")[0]), o.split(",")[3]))
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
inner_join = orderMap.join(orderItemsMap).take(5)
#print(inner_join)
left_outer = orderMap.leftOuterJoin(orderItemsMap)
#print(left_outer)
left_outer.count()
#left_outer.filter(lambda oi: oi[1][1] == None).take(5)
#left_outer.filter(lambda oi: oi[0] == 65596).take(5)
#for val in left_outer.take(100): if val[1][1] == None: print(val)

# COMMAND ----------

orderItemFilter = orderItems.filter(lambda oi: int(oi.split(",")[1]) == 2)
print(orderItemFilter.take(5))
orderItemSubTot = orderItemFilter.map(lambda val: float(val.split(",")[4]))
#help(orderItemSubTot.reduce)
from operator import add
add_val_rdd = orderItemSubTot.reduce(add)
#print(add_val_rdd)
add_val_func = orderItemSubTot.reduce(lambda x, y: x+y)
#print(add_val_func)
min_val_func = orderItemFilter.reduce(lambda x, y: x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y)
print(min_val_func)

# COMMAND ----------

order_stat = orders.map(lambda x: (x.split(",")[3], 1))
orderItemOrderSubTot = orderItemFilter.map(lambda order_val: (int(order_val.split(",")[1]), float(order_val.split(",")[4])))
l = orderItemOrderSubTot.groupByKey()
revenuePerOrderId = l.map(lambda ord_val: (ord_val[0], sum(ord_val[1])))
#revenuePerOrderId.take(10)
#list_map = map(lambda val: float(val), l)
#print(sum(list_map))
#sum(list_map)
#for i in orderItems.take(10): print(i)
orderItemsMap = orderItems.map(lambda val: (int(val.split(",")[1]), val))
#orderItemsMap.take(10)

orderItemsGroupByKey = orderItemsMap.groupByKey()
sortOrderMap = orderItemsGroupByKey.map(lambda listVal: sorted(listVal[1], key = lambda k: float(k.split(",")[4]), reverse=True))
#sortOrderMap.take(10)
sortOrderFlatMap = orderItemsGroupByKey.flatMap(lambda listVal: sorted(listVal[1], key = lambda k: float(k.split(",")[4]), reverse=True))
#sortOrderFlatMap.take(10)

reduceByKey_all = orderItemsMap.reduceByKey(lambda x, y: x+y)
reduceByKey_all.take(10)

reduceByKey_least = orderItemsMap.reduceByKey(lambda x, y: x if float(x.split(",")[4]) < float(y.split(",")[4]) else y)
reduceByKey_least.take(10)

# COMMAND ----------

#aggregatebykey
orderItems = sc.textFile("/FileStore/tables/cca175/test_data/retail_db/order_items/part_00000-6a99e")
#orderItems.take(1)
orderItemsMap = orderItems.map(lambda val: (int(val.split(",")[1]), float(val.split(",")[4])))
#orderItemsMap.take(10)
orderItemsAgg = orderItemsMap.aggregateByKey((0.0, 0), lambda x, y: (x[0] + y, x[1] + 1), lambda x, y: (x[0] + y[0], x[1] + y[1]))
orderItemsAgg.take(10)

# COMMAND ----------

#sortbykey
prodRDD = sc.textFile("/FileStore/tables/cca175/test_data/retail_db/products")
prodMapFiltered = prodRDD.filter(lambda f: f.split(",")[4] != "")
#prodMapFiltered.top(5, key=lambda k: k.split(",")[4])
#prodMapFiltered.takeOrdered(5, key=lambda k: -float(k.split(",")[4]))
prodMapKey = prodMapFiltered.map(lambda a: (a.split(",")[1], a))
#prodSort = prodMapKey.groupByKey().map(lambda b: b[1]).map(lambda c: sorted(c, reverse=True, key=lambda d: d.split(",")[4])).take(5)
prodSort = prodMapKey.groupByKey().map(lambda c: sorted(c[1], key=lambda d: d.split(",")[4], reverse=True)).take(5)
prodSort[0]

# COMMAND ----------

from pyspark.sql.functions import col, lower, upper, expr, when
orders = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/orders/part_00000-6a99e", header=False, inferSchema=True)
orderItems = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/order_items/part_00000-6a99e", header=False, inferSchema=True)
customers = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/customers/part_00000-6a99e", header=False, inferSchema=True)
products = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/products/part_00000-6a99e", header=False, inferSchema=True)
categories = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/categories/part_00000-6a99e", header=False, inferSchema=True)

orders_new_col = orders.toDF("order_id", "order_date", "order_customer_id", "order_status")
#orders_new_col.printSchema()
#orders_new_col.select("order_date", col("order_customer_id")).show(3)
orders_new_col.select(upper(orders_new_col.order_status), upper(col("order_status")))
orders_new_col.withColumn("status", orders_new_col.order_status.cast("string")).withColumn("date", col("order_date").cast("timestamp"))
orders_new_col.selectExpr('order_status||","||order_date||","||order_customer_id||","||order_status as textdata')
orders_new_col.selectExpr("order_status", 'CASE WHEN order_status in ("CLOSED", "COMPLETE") THEN "COMPLETED" WHEN order_status in ("PENDING_PAYMENT") THEN "PENDING" ELSE "NONE" END status').where("status == 'COMPLETED'")
orders_new_col.withColumn("status", when(orders_new_col.order_status.isin("CLOSED", "COMPLETE"), 'OVER').when(orders_new_col.order_status.isin("PENDING_PAYMENT"), "PENDING").otherwise("NONE"))

#orders_new_col.withColumn('Derived_Status',when(orders_new_col.order_status.isin('COMPLETED','CLOSED'),'OVER'). \
                  #when(orders_new_col.order_status.isin('CANCELED'),'PENDING').otherwise('NONE')).show()


# COMMAND ----------

from pyspark.sql.functions import col, countDistinct
case_val = orders_new_col.withColumn("derived_col", when(orders_new_col.order_status.isin("COMPLETE"), "COMPLETED").when(orders_new_col.order_status.isin("PENDING_PAYMENT"), "PENDING").otherwise("NONE"))
#case_val.show()
orderItemsNewCol = orderItems.toDF("col_1", "col_2", "col_3", "col_4", "col_5", "col_6")
#orders_new_col.join(orderItemsNewCol, orders_new_col.order_id==orderItemsNewCol.col_2).show()
cond = [orders_new_col.order_id == orderItemsNewCol.col_2]
orders_new_col.join(orderItemsNewCol, cond, "right").where("col_2 is null")
#orders_new_col.count()
#orderItemsNewCol.select("col_2").count()
#orderItemsNewCol.select("col_2").distinct().count()
orderItemsNewCol.agg(countDistinct("col_2"))
orderItemsNewCol.orderBy(col("col_3").desc())

# COMMAND ----------

from pyspark.sql.functions import count, avg, max, min, sum, round, rank, col, dense_rank
from pyspark.sql.window import Window
#orders_new_col.select(count(orders_new_col.order_id)).show()
#orderItemsNewCol.agg(count("col_2"), max("col_2"), min("col_2"), avg("col_2"), sum("col_2")).show()
#orderItemsNewCol.agg(count("col_2").alias("count_val"), max("col_2").alias("max_col"), min("col_2").alias("min_col"), round(avg("col_2"), 2).alias("avg_col"), sum("col_2").alias("sum_col")).show()
#orderItemsNewCol.select("*").show()
#orderItemsNewCol.groupBy("col_2", "col_3").agg(count("*")).show()
spec = Window.partitionBy("col_2").orderBy(col("col_3").desc())
orderItemsNewCol.withColumn("agg_val", sum("col_2").over(spec)).withColumn("avg_val", avg("col_2").over(spec)).withColumn("rank_val", rank().over(spec)).withColumn("dense_rank_val", dense_rank().over(spec)).show()

# COMMAND ----------

from pyspark.sql.functions import col, substring, substring_index, instr, split, concat_ws, repeat
from pyspark.sql.types import StringType
#substring
#orders_new_col.show()
func_df = orders_new_col.select('order_status', substring('order_status', 1, 2).alias("sub"), substring_index('order_status', "E", -3).alias("sub_ind")).select("*", instr('sub_ind', 'E').alias("instr_val"), split('order_status', "_")[0].alias("split_val")).select("*", concat_ws("|", "order_status", "sub").alias("concat_val"))
func_df.withColumn("repeat_val", repeat("instr_val", 3)).select("*", concat_ws("|", *func_df.columns).alias("conc_ws")).show(truncate=False)
#orders_new_col.select(substring_index('order_status', "_", 2)).show()
#list_1 = ["col_1", "col_2"]
#df_1 = spark.createDataFrame(list_1, StringType())
#df_1.select(substring_index("value", "_", 1)).show()

# COMMAND ----------

#Date
from pyspark.sql.functions import current_timestamp, current_date, date_format, dayofyear, year, month, date_add, date_sub, datediff, add_months, months_between, next_day, last_day, date_trunc, lit
orders_new_col.select(current_timestamp(), current_date(), date_format(current_timestamp(), "yyyy-MM-dd a hh:HH:mm").alias("dat"), dayofyear(current_timestamp()).alias("dayofyear_val"), month(current_timestamp()).alias("mon"), date_sub(current_timestamp(), 1).alias("date_d")).select(datediff(current_timestamp(), "date_d")).show(1, truncate=False)
orders_new_col.select(add_months(current_timestamp(), -1).alias("prev_mon")).select(months_between(current_timestamp(), "prev_mon").alias("no_of_mon")).select(next_day(current_timestamp(), "Mon")).select(last_day(current_timestamp())).show(1)
orders_new_col.select(date_trunc('year', lit('2020-04-01'))).show(1)

# COMMAND ----------

from pyspark.sql.functions import concat_ws
#orders_new_col.printSchema()
#spark.sql("create database if not exists newdb")
#spark.sql("show databases").show()
ordersText = orders_new_col.select(concat_ws("~", "order_id", "order_date", "order_customer_id", "order_status").alias("concal_col"))
#ordersText.write.mode("overwrite").text("/FileStore/tables/cca175/test_data/retail_db/order_items/text_format")
#read_text_file = spark.read.text("/FileStore/tables/cca175/test_data/retail_db/order_items/text_format")
#read_text_file.show(truncate=False)
ordersText.write.csv("/FileStore/tables/cca175/test_data/retail_db/order_items/csv_format", mode="overwrite")
orderCsv_read = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/order_items/csv_format", sep="~")
#orderCsv_read.show(truncate=False)
#ordersText.coalesce(1).write.csv("/FileStore/tables/cca175/test_data/retail_db/order_items/csv_format_1", mode="append")
orderCSV1Read = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/order_items/csv_format_1", sep="~")
#orderCSV1Read.show()
ordersText.repartition(10).write.json("/FileStore/tables/cca175/test_data/retail_db/order_items/json_format", mode="append")
orderJsonRead = spark.read.json("/FileStore/tables/cca175/test_data/retail_db/order_items/json_format")
orderJsonRead.show()

# COMMAND ----------

ordersNewCol = orders.toDF("order_id", "order_date", "order_customer_id", "order_status")
orderItemsNewCol = orderItems.toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price")
productsNewCol = products.toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")
categoriesNewCol = categories.toDF(*["category_id", "category_department_id", "category_name"])

#Problem 1
# 1. Top N orders for the day based on order revenue
from pyspark.sql.functions import rank, col, month, sum, count, avg, round, lead, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

join_orders_orderItems = ordersNewCol.join(orderItemsNewCol, ordersNewCol.order_id == orderItemsNewCol.order_item_order_id)
join_orders_orderItems_rev_sum = join_orders_orderItems.groupBy("order_item_order_id", "order_date").agg(count("order_item_order_id").alias("no_of_revenue"), round(sum("order_item_subtotal"), 2).alias("total_revenue"))
#join_orders_orderItems_rev_sum.show(10)
join_orders_orderItems_rev_sum.printSchema()
spec = Window.partitionBy(join_orders_orderItems_rev_sum.order_date).orderBy(join_orders_orderItems_rev_sum.total_revenue.desc())
rank_df = join_orders_orderItems_rev_sum.withColumn("rank_col", rank().over(spec))
rank_df.orderBy("order_date", col("total_revenue").desc()).where("rank_col <= 5").show()
#rank_df.select("order_id", "order_item_order_id", "order_date", "order_item_id", "order_item_subtotal", month("order_date").alias("month_val"), "rank_col").where("rank_col <= 4 and month_val != 7").orderBy(col("order_date").desc()).show(100)

#2. Get average revenue per day and all the orders which are more than average.
#join_orders_orderItems.where("order_date = '2013-07-27 00:00:00'").agg(avg("order_item_subtotal").alias("avg_val_dis")).show()
join_orders_orderItems_rev_sum = join_orders_orderItems.groupBy("order_item_order_id", "order_date").agg(round(sum("order_item_subtotal"), 2).alias("total_revenue"))
spec_1 = Window.partitionBy("order_date")
join_orders_orderItems_rev_avg = join_orders_orderItems_rev_sum.withColumn("avg_rev", avg("total_revenue").over(spec_1))
join_orders_orderItems_rev_avg.where("total_revenue >= avg_rev").orderBy("order_date", col("total_revenue").desc()).show()

#3. Top N products for the day
join_orders_orderItems = ordersNewCol.join(orderItemsNewCol, ordersNewCol.order_id == orderItemsNewCol.order_item_order_id)
join_orders_orderItems_productsNewCol = join_orders_orderItems.join(productsNewCol, join_orders_orderItems.order_item_product_id == productsNewCol.product_id)
join_orders_orderItems_rev_sum = join_orders_orderItems_productsNewCol.groupBy("order_date", "product_name").agg(round(sum("order_item_subtotal"), 2).alias("total_revenue"))
spec_2 = Window.partitionBy("order_date").orderBy(col("total_revenue").desc())
join_orders_rank = join_orders_orderItems_rev_sum.withColumn("rank_val", rank().over(spec_2)).orderBy("order_date", "rank_val")
join_orders_rank.where("rank_val <= 5").show(truncate=False)

#4. Percentage of order items in Order revenue
#orderItemsRevenue = orderItemsNewCol.groupBy("order_item_order_id").agg(round(sum("order_item_subtotal")).alias("sum_rev"))
spec_3 = Window.partitionBy("order_item_order_id")
orderItemsRevenue = orderItemsNewCol.withColumn("sum_rev", round(sum("order_item_subtotal").over(spec_3), 2))
orderItemsRevenue.select('order_item_order_id', 'order_item_id',
                 'order_item_subtotal', 'sum_rev').withColumn("perc_val", round(col("order_item_subtotal")/col("sum_rev"), 2)).orderBy(col('order_item_order_id')).show()

#5. Difference in top 2 order items for the order revenue
spec_4 = Window.partitionBy("order_item_order_id").orderBy(col("order_item_subtotal").desc())
orderItems_group = orderItemsNewCol.withColumn("rank_val", rank().over(spec_4))
orderItems_group_diff = orderItems_group.fillna(0).withColumn('next', lead(col("order_item_subtotal")).over(spec_4)).withColumn("diff_val", (col('order_item_subtotal').cast("double")-col("next").cast("double"))).where("rank_val == 1")
orderItems_group_diff.select('order_item_order_id', 'order_item_id',
                 'order_item_subtotal', 'next', round('diff_val', 2), 
                 'rank_val').fillna(0).orderBy('order_item_order_id', 'rank_val').show()

#6. Get order items contributing more than 75% of the total order
spec_5 = Window.partitionBy("order_item_order_id")
orderItems_sum = orderItemsNewCol.withColumn("total_val", round(sum("order_item_subtotal").over(spec_5), 2))
orderItems_sum.withColumn("perc_val", round(col("order_item_subtotal")/orderItems_sum.total_val, 2)).where("perc_val >= 0.75").orderBy("order_item_order_id").drop('order_item_product_id','order_item_quantity','order_item_product_price').show()

#7.What are the best-selling and the second best-selling products in every category?
#spec_7 = Window.partitionBy()
#orderItemQuantitySum = orderItemsNewCol.withColumn("total_sale", sum(col("order_item_quantity")).over(spec_7))
orderItemQuantitySumJoinProductJoinCategory = orderItemsNewCol.join(productsNewCol, orderItemsNewCol.order_item_product_id == productsNewCol.product_id).join(categoriesNewCol, productsNewCol.product_category_id == categoriesNewCol.category_id).groupBy("category_name", "product_name").agg(round(sum(col("order_item_subtotal")), 2).alias("total_val"))
#orderItemQuantitySumJoinProductJoinCategory.show(truncate=False)
spec_8 = Window.partitionBy("category_name").orderBy(col("total_val").desc())
orderItemQuantitySumJoinProductJoinCategory.withColumn("rank_val", dense_rank().over(spec_8)).orderBy(col("category_name"), col("rank_val")).where("rank_val <= 2").show()

# COMMAND ----------

cust_dict = {"customer_id":1,"customer_fname":"Richard","customer_lname":"Hernandez","customer_email":"XXXXXXXXX","customer_password":"XXXXXXXXX","customer_street":"6303 Heather Plaza","customer_city":"Brownsville","customer_state":"TX","customer_zipcode":"78521"}

orders = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/orders/part_00000-6a99e", header=False, inferSchema=True)
orderItems = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/order_items/part_00000-6a99e", header=False, inferSchema=True)
customers = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/customers/part_00000-6a99e", header=False, inferSchema=True)
products = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/products/part_00000-6a99e", header=False, inferSchema=True)
categories = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/categories/part_00000-6a99e", header=False, inferSchema=True)
departments = spark.read.csv("/FileStore/tables/cca175/test_data/retail_db/departments/part_00000-6a99e", header=False, inferSchema=True)

ordersNewCol = orders.toDF("order_id", "order_date", "order_customer_id", "order_status")
orderItemsNewCol = orderItems.toDF("order_item_id", "order_item_order_id", "order_item_product_id", "order_item_quantity", "order_item_subtotal", "order_item_product_price")
productsNewCol = products.toDF("product_id", "product_category_id", "product_name", "product_description", "product_price", "product_image")
categoriesNewCol = categories.toDF(*["category_id", "category_department_id", "category_name"])
customersNewCol = customers.toDF(*cust_dict.keys())
departmentsNewCol = departments.toDF("department_id", "department_name")

#Problem 1
# 1. Top N orders for the day based on order revenue
from pyspark.sql.functions import rank, col, month, sum, count, avg, round, lead, dense_rank, max, date_format, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# 8. What is the difference between the revenue of each product and the revenue of the best-selling product in the same category of that product?
#orderItemsNewCol.printSchema
spec_8 = Window.partitionBy("category_name", "product_name")
orderItemQuantitySumJoinProductJoinCategory = orderItemsNewCol.join(productsNewCol, orderItemsNewCol.order_item_product_id == productsNewCol.product_id).join(categoriesNewCol, productsNewCol.product_category_id == categoriesNewCol.category_id).groupBy("category_name", "product_name").agg(round(sum(col("order_item_subtotal")), 2).alias("sum_val")).orderBy("category_name", "product_name")
#orderItemQuantitySumJoinProductJoinCategory.show(truncate=False)
spec_9 = Window.partitionBy("category_name").orderBy(col("sum_val").desc())
orderItemQuantitySumJoinProductJoinCategory.withColumn("max_val", max("sum_val").over(spec_9)).withColumn("diff_val", round(col("max_val")-col("sum_val"), 2)).orderBy("category_name", col("diff_val").desc()).show(truncate=False)

#9.Most selling product (But Quantity not by Cost) for every month in the database (Between July 2013 to July 2014)
ordersNewColorderItemsNewColJoin = ordersNewCol.join(orderItemsNewCol, ordersNewCol.order_id == orderItemsNewCol.order_item_order_id).join(productsNewCol, productsNewCol.product_id == orderItemsNewCol.order_item_product_id).withColumn("date_val", date_format(col('order_date'), 'yyyyMM').cast('bigint')).where(col("date_val").between(201307, 201407))

#ordersNewColorderItemsNewColJoin.where(~col("date_val").isin(201307, 201407)).show()
#ordersNewColorderItemsNewColJoin.show()

orderNewColGroup = ordersNewColorderItemsNewColJoin.groupBy("date_val", "product_name").agg(sum("order_item_quantity").alias("sum_val")).orderBy("date_val")

#orderNewColGroup.where("date_val not in ('072013', '072014')").show()

spec_10 = Window.partitionBy("date_val").orderBy(col("sum_val").desc())

rank_group = orderNewColGroup.withColumn("rank_val", rank().over(spec_10)).where("rank_val = 1")
rank_group.show()

#10. Who are the top 10 revenue generating customers
ordersOrderItemsProducts = ordersNewCol.join(orderItemsNewCol, ordersNewCol.order_id == orderItemsNewCol.order_item_order_id).join(customersNewCol, customersNewCol.customer_id == ordersNewCol.order_customer_id)
ordersOrderItemsProductsGroup = ordersOrderItemsProducts.groupBy("order_customer_id", concat_ws(" ", customersNewCol.customer_fname, customersNewCol.customer_lname)).agg(round(sum(col("order_item_subtotal")), 2).alias("sum_val")).orderBy(col("sum_val").desc())
ordersOrderItemsProductsGroup.limit(10).show()

#11. What are the top 10 revenue generating products
ordersNewColorderItemsNewColJoin = ordersNewCol.join(orderItemsNewCol, ordersNewCol.order_id == orderItemsNewCol.order_item_order_id).join(productsNewCol, productsNewCol.product_id == orderItemsNewCol.order_item_product_id)
ordersNewColorderItemsNewColJoin.groupBy("order_item_product_id", "product_name").agg(round(sum("order_item_subtotal"), 2).alias("total_val")).orderBy(col("total_val").desc()).limit(10).show(truncate=False)

#12. Top 5 revenue generating deparments
orderItemQuantitySumJoinProductJoinCategory = orderItemsNewCol.join(productsNewCol, orderItemsNewCol.order_item_product_id == productsNewCol.product_id).join(categoriesNewCol, productsNewCol.product_category_id == categoriesNewCol.category_id).join(departmentsNewCol, categoriesNewCol.category_department_id == departmentsNewCol.department_id)
orderItemQuantitySumJoinProductJoinCategoryGroup = orderItemQuantitySumJoinProductJoinCategory.groupBy("department_id", "department_name").agg(round(sum("order_item_subtotal"), 2).alias("total_val"))
orderItemQuantitySumJoinProductJoinCategoryGroup.orderBy(col("total_val").desc()).limit(5).show()

#13. Top 5 revenue generating cities (from address of Customers)
ordersOrderItemsProducts = ordersNewCol.join(orderItemsNewCol, ordersNewCol.order_id == orderItemsNewCol.order_item_order_id).join(customersNewCol, customersNewCol.customer_id == ordersNewCol.order_customer_id)
ordersOrderItemsProductsGroup = ordersOrderItemsProducts.groupBy("customer_city").agg(round(sum("order_item_subtotal"), 2).alias("total_val"))
ordersOrderItemsProductsGroup.write.format('com.databricks.spark.avro').\
option('mode', 'overwrite').\
option('compression', 'snappy').\
saveAsTable('default.avrotbl')
#ordersOrderItemsProductsGroup.orderBy(col("total_val").desc()).limit(5).show()
#ordersOrderItemsProductsGroup.write.parquet("/FileStore/tables/cca175/test_data/retail_db/products/parquet", compression='snappy')

# COMMAND ----------

from pyspark.sql.functions import concat_ws

ordersNewCol. \
    join(orderItemsNewCol, ordersNewCol.order_id ==  orderItemsNewCol.order_item_order_id). \
    join(customersNewCol, customersNewCol.customer_id ==  ordersNewCol.order_customer_id). \
    groupBy(customersNewCol.customer_city). \
    agg(round(sum(orderItemsNewCol.order_item_subtotal),2).alias('city_revenue')). \
    orderBy(col('city_revenue').desc()). \
    limit(5). \
    show()

# COMMAND ----------

help(spark.read.format('avro'))
