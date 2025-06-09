import cbsodata
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NIELSEN_CASE_STUDY").getOrCreate()

res = cbsodata.get_data('84910NED',filters = "(RegioS eq 'PV20  ' or RegioS eq 'PV21  ' or RegioS eq 'PV22  ' or RegioS eq 'PV23  ' or RegioS eq 'PV24  ' or RegioS eq 'PV25  ' or RegioS eq 'PV26  ' or RegioS eq 'PV27  ' or RegioS eq 'PV28  ' or RegioS eq 'PV29  ' or RegioS eq 'PV30  ' or RegioS eq 'PV31  ' or RegioS eq 'NL01  ' or RegioS eq 'GM0513') and (Perioden eq '2021JJ00')")
df = spark.createDataFrame(res)

# Task1 - Fetching all the residents from 12 dutch provinces who are from 1st genration migration and are from China
china_df = df.where((df.Migratieachtergrond =='China') & (df.Generatie == '1e generatie migratieachtergrond') &  (df.Leeftijd == 'Totaal')  &  (df.Geslacht == 'Totaal mannen en vrouwen')).select("RegioS","BevolkingOp1Januari_1")

df1 = china_df.where('RegioS not in ("Gouda","Nederland")')
df1.show(truncate=False)

# Task2 - Creating dataframe from china_df and calculating average of 12 dutch provinces and then average of nederaland data.
df2 = (china_df 
    .agg(
        avg(when(col("RegioS").endswith("(PV)"), col("BevolkingOp1Januari_1"))).alias("weighted_province_average"),
        max(when(col("RegioS") == "Nederland", col("BevolkingOp1Januari_1")) / 12).alias("nederland_average")
    ) )
df2.show(truncate=False)

# Task-3 Creating a dataframe to fetch record of 12 dutch provinces plus Gouda municaiplaity which were 1st generation migration and were from India and aged between 20 to 50
india_df = df.where((df.Migratieachtergrond =='India') & (df.Generatie == '1e generatie migratieachtergrond') &  (df.Leeftijd.isin('20 tot 25 jaar','25 tot 30 jaar','30 tot 35 jaar','35 tot 40 jaar','40 tot 45 jaar','45 tot 50 jaar'))  &  (df.Geslacht == 'Vrouwen')).groupBy("RegioS").agg(sum("BevolkingOp1Januari_1").alias("BevolkingOp1Januari_1"))
df3 = india_df.where(~india_df.RegioS.isin("Nederland"))


df3.show(truncate=False)


# Task 4 Creating dataframe containing resident share of Gouda and one closest to it.
df4 =  df.where((df.Migratieachtergrond =='Totaal') & (df.Generatie == 'Totaal') &  (df.Leeftijd == 'Totaal')  &  (df.Geslacht == 'Totaal mannen en vrouwen')).select("RegioS","BevolkingOp1Januari_1")
df5 = df4.alias("tbl_1").join(india_df.alias("tbl_2") , df4.RegioS == india_df.RegioS, "inner").select("tbl_1.RegioS",expr("tbl_2.BevolkingOp1Januari_1/tbl_1.BevolkingOp1Januari_1 as share"),expr("tbl_2.BevolkingOp1Januari_1 as indian_reisdents"),expr("tbl_1.BevolkingOp1Januari_1 as total_resident"))


gouda_share = df5.filter(col("RegioS") == "Gouda").select("share").first()["share"]

# Filter only provinces (exclude Gouda and Nederland and municipalities)
provinces_df = df5.filter(
    (~col("RegioS").isin("Gouda", "Nederland"))
)
provinces_df = provinces_df.withColumn("diff", abs(col("share") - lit(gouda_share)))
closest_province = provinces_df.orderBy("diff").limit(1)
gouda_df = df5.filter(col("RegioS") == "Gouda")
df4 = gouda_df.unionByName(closest_province.select(df5.columns)).select("RegioS","share")


df4.show(truncate=False)

