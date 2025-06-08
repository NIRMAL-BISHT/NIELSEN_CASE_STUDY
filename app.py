import cbsodata
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NIELSEN_CASE_STUDY").getOrCreate()

res = cbsodata.get_data('84910NED',filters = "(RegioS eq 'PV20  ' or RegioS eq 'PV21  ' or RegioS eq 'PV22  ' or RegioS eq 'PV23  ' or RegioS eq 'PV24  ' or RegioS eq 'PV25  ' or RegioS eq 'PV26  ' or RegioS eq 'PV27  ' or RegioS eq 'PV28  ' or RegioS eq 'PV29  ' or RegioS eq 'PV30  ' or RegioS eq 'PV31  ' or RegioS eq 'NL01  ' or RegioS eq 'GM0513' or RegioS eq 'GM0518') and (Perioden eq '2021JJ00')")
df = spark.createDataFrame(res)

china_df = df.where((df.Migratieachtergrond =='China') & (df.Generatie == '1e generatie migratieachtergrond') &  (df.Leeftijd == 'Totaal')  &  (df.Geslacht == 'Totaal mannen en vrouwen')).select("RegioS","BevolkingOp1Januari_1")

df1 = china_df.where('RegioS not in ("\'s-Gravenhage (gemeente)","Gouda","Nederland")')

df1.show()

df2 = (china_df 
    .agg(
        avg(when(col("RegioS").endswith("(PV)"), col("BevolkingOp1Januari_1"))).alias("weighted_province_average"),
        max(when(col("RegioS") == "Nederland", col("BevolkingOp1Januari_1")) / 12).alias("nederland_average")
    ) )
df2.show()


india_df = df.where((df.Migratieachtergrond =='India') & (df.Generatie == '1e generatie migratieachtergrond') &  (df.Leeftijd.isin('20 tot 25 jaar','25 tot 30 jaar','30 tot 35 jaar','35 tot 40 jaar','40 tot 45 jaar','45 tot 50 jaar'))  &  (df.Geslacht == 'Vrouwen')).groupBy("RegioS").sum("BevolkingOp1Januari_1")
df3 = india_df.where(~india_df.RegioS.isin("\'s-Gravenhage (gemeente)","Nederland"))

df3.show()