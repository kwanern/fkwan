from ...libraries import *
from .Customer import *


class Segmentation(object):
    def __init__(self, spark, period='year', date='2019-06-30'):
        """
                This is a function that returns the segmentation metrics.
                :param spark: spark object
                :param period: string. e.g. 'year', 'quarter'
                :param date: string e.g. For Year: ('2018-06-30', '2019-06-30');
                For Quarter: ('2018-12-31', '2019-03-31', '2019-06-30', '2019-09-30')
                :return: beverage segmentation spark table

                Examples:
                >>> product = {
                >>>   "Promo_Start_Date": "2019-04-30",
                >>>   "Promo_End_Date": "2019-06-24",
                >>>   "Product_Name": "Flavored Ice Tea and Refreshers",
                >>>   "EPH_level": "NotionalProductlId",
                >>>   "Id": refreshers_summer1_visual + refreshers_summer1_line + ice_tea_summer1_line,
                >>>   "Purchased_Freq_Min": 1,
                >>>   "Purchased_Freq_Max": 999999
                >>> }
                >>> df = Segmentation(spark, period='year', date='2019-06-30')
        """
        self.start_date = None
        self.end_date = None
        self.products_names = None
        self.level = None
        self.products_id = None
        self.pch_frq_min = None
        self.pch_frq_max = None
        self.spark = spark
        self.name = None
        self.period = period
        self.seg_date = date
        self.type = None

        self.pos = (
            self.spark
            .table("fkwan.pos_line_item")
            .filter(
                sqlf.col("AccountId").isNotNull() |
                sqlf.col("FirstPaymentToken").isNotNull()
            )
            .withColumn(
                "Id",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), sqlf.col("AccountId")
                )
                .otherwise(sqlf.col("FirstPaymentToken"))
            )
            .withColumn(
                "Customer_Type",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), "SR"
                )
                .otherwise("Token")
            )
        )

    def __segmentation(self, tp, product, cohort=None, base=False, title=None):
        self.start_date = product["Promo_Start_Date"]
        self.end_date = product["Promo_End_Date"]
        self.products_names = product["Product_Name"]
        self.level = product["EPH_level"]
        self.products_id = product["Id"]
        self.pch_frq_min = product["Purchased_Freq_Min"]
        self.pch_frq_max = product["Purchased_Freq_Max"]
        self.type = tp

        if title:
            self.name = title
        else:
            self.name = self.products_names

        self.pos = (
            self.pos
            .alias("pos")
            .filter(sqlf.col("BusinessDate").between(self.start_date, self.end_date))
            .join(
                self.spark
                .table("ttran.taste_segments_v3")
                .alias("sr"),
                ((sqlf.col("pos.Id") == sqlf.col("sr.GuidId")) &
                 (sqlf.col("sr.period") == self.period) &
                 (sqlf.col("sr.end_date") == self.seg_date)),
                how="left"
            )
            .join(
                self.spark
                    .table("ttran.taste_segments_non_sr_v3")
                    .alias("nonsr"),
                ((sqlf.col("pos.Id") == sqlf.col("nonsr.FirstPaymentToken")) &
                 (sqlf.col("nonsr.period") == self.period) &
                 (sqlf.col("nonsr.end_date") == self.seg_date)),
                how="left"
            )
            .withColumn(
                "Product",
                sqlf.lit(self.name)
            )
            .where(
                sqlf.col("sr.GuidId").isNotNull() |
                sqlf.col("nonsr.FirstPaymentToken").isNotNull()
            )
        )

        if cohort:
            self.pos = (
                self.pos.alias("result")
                    .join(
                    cohort.pf_spdf.alias("cohort"),
                    sqlf.col("result.AccountId") == sqlf.col("cohort.Id"),
                    how="inner"
                )
            )

        result = (
            self.pos
            .filter(sqlf.col(self.level).isin(self.products_id))
            .withColumn(
                "Product",
                sqlf.lit(self.name)
            )
            .withColumn(
                self.type,
                sqlf.coalesce(sqlf.col("sr."+self.type),sqlf.col("nonsr."+self.type))
            )
            .groupBy("Product", "Customer_Type", self.type)
            .agg(
                (sqlf.sum(sqlf.col("GrossLineItemQty")) / sqlf.countDistinct(sqlf.col("Id"))).alias(
                    "units_cust"),
                (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS")
            )
        )

        result = (
            result
            .withColumn(
                'units_cust_proportion',
                sqlf.col('units_cust') / sqlf.sum('units_cust').over(Window.partitionBy("Customer_Type"))
            )
            .withColumn(
                'total_units_proportion',
                sqlf.col('units') / sqlf.sum('units').over(Window.partitionBy("Customer_Type"))
            )
            .withColumn(
                'total_nds_proportion',
                sqlf.col('NDS') / sqlf.sum('NDS').over(Window.partitionBy("Customer_Type"))
            )
        )

        if base:
            base = (
                self.pos
                .withColumn(
                    "Product",
                    sqlf.lit("Baseline")
                )
                .withColumn(
                    self.type,
                    sqlf.coalesce(sqlf.col("sr."+self.type), sqlf.col("nonsr."+self.type))
                )
                .groupBy("Product", "Customer_Type", self.type)
                .agg(
                    (sqlf.sum(sqlf.col("GrossLineItemQty")) / sqlf.countDistinct(sqlf.col("Id"))).alias(
                        "units_cust"),
                    (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                    (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS")
                )
            )

            base = (
                base
                .withColumn(
                    'units_cust_proportion',
                    sqlf.col('units_cust') / sqlf.sum('units_cust').over(Window.partitionBy("Customer_Type"))
                )
                .withColumn(
                    'total_units_proportion',
                    sqlf.col('units') / sqlf.sum('units').over(Window.partitionBy("Customer_Type"))
                )
                .withColumn(
                    'total_nds_proportion',
                    sqlf.col('NDS') / sqlf.sum('NDS').over(Window.partitionBy("Customer_Type"))
                )
            )
            return result.union(base)

        return result

    def beverage_segmentation(self, product, tp="bev_primary_segment", cohort=None, base=False, title=None):
        return self.__segmentation(self, product, tp=tp, cohort=cohort, base=base, title=title)

    def flavor_segmentation(self, product, tp="flavor_primary_segment", cohort=None, base=False, title=None):
        return self.__segmentation(self, product, tp=tp, cohort=cohort, base=base, title=title)

    def food_segmentation(self, product, tp="food_primary_segment", cohort=None, base=False, title=None):
        return self.__segmentation(self, product, tp=tp, cohort=cohort, base=base, title=title)
