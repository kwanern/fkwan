from ...libraries import *
from .Customer import *


class Segmentation(object):
    def __init__(self, spark, df="ttran.customer_product_segments_1y_fy19q2_v2", title=None):
        """
                This is a function that returns the segmentation metrics.
                :param spark: spark object
                :param df: segmentation table
                :param title: Overwrite existing title name
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
                >>> df = Segmentation(spark, promo, cohort, df = "ttran.customer_product_segments_1y_fy19q2_v2")
            """
        self.start_date = None
        self.end_date = None
        self.products_names = None
        self.level = None
        self.products_id = None
        self.pch_frq_min = None
        self.pch_frq_max = None
        self.spark = spark
        self.title = title
        self.df = df

        self.pos = (
            self.spark
            .table("fkwan.pos_line_item")
            .filter(
                sqlf.col("AccountId").isNotNull()
            )
        )

        if self.title:
            self.name = self.title
        else:
            self.name = self.products_names

    def beverage_segmentation(self, product, cohort=None, base=False):
        self.start_date = product["Promo_Start_Date"]
        self.end_date = product["Promo_End_Date"]
        self.products_names = product["Product_Name"]
        self.level = product["EPH_level"]
        self.products_id = product["Id"]
        self.pch_frq_min = product["Purchased_Freq_Min"]
        self.pch_frq_max = product["Purchased_Freq_Max"]

        self.pos = (
            self.pos
            .alias("pos")
            .filter(sqlf.col("BusinessDate").between(self.start_date, self.end_date))
            .join(
                self.spark
                .table(self.df)
                .alias("seg"),
                sqlf.col("pos.AccountId") == sqlf.col("seg.GuidId"),
                how="inner"
            )
            .withColumn(
                "Product",
                sqlf.lit(self.name)
            )
            .withColumn(
                "Beverage_Segment",
                sqlf.when(
                    sqlf.col("seg.bev_segment") == 0,
                    'Classic Craft'
                )
                .when(
                    sqlf.col("seg.bev_segment") == 1,
                    'Indulgent Mocha Drinker'
                )
                .when(
                    sqlf.col("seg.bev_segment") == 2,
                    'Coffee Head'
                )
                .when(
                    sqlf.col("seg.bev_segment") == 3,
                    'Treat Seeker'
                )
                .when(
                    sqlf.col("seg.bev_segment") == 5,
                    'Light and Iced'
                )
                .when(
                    sqlf.col("seg.bev_segment") == 6,
                    'Sweet and Flavorful'
                )
                .otherwise(None)
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
            .groupBy("Product", "Beverage_Segment")
            .agg(
                (sqlf.sum(sqlf.col("GrossLineItemQty")) / sqlf.countDistinct(sqlf.col("AccountId"))).alias(
                    "units_cust"),
                (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS")
            )
        )

        result = (
            result
            .withColumn(
                'units_cust_proportion',
                sqlf.col('units_cust') / sqlf.sum('units_cust').over(Window.partitionBy())
            )
            .withColumn(
                'total_units_proportion',
                sqlf.col('units') / sqlf.sum('units').over(Window.partitionBy())
            )
            .withColumn(
                'total_nds_proportion',
                sqlf.col('NDS') / sqlf.sum('NDS').over(Window.partitionBy())
            )
        )

        if base:
            base = (
                self.pos
                .withColumn(
                    "Product",
                    sqlf.lit("Base")
                )
                .groupBy("Product", "Beverage_Segment")
                .agg(
                    (sqlf.sum(sqlf.col("GrossLineItemQty")) / sqlf.countDistinct(sqlf.col("AccountId"))).alias(
                        "units_cust"),
                    (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                    (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS")
                )
            )

            base = (
                base
                .withColumn(
                    'units_cust_proportion',
                    sqlf.col('units_cust') / sqlf.sum('units_cust').over(Window.partitionBy())
                )
                .withColumn(
                    'total_units_proportion',
                    sqlf.col('units') / sqlf.sum('units').over(Window.partitionBy())
                )
                .withColumn(
                    'total_nds_proportion',
                    sqlf.col('NDS') / sqlf.sum('NDS').over(Window.partitionBy())
                )
            )
            return result.union(base)

        return result

    def flavor_segmentation(self, product, cohort=None, base=False):
        self.start_date = product["Promo_Start_Date"]
        self.end_date = product["Promo_End_Date"]
        self.products_names = product["Product_Name"]
        self.level = product["EPH_level"]
        self.products_id = product["Id"]
        self.pch_frq_min = product["Purchased_Freq_Min"]
        self.pch_frq_max = product["Purchased_Freq_Max"]

        self.pos = (
            self.pos
            .alias("pos")
            .filter(sqlf.col("BusinessDate").between(self.start_date, self.end_date))
            .join(
                self.spark
                .table(self.df)
                .alias("seg"),
                sqlf.col("pos.AccountId") == sqlf.col("seg.GuidId"),
                how="inner"
            )
            .withColumn(
                "Flavor_Segments",
                sqlf.when(
                    sqlf.col("seg.flavor_segment") == 0,
                    'Matcha'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 1,
                    'Caramel'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 2,
                    'WhiteChocolateMocha'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 3,
                    'Cinnamon'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 4,
                    'Chai'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 5,
                    'GreenTea'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 6,
                    'BlackTea'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 7,
                    'Explorer'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 8,
                    'Vanilla'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 10,
                    'Strawberry'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 11,
                    'Mocha'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 12,
                    'Fruit'
                )
                .when(
                    sqlf.col("seg.flavor_segment") == 16,
                    'NoFlavor'
                )
                .otherwise('Other')
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
            .groupBy("Product", "Flavor_Segments")
            .agg(
                (sqlf.sum(sqlf.col("GrossLineItemQty")) / sqlf.countDistinct(sqlf.col("AccountId"))).alias(
                    "units_cust"),
                (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS")
            )
        )

        result = (
            result
            .withColumn(
                'units_cust_proportion',
                sqlf.col('units_cust') / sqlf.sum('units_cust').over(Window.partitionBy())
            )
            .withColumn(
                'total_units_proportion',
                sqlf.col('units') / sqlf.sum('units').over(Window.partitionBy())
            )
            .withColumn(
                'total_nds_proportion',
                sqlf.col('NDS') / sqlf.sum('NDS').over(Window.partitionBy())
            )
        )

        if base:
            base = (
                self.pos
                .withColumn(
                    "Product",
                    sqlf.lit("Base")
                )
                .groupBy("Product", "Flavor_Segments")
                .agg(
                    (sqlf.sum(sqlf.col("GrossLineItemQty")) / sqlf.countDistinct(sqlf.col("AccountId"))).alias(
                        "units_cust"),
                    (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                    (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS")
                )
            )

            base = (
                base
                .withColumn(
                    'units_cust_proportion',
                    sqlf.col('units_cust') / sqlf.sum('units_cust').over(Window.partitionBy())
                )
                .withColumn(
                    'total_units_proportion',
                    sqlf.col('units') / sqlf.sum('units').over(Window.partitionBy())
                )
                .withColumn(
                    'total_nds_proportion',
                    sqlf.col('NDS') / sqlf.sum('NDS').over(Window.partitionBy())
                )
            )
            return result.union(base)

        return result


