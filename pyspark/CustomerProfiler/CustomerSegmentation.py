from ...libraries import *


class Segmentation(object):
    def __init__(self, spark, period="year", date="2019-06-30"):
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
            self.spark.table("fkwan.pos_line_item")
            .filter(
                sqlf.col("AccountId").isNotNull()
                | sqlf.col("FirstPaymentToken").isNotNull()
            )
            .withColumn(
                "Id",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), sqlf.col("AccountId")
                ).otherwise(sqlf.col("FirstPaymentToken")),
            )
            .withColumn(
                "Customer_Type",
                sqlf.when(sqlf.col("AccountId").isNotNull(), "SR").otherwise("Token"),
            )
        )

    def __segmentation(self, tp, product, cohort, base, title, base_filter):
        self.start_date = product["Promo_Start_Date"]
        self.end_date = product["Promo_End_Date"]
        self.products_names = product["Product_Name"]
        self.level = product["EPH_level"]
        self.products_id = product["Id"]
        self.pch_frq_min = product["Purchased_Freq_Min"]
        self.pch_frq_max = product["Purchased_Freq_Max"]
        self.type = tp
        self.result = None

        if title:
            self.name = title
        else:
            self.name = self.products_names

        pos = (
            self.pos.alias("pos")
            .filter(sqlf.col("BusinessDate").between(self.start_date, self.end_date))
            .join(
                self.spark.table("ttran.taste_segments_v3").alias("sr"),
                (
                    (sqlf.col("pos.Id") == sqlf.col("sr.GuidId"))
                    & (sqlf.col("sr.period") == self.period)
                    & (sqlf.col("sr.end_date") == self.seg_date)
                ),
                how="left",
            )
            .join(
                self.spark.table("ttran.taste_segments_non_sr_v3").alias("nonsr"),
                (
                    (sqlf.col("pos.Id") == sqlf.col("nonsr.FirstPaymentToken"))
                    & (sqlf.col("nonsr.period") == self.period)
                    & (sqlf.col("nonsr.end_date") == self.seg_date)
                ),
                how="left",
            )
            .withColumn("Product", sqlf.lit(self.name))
            .withColumn(
                self.type + "s",
                sqlf.coalesce(
                    sqlf.col("sr." + self.type), sqlf.col("nonsr." + self.type)
                ),
            )
            .where(
                sqlf.col("sr.GuidId").isNotNull()
                | sqlf.col("nonsr.FirstPaymentToken").isNotNull()
            )
        )

        if cohort:
            pos = pos.alias("result").join(
                cohort.pf_spdf.alias("cohort"),
                sqlf.col("result.Id") == sqlf.col("cohort.Id"),
                how="inner"
            ).select(["result.*"])

        result = (
            pos.filter(sqlf.col(self.level).isin(self.products_id))
            .groupBy(["Product", "Customer_Type", self.type + "s"])
            .agg(
                (
                    sqlf.sum(sqlf.col("GrossLineItemQty"))
                    / sqlf.countDistinct(sqlf.col("Id"))
                ).alias("units_cust"),
                (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                (sqlf.sum(sqlf.col("NetDiscountedSalesQty"))).alias("Net_units"),
                (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS"),
                (sqlf.sum(sqlf.col("GrossSalesLocalAmount"))).alias("Gross_sales"),
                (sqlf.countDistinct(sqlf.col("Id"))).alias("Customer_Counts"),
            )
        )

        result = (
            result.withColumn(
                "units_cust_proportion",
                sqlf.col("units_cust")
                / sqlf.sum("units_cust").over(Window.partitionBy("Customer_Type")),
            )
            .withColumn(
                "total_units_proportion",
                sqlf.col("units")
                / sqlf.sum("units").over(Window.partitionBy("Customer_Type")),
            )
            .withColumn(
                "total_nds_proportion",
                sqlf.col("NDS")
                / sqlf.sum("NDS").over(Window.partitionBy("Customer_Type")),
            )
            .withColumn(
                "total_cust_proportion",
                sqlf.col("Customer_Counts")
                / sqlf.sum("Customer_Counts").over(Window.partitionBy("Customer_Type")),
            )
        )

        if base:
            pos = (
            self.pos.alias("pos")
            .filter(sqlf.col("BusinessDate").between(self.start_date, self.end_date))
            .join(
                self.spark.table("ttran.taste_segments_v3").alias("sr"),
                (
                    (sqlf.col("pos.Id") == sqlf.col("sr.GuidId"))
                    & (sqlf.col("sr.period") == self.period)
                    & (sqlf.col("sr.end_date") == self.seg_date)
                ),
                how="left",
            )
            .join(
                self.spark.table("ttran.taste_segments_non_sr_v3").alias("nonsr"),
                (
                    (sqlf.col("pos.Id") == sqlf.col("nonsr.FirstPaymentToken"))
                    & (sqlf.col("nonsr.period") == self.period)
                    & (sqlf.col("nonsr.end_date") == self.seg_date)
                ),
                how="left",
            )
            .withColumn("Product", sqlf.lit(self.name))
            .withColumn(
                self.type + "s",
                sqlf.coalesce(
                    sqlf.col("sr." + self.type), sqlf.col("nonsr." + self.type)
                ),
            )
            .where(
                sqlf.col("sr.GuidId").isNotNull()
                | sqlf.col("nonsr.FirstPaymentToken").isNotNull()
            )
        )
            base = (
                pos.filter(sqlf.col("ProductTypeDescription") == base_filter)
                .withColumn("Product", sqlf.lit("Baseline"))
                .groupBy(["Product", "Customer_Type", self.type + "s"])
                .agg(
                    (
                        sqlf.sum(sqlf.col("GrossLineItemQty"))
                        / sqlf.countDistinct(sqlf.col("Id"))
                    ).alias("units_cust"),
                    (sqlf.sum(sqlf.col("GrossLineItemQty"))).alias("units"),
                    (sqlf.sum(sqlf.col("NetDiscountedSalesQty"))).alias("Net_units"),
                    (sqlf.sum(sqlf.col("NetDiscountedSalesAmount"))).alias("NDS"),
                    (sqlf.sum(sqlf.col("GrossSalesLocalAmount"))).alias("Gross_sales"),
                    (sqlf.countDistinct(sqlf.col("Id"))).alias("Customer_Counts"),
                )
            )

            base = (
                base.withColumn(
                    "units_cust_proportion",
                    sqlf.col("units_cust")
                    / sqlf.sum("units_cust").over(Window.partitionBy("Customer_Type")),
                )
                .withColumn(
                    "total_units_proportion",
                    sqlf.col("units")
                    / sqlf.sum("units").over(Window.partitionBy("Customer_Type")),
                )
                .withColumn(
                    "total_nds_proportion",
                    sqlf.col("NDS")
                    / sqlf.sum("NDS").over(Window.partitionBy("Customer_Type")),
                )
                .withColumn(
                    "total_cust_proportion",
                    sqlf.col("Customer_Counts")
                    / sqlf.sum("Customer_Counts").over(
                        Window.partitionBy("Customer_Type")
                    ),
                )
            )
            return result.union(base)

        return result

    def beverage_segmentation(
        self, product, tp="bev_primary_segment", cohort=None, base=False, title=None
    ):
        return self.__segmentation(
            product=product,
            tp=tp,
            cohort=cohort,
            base=base,
            title=title,
            base_filter="Beverage",
        )

    def flavor_segmentation(
        self, product, tp="flavor_primary_segment", cohort=None, base=False, title=None
    ):
        return self.__segmentation(
            product=product,
            tp=tp,
            cohort=cohort,
            base=base,
            title=title,
            base_filter="Beverage",
        )

    def food_segmentation(
        self, product, tp="food_primary_segment", cohort=None, base=False, title=None
    ):
        return self.__segmentation(
            product=product,
            tp=tp,
            cohort=cohort,
            base=base,
            title=title,
            base_filter="Food",
        )


def add_benchmark(result, benchmark="Baseline"):
    tp = ["bev_primary_segments", "flavor_primary_segments", "food_primary_segments"]
    tp = str(list(set(tp) & set(result.columns))[0])
    var = ["A." + x for x in result.columns] + [
        "benchmark_units_cust_proportion",
        "benchmark_total_units_proportion",
        "benchmark_total_nds_proportion",
        "benchmark_total_cust_proportion",
    ]

    baseline = result.filter(sqlf.col("Product").isin(benchmark)).distinct()

    result = (
        result.alias("A")
        .join(
            baseline.alias("B"),
            (sqlf.col("A.Customer_Type") == sqlf.col("B.Customer_Type"))
            & (sqlf.col("A." + tp) == sqlf.col("B." + tp)),
            how="left",
        )
        .withColumn(
            "benchmark_units_cust_proportion", sqlf.col("B.units_cust_proportion")
        )
        .withColumn(
            "benchmark_total_units_proportion", sqlf.col("B.total_units_proportion")
        )
        .withColumn(
            "benchmark_total_nds_proportion", sqlf.col("B.total_nds_proportion")
        )
        .withColumn(
            "benchmark_total_cust_proportion", sqlf.col("B.total_cust_proportion")
        )
        .select(var)
    )
    return result
