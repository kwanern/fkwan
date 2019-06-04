from ..libraries.__init__ import *
from .udf import concat_string_arrays
from pyspark.sql import SparkSession


spark = spark


class CustomerProfiler(object):
    def __init__(self, products, date_range):
        self.start_dates_pd = [pd.to_datetime(a["Promo_Start_Date"]).date() for a in products.values()]
        self.end_dates_pd = [pd.to_datetime(a["Promo_End_Date"]).date() for a in products.values()]
        self.products_names = [a["Product_Name"] for a in products.values()]
        self.products_id = [a["NotionalProductlId"] for a in products.values()]
        self.pch_frq_min = [a["Purchased_Freq_Min"] for a in products.values()]
        self.pch_frq_max = [a["Purchased_Freq_Max"] for a in products.values()]
        self.date_range = date_range

        # POS
        self.pos = (
            spark
            .table("fkwan.pos_line_item")
            .filter(
                sqlf.col("AccountId").isNotNull() |
                sqlf.col("FirstPaymentToken").isNotNull()
            )
            .filter(sqlf.col("BusinessDate").between(self.date_range[0], self.date_range[1]))
            .withColumn(
                "Customer_Type",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), "SR"
                )
                .otherwise("Token")
            )
            .withColumn(
                "Id",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), sqlf.col("AccountId")
                )
                .otherwise(sqlf.col("FirstPaymentToken"))
            )
            .withColumn(
                "ProductCategoryDescription",
                sqlf.when(
                    sqlf.col("ProductTypeDescription").isin(["Food", "Beverage"]),
                    sqlf.col("ProductCategoryDescription")
                )
                .otherwise("Other")
            )
            .withColumn(
                "ProductStyleDescription",
                sqlf.when(
                    sqlf.col("ProductStyleDescription").isin(["Food", "Beverage"]),
                    sqlf.col("ProductStyleDescription")
                )
                .otherwise("Other")
            )
        )

        # Convert products dict to DataFrame
        self.products_df = pd.DataFrame([(a["Product_Name"], b, a["Purchased_Freq_Min"], a["Purchased_Freq_Max"]) \
                                    for a in products.values() \
                                    for b in a["NotionalProductlId"]
                                    ],
                                   columns=["Product", "Product_Id", "Min", "Max"])
        self.products_spdf = spark.createDataFrame(self.products_df)

    def overlap(self):
        # Purchased Frequency
        pf_spdf = (
            self.pos.alias("pos")
            .filter(
                sqlf.col("BusinessDate").between(str(min(self.start_dates_pd)), str(max(self.end_dates_pd)))
            )
            .join(
                self.products_spdf.alias("ind"),
                sqlf.col("pos.NotionalProductlId") == sqlf.col("ind.Product_Id"),
                how="inner"
            )
            .groupBy("pos.Id", "ind.Product", "ind.Min", "ind.Max")
            .agg(
                sqlf.sum("pos.GrossLineItemQty").alias("Qty")
            )
            .where(sqlf.col("Qty").between(sqlf.col("Min"), sqlf.col("Max")))
        )

        # Indicator
        exprs_ind = [
            (sqlf.max(
                sqlf.when(
                    ((sqlf.col("BusinessDate")
                      .between(str(self.start_dates_pd[i]), str(self.end_dates_pd[i]))) &
                     (sqlf.col("NotionalProductlId").isin(self.products_id[i]))
                     ), self.products_names[i])
                .otherwise(None)))
            .alias(re.sub("\s", "_", self.products_names[i])) for i in range(0, len(self.products_id))]

        ind = (
            pf_spdf.alias("ind")
            .join(
                self.pos.alias("pos"),
                sqlf.col("pos.Id") == sqlf.col("ind.Id"),
                how="left"
            )
            .groupBy("ind.Id")
            .agg(*exprs_ind)
        )

        # Result Table
        grp_var = ([
            "pos.FiscalYearNumber",
            "pos.FiscalPeriodInYearNumber",
            "Customer_Type",
            "pos.Id",
            "Product",
            "ProductCategoryDescription",
            "ProductStyleDescription",
            "NotionalProductDescription",
            "pos.NetDiscountedSalesAmount",
            "pos.TransactionId",
            "pos.NetDiscountedSalesQty"
        ])

        table_overlap = (
            self.pos.alias("pos")
            .join(
                ind.alias("ind"),
                sqlf.col("pos.Id") == sqlf.col("ind.Id"),
                how="inner"
            )
            .withColumn(
                "Product",
                concat_string_arrays(*[re.sub("\s", "_", i) for i in self.products_names])
            )
            .select(grp_var)
            .filter(sqlf.col("Product") != '')
        )

        return table_overlap
