from libraries import *
from pyspark.udf import concat_string_arrays


class CustomerProfiler(object):
    def __init__(self, spark, products, date_range):
        self.start_dates_pd = [pd.to_datetime(a["Promo_Start_Date"]).date() for a in products.values()]
        self.end_dates_pd = [pd.to_datetime(a["Promo_End_Date"]).date() for a in products.values()]
        self.products_names = [a["Product_Name"] for a in products.values()]
        self.level = [a["EPH_level"] for a in products.values()][0]
        self.products_id = [a["Id"] for a in products.values()]
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
                    sqlf.col("ProductTypeDescription").isin(["Food", "Beverage"]),
                    sqlf.col("ProductStyleDescription")
                )
                .otherwise("Other")
            )
        )

        # Join EPH
        self.pos = (
            self.pos
            .alias("pos")
            .join(
                spark
                .table("edap_pub_productitem.enterprise_product_hierarchy")
                .alias("EPH"),
                sqlf.col("pos.ItemNumber") == sqlf.col("EPH.ItemId"),
                how="inner"
            )
            .select([
                "pos.*",
                "EPH.MarketedProductDescription",
                "EPH.MarketedProductId"
            ])
        )

        # Convert products dict to DataFrame
        self.products_df = pd.DataFrame([(a["Product_Name"], b, a["Purchased_Freq_Min"], a["Purchased_Freq_Max"])
                                        for a in products.values()
                                        for b in a["Id"]
                                        ],
                                        columns=["Product", "Product_Id", "Min", "Max"])
        self.products_spdf = spark.createDataFrame(self.products_df)

        # Purchased Frequency
        self.pf_spdf = (
            self.pos.alias("pos")
            .filter(
                sqlf.col("BusinessDate").between(str(min(self.start_dates_pd)), str(max(self.end_dates_pd)))
            )
            .join(
                self.products_spdf.alias("ind"),
                sqlf.col("pos." + self.level) == sqlf.col("ind.Product_Id"),
                how="inner"
            )
            .groupBy("pos.Id", "ind.Product", "ind.Min", "ind.Max")
            .agg(
                sqlf.sum("pos.GrossLineItemQty").alias("Qty")
            )
            .where(sqlf.col("Qty").between(sqlf.col("Min"), sqlf.col("Max")))
        )

    def overlap(self):
        # Indicator
        exprs_ind = [
            (sqlf.max(
                sqlf.when(
                    ((sqlf.col("BusinessDate")
                      .between(str(self.start_dates_pd[i]), str(self.end_dates_pd[i]))) &
                     (sqlf.col(self.level[i]).isin(self.products_id[i]))
                     ), self.products_names[i])
                .otherwise(None)))
            .alias(re.sub("\s", "_", self.products_names[i])) for i in range(0, len(self.products_id))]

        ind = (
            self.pf_spdf.alias("ind")
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

    def individual(self):
        ind = (
            self.pf_spdf
            .select(["Id", "Product"])
            .distinct()
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

        table_a = (
            self.pos.alias("pos")
            .join(
                ind.alias("ind"),
                sqlf.col("pos.Id") == sqlf.col("ind.Id"),
                how="inner"
            )
            .select(grp_var)
            .filter(sqlf.col("Product") != '')
        )

        return table_a