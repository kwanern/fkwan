from ..libraries.__init__ import *
from .udf import concat_string_arrays, union_all


class Customer(object):
    def __init__(self, spark, products):
        self.start_dates_pd = pd.to_datetime(products["Promo_Start_Date"]).date()
        self.end_dates_pd = pd.to_datetime(products["Promo_End_Date"]).date()
        self.products_names = products["Product_Name"]
        self.level = products["EPH_level"]
        self.products_id = products["Id"]
        self.pch_frq_min = products["Purchased_Freq_Min"]
        self.pch_frq_max = products["Purchased_Freq_Max"]

        # Join EPH
        pos = (
            spark
            .table("fkwan.pos_line_item")
            .alias("pos")
            .join(
                spark
                .table("edap_pub_productitem.enterprise_product_hierarchy")
                .alias("EPH"),
                sqlf.col("pos.ItemNumber") == sqlf.col("EPH.ItemId"),
                how="inner"
            )
            .filter(
                (
                    sqlf.col("AccountId").isNotNull() |
                    sqlf.col("FirstPaymentToken").isNotNull()
                ) &
                sqlf.col("BusinessDate").between(str(self.start_dates_pd), str(self.end_dates_pd))
            )
            .withColumn(
                "Id",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), sqlf.col("AccountId")
                )
                .otherwise(sqlf.col("FirstPaymentToken"))
            )
            .select([
                "pos.*",
                "Id",
                "EPH.MarketedProductDescription",
                "EPH.MarketedProductId"
            ])
        )

        # Select Cohort
        self.pf_spdf = (
            pos
            .filter(
                sqlf.col("BusinessDate").between(str(min(self.start_dates_pd)), str(max(self.end_dates_pd))) &
                sqlf.col(self.level).isin(self.products_id)
            )
            .withColumn(
                "Product",
                self.products_names
            )
            .groupBy("Id", "Product")
            .agg(
                sqlf.sum("pos.GrossLineItemQty").alias("Qty")
            )
            .where(
                sqlf.col("Qty").between(self.pch_frq_min, self.pch_frq_max)
            )
        )


class Profiler(object):
    def __init__(self, customers, date_range):
        self.pf_spdf = union_all(*customers)
        self.start_dates_pd = [a for a in customers.start_dates_pd]
        self.end_dates_pd = [a for a in customers.end_dates_pd]
        self.products_names = [a for a in customers.product_names]
        self.level = [a for a in customers.level]
        self.products_id = [a for a in customers.products_id]
        self.pch_frq_min = [a for a in customers.pch_frq_min]
        self.pch_frq_max = [a for a in customers.pch_frq_max]
        self.date_range = date_range

    def overlap(self):
        # Indicator
        exprs_ind = [
            (sqlf.max(
                sqlf.when(
                    ((sqlf.col("BusinessDate")
                      .between(str(self.start_dates_pd[i]), str(self.end_dates_pd[i]))) &
                     (sqlf.col(self.level).isin(self.products_id[i]))
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






