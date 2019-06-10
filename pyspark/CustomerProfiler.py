from ..libraries.__init__ import *
from .udf import concat_string_arrays, union_all


class Customer(object):
    def __init__(self, spark, products):
        """
            This is a class that cohort a group of customers based on their
            purchased frequency of a specific product.

            :param spark: spark initialization object
            :param products: dictionary
            :return: Customer class object

            Examples:
            >>> product = {
            >>>             "Promo_Start_Date": "2019-03-05",
            >>>             "Promo_End_Date": "2019-04-29",
            >>>             "Product_Name": "Caramel Cloud",
            >>>             "EPH_level": "NotionalProductlid",
            >>>             "Id": ["3067"],
            >>>             "Purchased_Freq_Min": 1,
            >>>             "Purchased_Freq_Max": 999999
            >>>           }
            >>> caramel_cloud = Customer(spark, products["Caramel Cloud"])
        """
        self.start_dates_pd = products["Promo_Start_Date"]
        self.end_dates_pd = products["Promo_End_Date"]
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
            .filter(
                (
                    sqlf.col("AccountId").isNotNull() |
                    sqlf.col("FirstPaymentToken").isNotNull()
                ) &
                sqlf.col("BusinessDate").between(self.start_dates_pd, self.end_dates_pd)
            )
            .withColumn(
                "Id",
                sqlf.when(
                    sqlf.col("AccountId").isNotNull(), sqlf.col("AccountId")
                )
                .otherwise(sqlf.col("FirstPaymentToken"))
            )
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
                sqlf.lit(self.products_names)
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
    def __init__(self, spark, customers, date_range, level=["ProductCategoryDescription"]):
        """
            This is a class that combines multiple customer classes.

            :param spark: spark initialization object
            :param customers: Customer class
            :param level: array of levels
            :return: Profiler class object

            Examples:
            >>> cust_prof = cp.Profiler(spark,
            >>>         [
            >>>           caramel_cloud,
            >>>           cinnamon_cloud
            >>>         ],
            >>>         date_range = ("2018-02-26", "2019-05-26"),
            >>>         level = ["ProductCategoryDescription", "ProductStyleDescription"]
            >>> )
        """
        self.pf_spdf = union_all(*[a.pf_spdf for a in customers])
        self.start_dates_pd = [a.start_dates_pd for a in customers]
        self.end_dates_pd = [a.end_dates_pd for a in customers]
        self.products_names = [a.products_names for a in customers]
        self.level = [a.level for a in customers]
        self.products_id = [a.products_id for a in customers]
        self.pch_frq_min = [a.pch_frq_min for a in customers]
        self.pch_frq_max = [a.pch_frq_max for a in customers]
        self.date_range = date_range
        self.spark = spark

        self.var = ([
            "pos.FiscalYearNumber",
            "pos.FiscalPeriodInYearNumber",
            "Customer_Type",
            "pos.Id",
            "Product",
            "pos.NetDiscountedSalesAmount",
            "pos.TransactionId",
            "pos.NetDiscountedSalesQty",
            "pos.sugars",
            "pos.calories",
            "pos.LoyaltyMemberTenureDays"
        ])

        self.var.extend(level)

        # POS
        self.pos = (
            self.spark
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

        # Join Nutrition
        self.pos = (
            self.pos
            .alias("pos")
            .join(
                spark
                .table("ttran.product_nutrition")
                .alias("nutrition"),
                sqlf.col("pos.ItemNumber") == sqlf.col("nutrition.sku"),
                how="left"
            )
            .join(
                spark
                .table("edap_pub_customer.customer360_behavior_restricted")
                .alias("cust"),
                sqlf.col("pos.Id") == sqlf.col("cust.AccountId"),
                how="left"
            )
            .select([
                "pos.*",
                "nutrition.sugars",
                "nutrition.calories",
                "cust.LoyaltyMemberTenureDays"
            ])
        )

    def details(self):
        """
            This method generates customer profile cohort details.

            :param spark: spark initialization object
            :return: spark dataframe

            Examples:
            >>> cust_prof.details()
        """
        cols = ["Details"] + self.products_names
        cols = [re.sub("\s", "_", i) for i in cols]
        fields = [(StructField(field, StringType(), True)) for field in cols]
        schema = StructType(fields)

        df = pd.DataFrame(
            [
                ["Cohort Start Date"] + self.start_dates_pd,
                ["Cohort End Date"] + self.end_dates_pd,
                ["EPH Level"] + self.level,
                ["Min Units Purchased"] + self.pch_frq_min,
                ["Max Units Purchased"] + self.pch_frq_max,
                ["Id"] + self.products_id
            ],
            columns=cols
        )
        return self.spark.createDataFrame(df, schema)

    def overlap(self):
        """
            This method generates customer profile with overlap product segments.

            :return: spark dataframe

            Examples:
            >>> cust_prof_overlap = cust_prof.overlap()
        """
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
            self.pf_spdf
            .alias("ind")
            .join(
                self.pos.alias("pos"),
                sqlf.col("pos.Id") == sqlf.col("ind.Id"),
                how="left"
            )
            .groupBy("ind.Id")
            .agg(*exprs_ind)
        )

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
            .select(self.var)
            .filter(sqlf.col("Product") != '')
        )

        return table_overlap

    def individual(self):
        """
            This method generates customer profile with product segments.

            :return: spark dataframe

            Examples:
            >>> cust_prof_indv = cust_prof.individual()
        """
        ind = (
            self.pf_spdf
            .select(["Id", "Product"])
            .distinct()
        )

        table_a = (
            self.pos.alias("pos")
            .join(
                ind.alias("ind"),
                sqlf.col("pos.Id") == sqlf.col("ind.Id"),
                how="inner"
            )
            .select(self.var)
            .filter(sqlf.col("Product") != '')
        )

        return table_a






