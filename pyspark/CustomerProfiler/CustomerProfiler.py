from ..udf import concat_string_arrays, union_all
from ...libraries import *


class Profiler(object):
    def __init__(self, spark, customers, date_range, indicator=[], granularity="Period"):
        """
            This is a class that combines multiple customer classes.

            :param spark: spark initialization object
            :param customers: Customer class
            :param indicator: Boolean
            :param granularity: string ("Period", "Week", "Day")
            :return: Profiler class object

            Examples:
            >>> cust_prof = cp.Profiler(spark,
            >>>         [
            >>>           caramel_cloud,
            >>>           cinnamon_cloud
            >>>         ],
            >>>         date_range = ("2018-02-26", "2019-05-26")
            >>> )
        """
        self.pf_spdf = union_all(*[a.pf_spdf for a in customers])
        self.start_dates_pd = [str(a.start_dates_pd) for a in customers]
        self.end_dates_pd = [str(a.end_dates_pd) for a in customers]
        self.products_names = [a.products_names for a in customers]
        self.level = [a.level for a in customers]
        self.products_id = [a.products_id for a in customers]
        self.pch_frq_min = [a.pch_frq_min for a in customers]
        self.pch_frq_max = [a.pch_frq_max for a in customers]
        self.date_range = date_range
        self.spark = spark
        self.indicator = indicator

        if granularity == "Period":
            date_granularity = ["pos.FiscalYearNumber", "pos.FiscalPeriodInYearNumber"]
        elif granularity == "Week":
            date_granularity = ["pos.FiscalYearNumber", "pos.FiscalPeriodInYearNumber", "pos.FiscalWeekInYearNumber"]
        else:
            date_granularity = [
                "pos.FiscalYearNumber",
                "pos.FiscalPeriodInYearNumber",
                "pos.FiscalWeekInYearNumber",
                "pos.BusinessDate"
            ]

        self.var = (
            date_granularity +
            [
                "Customer_Type",
                "pos.Id",
                "P30_Trans_Freq",
                "Product",
                "ProductTypeDescription",
                "ProductTypeId",
                "ProductCategoryDescription",
                "ProductCategoryId",
                "ProductStyleDescription",
                "ProductStyleId",
                "NotionalProductDescription",
                "NotionalProductlId",
                "pos.NetDiscountedSalesAmount",
                "pos.TransactionId",
                "pos.GrossLineItemQty",
                "pos.NetDiscountedSalesQty",
                "pos.sugars",
                "pos.calories",
                "pos.LoyaltyMemberTenureDays"
             ]
        )
        self.grp_var = ["ind.Id", "ind.P30_Trans_Freq"]

        if self.indicator:
            self.var.extend(["ind." + i for i in self.indicator])
            self.grp_var.extend(["ind." + i for i in self.indicator])

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
            .groupBy(self.grp_var)
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
            .alias("ind")
            .select(self.grp_var + ["Product"])
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

    def proportion(self, rng=[(1/3), (2/3)]):
        """
        This method generates customer profile with dominant proportion.

        :param rng: array with cutoffs
        :return: spark dataframe

        Examples:
        >>> cust_prof_proportion = cust_prof.proportion(rng=[(1/3), (2/3)])
        """
        if len(self.products_names) != 2:
            return print("This function can only accept Profiler with 2 customers.")

        self.var.remove("Product")

        proportion = (
            self.pf_spdf.alias("ind")
            .join(
                (
                    self.pf_spdf
                    .filter(sqlf.col("Product") == self.products_names[0])
                )
                .alias("A"),
                sqlf.col("ind.Id") == sqlf.col("A.Id"),
                how="left"
            )
            .join(
                (
                    self.pf_spdf
                    .filter(sqlf.col("Product") == self.products_names[1])
                )
                .alias("B"),
                sqlf.col("ind.Id") == sqlf.col("B.Id"),
                how="left"
            )
            .withColumn(
                "Proportion",
                (sqlf.coalesce(sqlf.col("A.Qty"), sqlf.lit(0.0))/
                (sqlf.coalesce(sqlf.col("B.Qty"), sqlf.lit(0.0))+sqlf.coalesce(sqlf.col("A.Qty"), sqlf.lit(0.0))))
            )
            .select(self.grp_var + ["Proportion"])
            .distinct()
            .where(sqlf.col("Proportion").isNotNull())
        )

        cohort = (
            proportion
            .withColumn(
                "Cohort",
                sqlf.when(
                    sqlf.col("Proportion") <= rng[0],
                    self.products_names[1] + " Dominant"
                )
                .when(
                    sqlf.col("Proportion") >= rng[1],
                    self.products_names[0] + " Dominant"
                )
                .otherwise("Not Dominant")
            )
            .select(self.grp_var + ["Proportion", "Cohort"])
            .distinct()
        )

        result = (
            self.pos.alias("pos")
            .join(
                cohort.alias("ind"),
                sqlf.col("pos.Id") == sqlf.col("ind.Id"),
                how="inner"
            )
            .withColumn(
                "Cohort_2",
                sqlf.when(
                    sqlf.col("ind.Proportion") == 1,
                    self.products_names[0] + " only"
                )
                .when(
                    sqlf.col("ind.Proportion") == 0,
                    self.products_names[0] + " only"
                )
                .otherwise(sqlf.col("ind.Cohort"))
            )
            .select(self.var + ["Proportion", "Cohort", "Cohort_2"])
        )

        return result






