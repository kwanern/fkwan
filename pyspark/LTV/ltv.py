import pyspark.sql.functions as sqlf
from datetime import date, datetime, timedelta


class ltv(object):
    def __init__(self, spark, customer):
        """
        Parameters
        ----------
        spark: spark
            spark object
        customer: string
            "SR" or "Non-SR".
        """
        self.spark = spark
        self.customer = customer
        self.cust_dict = {"SR": "AccountId", "Non-SR": "AmperityId"}

    def data_pull(self, date, environment, adls_base_path):
        """
        Parameters
        ----------
        Date: datetime
            Today's date.
        environment: string
            folder name
        adls_base_path
            path towards the ADLS
        Returns
        -------
        Spark DataFrame
            With customer_ids and the following columns:
            'Frequency', 'Recency', 'Age', 'Monetary_Value', 'Avg_Monetary_Value', 'Max_BusinessDate', 'Min_BusinessDate'
        """
        self.RunDate = str(date)
        self.RunStartDate = str(date - timedelta(days=1))
        self.RunEndDate = str(date - timedelta(daweeksys=52))
        self.prod_hierarchy = (
            self.spark.table("edap_pub_productitem.enterprise_product_hierarchy")
            .alias("d1")
            .join(
                self.spark.table(
                    "edap_pub_productitem.legacy_item_profile_retail_auxiliary"
                ).alias("d2"),
                sql.col("d1.ItemId") == sqlf.col("d2.ItemNumber"),
                how="left",
            )
            .withColumn(
                "ProductGroupings",
                sqlf.when(
                    (sqlf.col("ProductTypeDescription") == "Food")
                    & (sqlf.col("ProductCategoryDescription") == "Bakery"),
                    "Food-Bakery",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Food")
                    & (sqlf.col("ProductCategoryDescription") == "Breakfast"),
                    "Food-Breakfast",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Food")
                    & (sqlf.col("ProductCategoryDescription") == "Lunch"),
                    "Food-Lunch",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Frappuccino"),
                    "Beverage-Frappuccino",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (
                        sqlf.col("ProductCategoryDescription").isin(
                            "Milk Beverage",
                            "Cocoa",
                            "Alcohol",
                            "Beverage Flight",
                            "Cocktail",
                        )
                    ),
                    "Beverage-Other",
                )
                .when(
                    (
                        (sqlf.col("ProductTypeDescription") == "Beverage")
                        & (sqlf.col("ProductCategoryDescription") == "Ready-to-Drink")
                    )
                    | (
                        (sqlf.col("ProductTypeDescription") == "Food")
                        & (sqlf.col("ProductCategoryDescription") == "Snack")
                    ),
                    "ReadyToDrinkAndSnack",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Refreshment"),
                    "Beverage-Refreshment",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Tea")
                    & (sqlf.col("ColdBeverageInd") != 1),
                    "Beverage-TeaHot",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Tea")
                    & (sqlf.col("ColdBeverageInd") == 1),
                    "Beverage-TeaIced",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Espresso")
                    & (sqlf.col("ColdBeverageInd") != 1),
                    "Beverage-EspressoHot",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Espresso")
                    & (sqlf.col("ColdBeverageInd") == 1),
                    "Beverage-EspressoCold",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Coffee")
                    & (sqlf.col("ColdBeverageInd") != 1),
                    "Beverage-CoffeeHot",
                )
                .when(
                    (sqlf.col("ProductTypeDescription") == "Beverage")
                    & (sqlf.col("ProductCategoryDescription") == "Coffee")
                    & (sqlf.col("ColdBeverageInd") == 1),
                    "Beverage-CoffeeCold",
                )
                .otherwise("AtHomeAndLobbyAndMisc"),
            )
            .select(["ItemNumber", "ProductGroupings"])
            .orderBy("ProductGroupings")
        )

        b_cogs = self.spark.read.csv(
            adls_base_path + environment + "/static_info/beverage_COGS_active.csv",
            header="true",
        ).where(col("COGSPercent") != "#DIV/0!")

        self.trasactions = (
            self.spark.table("fkwan.pos_line_item")
            .alias("a")
            .join(
                b_cogs.alias("b"),
                sqlf.col("a.ItemNumber") == sqlf.col("b.ItemNumber"),
                how="left",
            )
            .join(
                prod_hierarchy.alias("p"),
                sqlf.col("a.ItemNumber") == sqlf.col("p.ItemNumber"),
                how="left",
            )
            .filter(sqlf.col("BusinessDate").between(RunStartDate, RunEndDate))
            .withColumn(
                "NETDISCOUNTEDSALESAMOUNT_REFINED",
                sqlf.when(
                    sqlf.col("p.ProductGroupings").like("Beverage%")
                    & sqlf.col("b.NetProductMarginPercent").isNotNull(),
                    sqlf.col("a.NetDiscountedSalesAmount")
                    - (
                        (
                            sqlf.when(
                                sqlf.col("GrossSalesLocalAmount") > 200, 200
                            ).otherwise(sqlf.col("GrossSalesLocalAmount"))
                        )
                        * sqlf.col("b.COGSPercent")
                    ),
                )
                .when(
                    sqlf.col("p.ProductGroupings").like("Beverage%")
                    & sqlf.col("b.NetProductMarginPercent").isNull(),
                    sqlf.col("a.NetDiscountedSalesAmount")
                    - (
                        (
                            sqlf.when(
                                sqlf.col("GrossSalesLocalAmount") > 200, 200
                            ).otherwise(sqlf.col("GrossSalesLocalAmount"))
                        )
                        * sqlf.lit(1 - 0.735)
                    ),
                )
                .when(
                    sqlf.col("p.ProductGroupings").like("Food%")
                    & sqlf.col("b.NetProductMarginPercent").isNull(),
                    sqlf.col("a.NetDiscountedSalesAmount")
                    - (
                        (
                            sqlf.when(
                                sqlf.col("GrossSalesLocalAmount") > 200, 200
                            ).otherwise(sqlf.col("GrossSalesLocalAmount"))
                        )
                        * sqlf.lit(1 - 0.33)
                    ),
                )
                .when(
                    sqlf.col("p.ProductGroupings").like("AtHomeAndLobbyAndMisc%"),
                    sqlf.col("a.NetDiscountedSalesAmount")
                    - (
                        (
                            sqlf.when(
                                sqlf.col("GrossSalesLocalAmount") > 200, 200
                            ).otherwise(sqlf.col("GrossSalesLocalAmount"))
                        )
                        * sqlf.lit(1 - 0.44)
                    ),
                )
                .when(
                    sqlf.col("p.ProductGroupings").like("ReadyToDrinkAndSnack%"),
                    sqlf.col("a.NetDiscountedSalesAmount")
                    - (
                        (
                            sqlf.when(
                                sqlf.col("GrossSalesLocalAmount") > 200, 200
                            ).otherwise(sqlf.col("GrossSalesLocalAmount"))
                        )
                        * sqlf.lit(1 - 0.56)
                    ),
                )
                .otherwise(
                    sqlf.col("a.NetDiscountedSalesAmount")
                    - (
                        (
                            sqlf.when(
                                sqlf.col("GrossSalesLocalAmount") > 200, 200
                            ).otherwise(sqlf.col("GrossSalesLocalAmount"))
                        )
                        * sqlf.lit(1 - 0.50)
                    )
                ),
            )
        )

        if self.customer == "SR":
            self.trasactions = self.trasactions.filter(
                sqlf.col(cust_dict[customer]).isNotNull()
                & (sqlf.col("LoyaltyProgramName") == "MSR_USA")
            ).select(
                [
                    self.cust_dict[self.customer],
                    "BusinessDate",
                    "NETDISCOUNTEDSALESAMOUNT_REFINED",
                ]
            )

        elif self.customer == "Non-SR":
            identity = (
                spark.table("cdl_prod_publish.nucleus_crosswalk")
                .withColumn(
                    "FirstPaymentToken",
                    sqlf.explode("PaymentTokenWithScore.PaymentToken"),
                )
                .filter(sqlf.col("ExternalUserId2AccountId").isNull())
            )
            self.trasactions = (
                self.trasactions.alias("t")
                .join(
                    identity.alias("id"),
                    sqlf.col("t.FirstPaymentToken") == sqlf.col("id.FirstPaymentToken"),
                    how="inner",
                )
                .filter(sqlf.col(cust_dict[customer]).isNotNull())
                .select(
                    [
                        self.cust_dict[self.customer],
                        "BusinessDate",
                        "NETDISCOUNTEDSALESAMOUNT_REFINED",
                    ]
                )
            )
        return self.rfm_data(self.trasactions, self.RunStartDate, self.RunEndDate)

    def rfm_data(self, obs_tbl, start_date, end_date):
        if type(obs_tbl) == str:
            bs_tbl = self.spark.table(obs_tbl)
        else:
            obs_tbl = obs_tbl

        drv = (
            obs_tbl.filter(sqlf.col("BusinessDate").between(start_date, end_date))
            .groupBy(self.cust_dict[self.customer])
            .agg(
                sqlf.countDistinct(sqlf.weekofyear(sqlf.col("BusinessDate"))).alias(
                    "FREQUENCY"
                ),
                (
                    sqlf.round(
                        sqlf.datediff(
                            sqlf.max(sqlf.col("BusinessDate")),
                            sqlf.min(sqlf.col("BusinessDate")),
                        )
                        / 7,
                        2,
                    )
                    * sqlf.lit(1.0)
                ).alias("RECENCY"),
                (
                    sqlf.round(
                        sqlf.datediff(
                            sqlf.to_date(sqlf.lit(end_date)),
                            sqlf.min(sqlf.col("BusinessDate")),
                        )
                        / 7,
                        2,
                    )
                    * sqlf.lit(1.0)
                ).alias("AGE"),
                sqlf.round(
                    sqlf.sum(sqlf.col("NETDISCOUNTEDSALESAMOUNT_REFINED")), 2
                ).alias("MONETARY_VALUE"),
                sqlf.max(sqlf.col("BusinessDate")).alias("MAX_BUSINESSDATE"),
                sqlf.min(sqlf.col("BusinessDate")).alias("MIN_BUSINESSDATE"),
            )
            .where(sqlf.col("MONETARY_VALUE") > 0)
        )

        rfm_actual_training = (
            drv.withColumn(
                "RECENCY",
                sqlf.when(sqlf.col("FREQUENCY") == 0, 0).otherwise(sqlf.col("RECENCY")),
            )
            .withColumn(
                "AVG_MONETARY_VALUE",
                sqlf.coalesce(
                    sqlf.col("MONETARY_VALUE") / sqlf.col("FREQUENCY"), sqlf.lit(0)
                ),
            )
            .select(
                [
                    self.cust_dict[self.customer],
                    "FREQUENCY",
                    "RECENCY",
                    "AGE",
                    "MONETARY_VALUE",
                    "AVG_MONETARY_VALUE",
                    "MAX_BUSINESSDATE",
                    "MIN_BUSINESSDATE",
                ]
            )
        )

        return rfm_actual_training
