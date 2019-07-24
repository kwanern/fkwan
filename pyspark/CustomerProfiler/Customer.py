from ...python.time import days
from ...libraries import *


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
        self.start_dates_pd = pd.to_datetime(products["Promo_Start_Date"]).date()
        self.end_dates_pd = pd.to_datetime(products["Promo_End_Date"]).date()
        self.products_names = products["Product_Name"]
        self.level = products["EPH_level"]
        self.products_id = products["Id"]
        self.pch_frq_min = products["Purchased_Freq_Min"]
        self.pch_frq_max = products["Purchased_Freq_Max"]
        self.spark = spark

        # Join EPH
        self.pos = (
            spark
            .table("fkwan.pos_line_item")
            .alias("pos")
            .filter(
                (
                    sqlf.col("AccountId").isNotNull() |
                    sqlf.col("FirstPaymentToken").isNotNull()
                )
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
            self.pos
            .filter(
                sqlf.col("BusinessDate").between(str(self.start_dates_pd + days(-31)), str(self.end_dates_pd)) &
                sqlf.col(self.level).isin(self.products_id)
            )
            .withColumn(
                "Product",
                sqlf.lit(self.products_names)
            )
            .groupBy("Id", "Product")
            .agg(
                sqlf.sum(
                    sqlf.when(
                        sqlf.col("BusinessDate").between(str(self.start_dates_pd), str(self.end_dates_pd)),
                        sqlf.col("pos.GrossLineItemQty")
                    )
                    .otherwise(0)
                ).alias("Qty"),
                sqlf.countDistinct(
                    sqlf.when(
                        sqlf.col("BusinessDate") \
                            .between(str(self.start_dates_pd + days(-31)), str(self.end_dates_pd + days(-1))),
                        sqlf.col("pos.TransactionId")
                    )
                    .otherwise(None)
                ).alias("P30_Trans_Count")
            )
            .where(
                sqlf.col("Qty").between(self.pch_frq_min, self.pch_frq_max)
            )
        )

        self.pf_spdf = (
            self.pf_spdf
                .withColumn("P30_Trans_Freq",
                            sqlf.when(sqlf.col("P30_Trans_Count").between(1, 2), "Active 30D, 1-2") \
                            .when(sqlf.col("P30_Trans_Count").between(3, 5), "Active 30D, 3-5") \
                            .when(sqlf.col("P30_Trans_Count").between(6, 9), "Active 30D, 6-9") \
                            .when(sqlf.col("P30_Trans_Count") >= 10, "Active 30D, 10+") \
                            .otherwise("0")
                            )
        )

    def indicator(self, ind):
        """
           This is method to add additional indicator column for benchmark.

           :param ind: dictionary

            Examples:
            >>> ind = {
            >>>       "Indicator_Start_Date": "2019-03-31",
            >>>       "Indicator_End_Date": "2019-04-29",
            >>>       "EPH_level": "NotionalProductlid",
            >>>       "Id": ["4017"],
            >>>       "Indicator_Label": ("P1M Bought Refresher", "0")
            >>>       "Indicator_colname": "Indicator"
            >>>      }
            >>> caramel_cloud = Customer(spark, products["Caramel Cloud"]).indicator(ind)
        """

        if "EPH_level" in ind.keys():
            indicator_df = (
                self.pos
                .filter(
                    sqlf.col("BusinessDate").between(ind["Indicator_Start_Date"], ind["Indicator_End_Date"]) &
                    sqlf.col(ind["EPH_level"]).isin(ind["Id"])
                )
                .select(
                    ["Id"]
                )
                .distinct()
            )
        else:
            indicator_df = (
                self.pos
                .filter(
                    sqlf.col("BusinessDate").between(ind["Indicator_Start_Date"], ind["Indicator_End_Date"])
                )
                .select(
                    ["Id"]
                )
                .distinct()
            )

        var = ["A." + i for i in self.pf_spdf.columns] + [ind["Indicator_colname"]]

        self.pf_spdf = (
            self.pf_spdf
            .alias("A")
            .join(
                indicator_df
                .alias("ind"),
                sqlf.col("A.Id") == sqlf.col("ind.Id"),
                how="left"
            )
            .withColumn(
                ind["Indicator_colname"],
                sqlf.when(
                    sqlf.col("ind.Id").isNotNull(),
                    ind["Indicator_Label"][0]
                )
                .otherwise(ind["Indicator_Label"][1])
            )
            .select(var)
        )
