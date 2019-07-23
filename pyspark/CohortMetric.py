from ..libraries.__init__ import *
from .udf import concat_string_arrays, union_all
from ..python.time import days

def engagement(promo, cohort):
    """
        This is a function that returns the cohort metrics such as engagement rate and in cohort percentage.

        :param promo: dictionary
        :param cohort: spark table
        :return: summary text

        Examples:
        >>> promo = {
        >>>   "Promo_Start_Date": "2019-04-30",
        >>>   "Promo_End_Date": "2019-06-24",
        >>>   "Product_Name": "Flavored Ice Tea and Refreshers",
        >>>   "EPH_level": "NotionalProductlId",
        >>>   "Id": refreshers_summer1_visual + refreshers_summer1_line + ice_tea_summer1_line,
        >>>   "Purchased_Freq_Min": 1,
        >>>   "Purchased_Freq_Max": 999999
        >>> }
        >>> cohort = (spark.table("fkwan.Ice_Tea_Refreshers"))
        >>> engagement(promo, cohort)
    """
    promo_spdf = (
        spark
        .table("fkwan.pos_line_item").alias("pos")
        .filter(
            sqlf.col(promo["EPH_level"]).isin(promo["Id"]) &
            sqlf.col("BusinessDate").between(promo["Promo_Start_Date"], promo["Promo_End_Date"])
        )
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
        .groupBy("Id", "Customer_Type")
        .agg(
            sqlf.sum(
                sqlf.col("pos.GrossLineItemQty")
            ).alias("Qty")
        )
        .where(
            sqlf.col("Qty").between(promo["Purchased_Freq_Min"], promo["Purchased_Freq_Max"])
        )
    )

    old_customers = (
        spark
        .table("fkwan.pos_line_item")
        .filter(
            sqlf.col("BusinessDate") < promo["Promo_Start_Date"]
        )
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
        .select("Id", "Customer_Type")
        .distinct()
    )

    promo_customer_total_count = (
        promo_spdf
        .alias("promo")
        .join(
            old_customers.alias("old"),
            sqlf.col("promo.Id") == sqlf.col("old.Id"),
            how="left"
        )
        .groupBy(
            "promo.Customer_Type"
        )
        .agg(
            sqlf.countDistinct(
                sqlf.col("promo.Id")
            ).alias("Total_Count"),
            sqlf.countDistinct(
                sqlf.when(
                    sqlf.col("old.Id").isNull(),
                    sqlf.col("promo.Id")
                )
                    .otherwise(None)
            ).alias("New_Count"),
            sqlf.countDistinct(
                sqlf.when(
                    sqlf.col("old.Id").isNotNull(),
                    sqlf.col("promo.Id")
                )
                    .otherwise(None)
            ).alias("Existing_Count")
        )
    ).toPandas()

    cohort_customer_total_count = (
        cohort.alias("cohort")
        .join(
            promo_spdf.alias("promo"),
            sqlf.col("cohort.Id") == sqlf.col("promo.Id"),
            how="left"
        )
        .groupBy(
            "cohort.Customer_Type"
        )
        .agg(
            sqlf.countDistinct(
                sqlf.col("cohort.Id")
            ).alias("Total_Count"),
            sqlf.countDistinct(
                sqlf.when(
                    sqlf.col("promo.Id").isNull(),
                    sqlf.col("cohort.Id")
                )
                    .otherwise(None)
            ).alias("Not_Engaged_Count"),
            sqlf.countDistinct(
                sqlf.when(
                    sqlf.col("promo.Id").isNotNull(),
                    sqlf.col("cohort.Id")
                )
                    .otherwise(None)
            ).alias("Engaged_Count")
        )
    ).toPandas()

    promo_customer_total_count["New_Percentage"] = pd.Series(
        ["{0:.2f}%".format(val * 100) for val in
         (promo_customer_total_count["New_Count"] / promo_customer_total_count["Total_Count"]).tolist()]
    )
    promo_customer_total_count["Existing_Percentage"] = pd.Series(
        ["{0:.2f}%".format(val * 100) for val in
         (promo_customer_total_count["Existing_Count"] / promo_customer_total_count["Total_Count"]).tolist()]
    )
    cohort_customer_total_count["Engaged_Percentage"] = cohort_customer_total_count["Engaged_Count"] / \
                                                        promo_customer_total_count["Total_Count"]
    cohort_customer_total_count["Not_Engaged_Percentage"] = cohort_customer_total_count["Not_Engaged_Count"] / \
                                                            promo_customer_total_count["Total_Count"]
    promo_customer_total_count["In_Cohort_Percentage"] = pd.Series(
        ["{0:.2f}%".format(val * 100) for val in
         (cohort_customer_total_count["Engaged_Count"] / promo_customer_total_count["Total_Count"]).tolist()]
    )

    promo_text = (
        """Summary for "{0}" Promo
        \n-------------------------
        \n{1}
        \n-------------------------
        \nSummary for Cohort
        \n-------------------------
        \n{2}
        \n-------------------------
        """.format(
            promo["Product_Name"],
            promo_customer_total_count,
            cohort_customer_total_count
        )
    )

    return print(promo_text)

