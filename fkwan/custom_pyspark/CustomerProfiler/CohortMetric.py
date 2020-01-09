from ...libraries import *
from .CustomerProfiler import *
from .Customer import *


def engagement(promo, cohort, promo_start_dt):
    """
        This is a function that returns the cohort metrics such as engagement rate and in cohort percentage.

        :param promo: dictionary
        :param cohort: spark table
        :param promo_start_dt: string start_date for new
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
        >>> promo = Customer(spark, promo)
        >>> engagement(promo, cohort, Promo_Start_Date)
    """

    spark = promo.spark

    old_customers = (
        spark
        .table("fkwan.pos_line_item")
        .filter(
            sqlf.col("BusinessDate") < promo_start_dt
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
            "CustomerType",
            sqlf.when(
                sqlf.col("AccountId").isNotNull(), "SR"
            )
            .otherwise("Token")
        )
        .select("Id", "CustomerType")
        .distinct()
    )

    promo_customer_total_count = (
        promo.pf_spdf
        .alias("promo")
        .join(
            old_customers.alias("old"),
            sqlf.col("promo.Id") == sqlf.col("old.Id"),
            how="left"
        )
        .groupBy(
            "promo.CustomerType"
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
        cohort.pf_spdf
        .alias("cohort")
        .join(
            promo.alias("promo"),
            sqlf.col("cohort.Id") == sqlf.col("promo.Id"),
            how="left"
        )
        .groupBy(
            "cohort.CustomerType"
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
    cohort_customer_total_count["Engaged_Percentage"] = pd.Series(
        ["{0:.2f}%".format(val * 100) for val in
         (cohort_customer_total_count["Engaged_Count"] / cohort_customer_total_count["Total_Count"]).tolist()]
    )
    cohort_customer_total_count["Not_Engaged_Percentage"] = pd.Series(
        ["{0:.2f}%".format(val * 100) for val in
         (cohort_customer_total_count["Not_Engaged_Count"] / cohort_customer_total_count["Total_Count"]).tolist()]
    )
    promo_customer_total_count["In_Cohort_Percentage"] = pd.Series(
        ["{0:.2f}%".format(val * 100) for val in
         (cohort_customer_total_count["Engaged_Count"] / promo_customer_total_count["Total_Count"]).tolist()]
    )

    promo_text = (
        """\n-------------------------
        \nSummary for Promo
        \n-------------------------
        \n{0}
        \n-------------------------
        \nSummary for Cohort
        \n-------------------------
        \n{1}
        \n-------------------------
        """.format(
            promo_customer_total_count.set_index('CustomerType').T,
            cohort_customer_total_count.set_index('CustomerType').T
        )
    )

    return print(promo_text)

