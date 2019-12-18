from matplotlib import pyplot as plt
import pyspark.sql.functions as sqlf
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from pyspark.sql.window import Window
from matplotlib.ticker import FuncFormatter
from lifetimes import BetaGeoFitter, GammaGammaFitter, ModifiedBetaGeoFitter


class ltv_validation(object):
    def __init__(self, spark, customer, obs_tbl, calibration_end, observation_end):
        self.spark = spark
        self.customer = customer
        self.calibration_end = calibration_end
        self.observation_end = observation_end
        self.obs_tbl = obs_tbl
        self.cust_dict = {"SR": "AccountId", "Non-SR": "AmperityId"}

    def validation_test_split(self, start_date, end_date):
        drv = (
            self.spark.table(self.obs_tbl)
            .filter(sqlf.col("BusinessDate").between(start_date, end_date))
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

    def clv_prediction(self, model, time=6.0, monetary_col="AVG_MONETARY_VALUE"):
        t = 52.08 / (12 / time)  # 365 days

        pd_actual_training = self.validation_test_split(
            start_date="2010-01-01", end_date=self.calibration_end
        ).toPandas()

        validation_spdf = self.validation_test_split(
            start_date=self.calibration_end, end_date=self.observation_end
        )

        for col in ["RECENCY", "AGE", monetary_col]:
            pd_actual_training[col] = pd_actual_training[col].astype("float")

        # Fitting beta geo fitter and predicting the frequency and alive probability
        bgf_actual = model
        bgf_actual.fit(
            pd_actual_training["FREQUENCY"],
            pd_actual_training["RECENCY"],
            pd_actual_training["AGE"],
        )

        pd_actual_training[
            "PRED_VISITS"
        ] = bgf_actual.conditional_expected_number_of_purchases_up_to_time(
            t,
            pd_actual_training["FREQUENCY"],
            pd_actual_training["RECENCY"],
            pd_actual_training["AGE"],
        )

        pd_actual_training["PROB_ALIVE"] = bgf_actual.conditional_probability_alive(
            pd_actual_training["FREQUENCY"],
            pd_actual_training["RECENCY"],
            pd_actual_training["AGE"],
        )

        # Fitting gamma gamma fitter and predicting the ltv score
        refined_pd_actual_training = pd_actual_training[
            pd_actual_training["FREQUENCY"] > 1
        ]

        ggf_actual = GammaGammaFitter(
            penalizer_coef=0.0001
        )  # Convergence Errors at .0001
        ggf_actual.fit(
            refined_pd_actual_training["FREQUENCY"],
            refined_pd_actual_training[monetary_col],
        )

        pd_actual_training["PRED_CLV"] = ggf_actual.customer_lifetime_value(
            bgf_actual,
            pd_actual_training["FREQUENCY"],
            pd_actual_training["RECENCY"],
            pd_actual_training["AGE"],
            pd_actual_training[monetary_col],
            freq="W",
            time=time,
            discount_rate=0.0056,
        )

        pd_actual_training[
            "COND_EXP_AVG_PROFT"
        ] = ggf_actual.conditional_expected_average_profit(
            pd_actual_training["FREQUENCY"], pd_actual_training[monetary_col]
        )

        result = self.spark.createDataFrame(pd_actual_training)

        w = Window.partitionBy().orderBy(sqlf.col("PRED_CLV"))
        w2 = Window.partitionBy().orderBy(sqlf.col("result." + monetary_col))

        validation = (
            result.alias("result")
            .join(
                validation_spdf.alias("validation"),
                sqlf.col("result."+self.cust_dict[self.customer]) == sqlf.col("validation."+self.cust_dict[self.customer]),
                how="inner",
            )
            .withColumn(
                "Actual_Monetary",
                sqlf.coalesce(sqlf.col("validation.MONETARY_VALUE"), sqlf.lit(0)),
            )
            .withColumn("PRED_PERCENTILE", sqlf.ntile(100).over(w))
            .withColumn("AVG_MONETARY_PERCENTILE", sqlf.ntile(100).over(w2))
        )

        result = (
            validation.groupBy("AVG_MONETARY_PERCENTILE")
            .agg(
                sqlf.avg(sqlf.col("result.PRED_CLV")).alias("AVG_PRED_CLV"),
                sqlf.avg(sqlf.col("result.COND_EXP_AVG_PROFT")).alias(
                    "AVG_COND_EXP_AVG_PROFT"
                ),
                sqlf.avg(sqlf.col("Actual_Monetary")).alias("AVG_Actual_Monetary"),
                (
                    (
                        sqlf.avg(sqlf.col("result.PRED_CLV"))
                        - sqlf.avg(sqlf.col("Actual_Monetary"))
                    )
                    / sqlf.avg(sqlf.col("Actual_Monetary"))
                ).alias("diff"),
                sqlf.max(sqlf.col("result." + monetary_col)).alias(
                    "MONETARY_PERCENTILE"
                ),
                sqlf.countDistinct(sqlf.col("result."+self.cust_dict[self.customer])).alias("count"),
            )
            .orderBy("MONETARY_PERCENTILE")
        )

        return result


def monetary_percentile_plot(ls, labels, title):
    title = title
    xlabel = "Average Monetary Percentile"
    ylabel_1 = "% Differences"

    def millions(x, pos):
        "The two args are the value and tick position"
        return "%1.1fM" % (x * 1e-6)

    formatter = FuncFormatter(millions)
    fig, ax1 = plt.subplots()

    for x in range(0, len(ls)):
        ax1.plot(ls[x]["AVG_MONETARY_PERCENTILE"], ls[x]["diff"], label=labels[x])

    ax1.plot(ls[x]["AVG_MONETARY_PERCENTILE"], np.zeros(ls[x].shape[0]), ":r")
    ax1.set_ylabel(ylabel_1)
    ax1.set_xlabel(xlabel)
    ax1.legend()
    plt.title(title)

    return fig
