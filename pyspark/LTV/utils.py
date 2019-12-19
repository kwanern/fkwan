from matplotlib import pyplot as plt
import pyspark.sql.functions as sqlf
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from pyspark.sql.window import Window
from matplotlib.ticker import FuncFormatter
from lifetimes import BetaGeoFitter, GammaGammaFitter, ModifiedBetaGeoFitter
from .ltv import *


class ltv_validation(ltv):
    def __init__(self, spark, customer, obs_tbl, calibration_end, observation_end):
        self.calibration_end = calibration_end
        self.observation_end = observation_end
        self.obs_tbl = obs_tbl
        super().__init__(self, spark, customer)

    def clv_prediction(self, model, time=6.0, monetary_col="AVG_MONETARY_VALUE"):
        t = 52.08 / (12 / time)  # 365 days

        pd_actual_training = self.rfm_data(
            self.obs_tbl, start_date="2010-01-01", end_date=self.calibration_end
        ).toPandas()

        validation_spdf = self.rfm_data(
            self.obs_tbl, start_date=self.calibration_end, end_date=self.observation_end
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
                sqlf.col("result." + self.cust_dict[self.customer])
                == sqlf.col("validation." + self.cust_dict[self.customer]),
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
                sqlf.countDistinct(
                    sqlf.col("result." + self.cust_dict[self.customer])
                ).alias("count"),
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
