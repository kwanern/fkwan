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
        self.result = None
        super().__init__(spark, customer)

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

        self.validation = (
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
            .withColumn(
                "Actual_Frequency",
                sqlf.coalesce(sqlf.col("validation.FREQUENCY"), sqlf.lit(0)),
            )
            .withColumn("PRED_PERCENTILE", sqlf.ntile(100).over(w))
            .withColumn("AVG_MONETARY_PERCENTILE", sqlf.ntile(100).over(w2))
        )

    def collect(self, groupByName="AVG_MONETARY_PERCENTILE"):
        result = (
            self.validation.groupBy(groupByName)
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
                ).alias("monetary_avg_diff"),
                (
                    sqlf.avg(sqlf.col("result.PRED_CLV"))
                    - sqlf.avg(sqlf.col("Actual_Monetary"))
                ).alias("monetary_diff"),
                (
                    (
                        sqlf.avg(sqlf.col("result.PRED_VISITS"))
                        - sqlf.avg(sqlf.col("Actual_Frequency"))
                    )
                    / sqlf.avg(sqlf.col("Actual_Frequency"))
                ).alias("frequency_avg_diff"),
                (
                    sqlf.avg(sqlf.col("result.PRED_VISITS"))
                    - sqlf.avg(sqlf.col("Actual_Frequency"))
                ).alias("frequency_diff"),
                sqlf.max(sqlf.col("result." + monetary_col)).alias(
                    "MONETARY_PERCENTILE"
                ),
                sqlf.countDistinct(
                    sqlf.col("result." + self.cust_dict[self.customer])
                ).alias("count"),
            )
            .orderBy(groupByName)
        )

        return result

    def mean_absolute_percentage_error(self):
        result = (
            self.validation
            .withColumn(
                "CLV_MAPE",
                sqlf.abs(sqlf.col("Actual_Monetary")-sqlf.col("result.PRED_CLV"))/sqlf.col("Actual_Monetary")
            )
            .withColumn(
                "Frequency_MAPE",
                sqlf.abs(sqlf.col("Actual_Frequency")-sqlf.col("result.PRED_VISITS"))/sqlf.col("Actual_Frequency")
            )
            .groupBy()
            .agg(
                sqlf.mean(sqlf.col("CLV_MAPE")).alias("CLV_MAPE"),
                sqlf.mean(sqlf.col("Frequency_MAPE")).alias("Frequency_MAPE")
            )
        )
        return result

def monetary_percentile_plot(ls, mape_ls, labels, title, y_col="monetary_avg_diff", y_label="% Differences"):
    title = title
    xlabel = "Average Monetary Percentile"
    ylabel_1 = y_label

    def millions(x, pos):
        "The two args are the value and tick position"
        return "%1.1fM" % (x * 1e-6)

    formatter = FuncFormatter(millions)
    fig, ax1 = plt.subplots()

    for x in range(0, len(ls)):
        ax1.plot(
            ls[x]["AVG_MONETARY_PERCENTILE"],
            ls[x][y_col],
            label=labels[x],
        )
        txt="MAPE for {0} = {1}".format(labels[x], mape_ls[x])
        plt.figtext(0.5, 0.01*(x), txt, wrap=True, horizontalalignment='left', fontsize=10)

    ax1.plot(ls[x]["AVG_MONETARY_PERCENTILE"], np.zeros(ls[x].shape[0]), ":r")
    ax1.set_ylabel(ylabel_1)
    ax1.set_xlabel(xlabel)
    ax1.legend()
    plt.title(title)

    return fig, ax1


    def plot_calibration_purchases_vs_holdout_purchases(ls, labels):
    x_labels = {
        "frequency_cal": "Purchases in calibration period",
        "recency_cal": "Age of customer at last purchase",
        "T_cal": "Age of customer at the end of calibration period",
        "time_since_last_purchase": "Time since user made last purchase",
    }
    summary = calibration_holdout_matrix.copy()
    duration_holdout = summary.iloc[0]["duration_holdout"]

    summary["model_predictions"] = model.conditional_expected_number_of_purchases_up_to_time(
            duration_holdout, summary["frequency_cal"], summary["recency_cal"], summary["T_cal"])

    if kind == "time_since_last_purchase":
        summary["time_since_last_purchase"] = summary["T_cal"] - summary["recency_cal"]
        ax = (
            summary.groupby(["time_since_last_purchase"])[["frequency_holdout", "model_predictions"]]
            .mean()
            .iloc[:n]
            .plot(**kwargs)
        )
    else:
        ax = summary.groupby(kind)[["frequency_holdout", "model_predictions"]].mean().iloc[:n].plot(**kwargs)

    plt.title("Actual Purchases in Holdout Period vs Predicted Purchases")
    plt.xlabel(x_labels[kind])
    plt.ylabel("Average of Purchases in Holdout Period")
    plt.legend()

    return ax
