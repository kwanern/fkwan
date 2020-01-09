import datetime
from lifetimes import BetaGeoFitter, GammaGammaFitter, ModifiedBetaGeoFitter
from pyspark.sql.window import Window
import pytz
from pyspark.sql import SparkSession
from matplotlib import pyplot as plt
import pyspark.sql.functions as sqlf
import pickle
import sys
sys.path.insert(0, r'C:\Users\fkwan\OneDrive - Starbucks\PySpark\packages')
import fkwan.custom_pyspark.LTV_package as ltv


# Variables used throughout the script
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# Variables
environment = "prod"
adls_base_path = "/mnt/workspaces/CMA/Reference/PreProd/Teams/data_science/customer-ltv-prod/"
path = r'C:\Users\fkwan\OneDrive - Starbucks\Data Science\LTV'
TODAY = datetime.datetime.now(pytz.timezone("US/Pacific")).date()

# Training Data Pull Dates
training_date = datetime.date(2019, 3, 4) # Monday
training_start_date = str(training_date - datetime.timedelta(weeks=52))
training_end_date = str(training_date - datetime.timedelta(days=1))
further_end_date = '2019-12-29'

# Sampling 20% Variables
seed = 12345
frac = 0.20
replacement = False

# Model Variables
penalizer = 0.001
calibration_end = str(training_date - datetime.timedelta(days=1))
observation_end_3M = str(training_date - datetime.timedelta(days=1) + datetime.timedelta(weeks=3*4))
observation_end_6M = str(training_date - datetime.timedelta(days=1) + datetime.timedelta(weeks=6*4))
observation_end_9M = str(training_date - datetime.timedelta(days=1) + datetime.timedelta(weeks=9*4))
observation_P3M = str(training_date - datetime.timedelta(days=1) - datetime.timedelta(weeks=3*4))
observation_P6M = str(training_date - datetime.timedelta(days=1) - datetime.timedelta(weeks=6*4))
observation_P9M = str(training_date - datetime.timedelta(days=1) - datetime.timedelta(weeks=9*4))
observation_end = "2019-12-01"
sr_obs_tbl_name = "fkwan.SR_obs_subset" 
sr_customer_type='SR'
nonsr_obs_tbl_name = "fkwan.ampId_obs_subset" 
nonsr_customer_type='Non-SR'
customer_type = [sr_customer_type, nonsr_customer_type]

time_dict = {
    observation_end_3M: 3.0,
    observation_end_6M: 6.0,
    observation_end_9M: 9.0
}

cust_dict = {
    sr_customer_type: "fkwan.SR_obs_full",
    nonsr_customer_type: "fkwan.ampId_obs_full"
}

past_obs = {
    observation_end_3M: observation_P3M,
    observation_end_6M: observation_P6M,
    observation_end_9M: observation_P9M
}

model_ls = [BetaGeoFitter(penalizer_coef=penalizer)]
model_name = ["BetaGeoFitter"]

#for cust in list(cust_dict.keys()):
#    for time in list(time_dict.keys()):
#        ltv_validation = ltv.ltv_validation(
#            spark=spark,
#            customer=cust,
#            obs_tbl=cust_dict[cust],
#            calibration_end=calibration_end,
#            observation_end=time
#        )
#
#        BetaGeoFitter_validation_frequency = (
#            ltv_validation.clv_prediction(
#                BetaGeoFitter(penalizer_coef=penalizer), monetary_col="AVG_MONETARY_VALUE", time=time_dict[time]
#            )
#            .collect(groupByName="FREQUENCY")
#        ).toPandas()
#
#        fig,ax = ltv.plot_calibration_purchases_vs_holdout_purchases(
#            BetaGeoFitter_validation_frequency,
#            title = "Actual Purchases in Holdout Period vs Predicted Purchases - 3 months"
#        )
#
#        fig.savefig(path + '\\frequency_'+ cust + '_' + str(int(time_dict[time])) + 'M.png', format = 'png', dpi = 1200)

for cust in list(cust_dict.keys()):
    for time in list(time_dict.keys()):
        BetaGeoFitter_validation_frequency = (
            spark.table("fkwan.frequency_" + cust.replace("-","_") + '_' + str(int(time_dict[time])) + 'M')
        ).toPandas()

        fig,ax = ltv.plot_calibration_purchases_vs_holdout_purchases(
            BetaGeoFitter_validation_frequency,
            title = "{0} - Actual Purchases in Holdout Period vs Predicted Purchases - {1} months".format(cust.replace("-","_"), str(int(time_dict[time])))
        )

        fig.savefig(path + '\\graphs\\frequency_'+ cust.replace("-","_") + '_' + str(int(time_dict[time])) + 'M.png', format = 'png', dpi = 1200)


for cust in list(cust_dict.keys()):
    for time in list(time_dict.keys()):
        for x in range(0, len(model_ls)):
            BetaGeoFitter_validation_clv = (
                spark.table("fkwan.{0}_clv_{1}_{2}M".format(model_name[x], cust.replace("-","_"), str(int(time_dict[time]))))
            ).toPandas()

            naive = (
                spark.table("fkwan.naive_{0}_{1}M".format(cust.replace("-","_"), str(int(time_dict[time]))))
            ).toPandas()

            pkl = "{0}_mape_{1}_{2}M.pkl".format(model_name[x], cust.replace("-","_"), str(int(time_dict[time])))
            f = open(path+"\\variables\\"+pkl, "rb")
            mape = pickle.load(f)

            fig,ax = ltv.monetary_percentile_plot(
                [naive, BetaGeoFitter_validation_clv],
                mape_ls = [0, mape],
                labels = ["Naive", model_name[x]],
                title = "{0} - % Variation CLV - {1} months".format(cust.replace("-","_"), str(int(time_dict[time]))),
                y_col="monetary_diff", 
                y_label="% Variation CLV"
            )

            fig.savefig(path + "\\graphs\\{0}_clv_{1}_{2}".format(model_name[x], cust.replace("-","_"), str(int(time_dict[time]))) + 'M.png', format = 'png', dpi = 1200)

for cust in list(cust_dict.keys()):
    for time in list(time_dict.keys()):
        for x in range(0, len(model_ls)):
            BetaGeoFitter_validation_clv = (
                spark.table("fkwan.{0}_clv_{1}_{2}M".format(model_name[x], cust.replace("-","_"), str(int(time_dict[time]))))
            ).toPandas()

            naive = (
                spark.table("fkwan.naive_{0}_{1}M".format(cust.replace("-","_"), str(int(time_dict[time]))))
            ).toPandas()

            fig, ax1 = plt.subplots()
            ax1.plot(
                BetaGeoFitter_validation_clv["AVG_MONETARY_PERCENTILE"],
                BetaGeoFitter_validation_clv["AVG_PRED_CLV"],
                label="Model",
            )
            ax1.plot(
                BetaGeoFitter_validation_clv["AVG_MONETARY_PERCENTILE"],
                BetaGeoFitter_validation_clv["AVG_Actual_Monetary"],
                label="Actual",
            )
            ax1.plot(
                naive["AVG_MONETARY_PERCENTILE"],
                naive["AVG_Naive_monetary"],
                label="Naive",
            )

            title="{0} - Average LTV by Average Monetary Percentile - {1} months".format(cust.replace("-","_"), str(int(time_dict[time])))
            plt.title(title)
            plt.xlabel("Average Monetary Percentile")
            plt.ylabel("Average LTV")
            plt.legend()

            fig.savefig(path + "\\graphs\\{0}_ltv_{1}_{2}".format(model_name[x], cust.replace("-","_"), str(int(time_dict[time]))) + 'M.png', format = 'png', dpi = 1200)

