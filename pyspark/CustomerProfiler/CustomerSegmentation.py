from ...libraries import *
from .Customer import *


def beverage_segmentation(spark, product, cohort=None):
    """
        This is a function that returns the beverage segmentation metrics.
        :param spark: spark object
        :param product: dictionary
        :param cohort: customer class
        :return: flavor segmentation spark table

        Examples:
        >>> product = {
        >>>   "Promo_Start_Date": "2019-04-30",
        >>>   "Promo_End_Date": "2019-06-24",
        >>>   "Product_Name": "Flavored Ice Tea and Refreshers",
        >>>   "EPH_level": "NotionalProductlId",
        >>>   "Id": refreshers_summer1_visual + refreshers_summer1_line + ice_tea_summer1_line,
        >>>   "Purchased_Freq_Min": 1,
        >>>   "Purchased_Freq_Max": 999999
        >>> }
        >>> df = beverage_segmentation(spark, promo, cohort)
    """
    pos = (
        spark
        .table("fkwan.pos_line_item")
        .filter(
            sqlf.col("AccountId").isNotNull() &
            sqlf.col("BusinessDate").between(product["Promo_Start_Date"], product["Promo_End_Date"]) &
            sqlf.col(product["EPH_level"]).isin(product["Id"])
        )
    )
    if cohort:
        pos = (
            pos.alias("result")
            .join(
                cohort.pf_spdf.alias("cohort"),
                sqlf.col("result.AccountId") == sqlf.col("cohort.Id"),
                how="inner"
            )
        )

    result = (
        pos
        .alias("pos")
        .join(
            spark
            .table("ttran.customer_product_segments_1y_fy19q2_v2")
            .alias("seg"),
            sqlf.col("pos.AccountId") == sqlf.col("seg.GuidId"),
            how="inner"
        )
        .withColumn(
            "Product",
            sqlf.lit(product["Product_Name"])
        )
        .withColumn(
            "Beverage_Segment",
            sqlf.when(
                sqlf.col("seg.bev_segment") == 0,
                'Moderate Coffee Lover'
            )
            .when(
                sqlf.col("seg.bev_segment") == 1,
                'Indulgent Coffee Drinker'
            )
            .when(
                sqlf.col("seg.bev_segment") == 2,
                'Coffee Head'
            )
            .when(
                sqlf.col("seg.bev_segment") == 3,
                'Sugar Lover'
            )
            .when(
                sqlf.col("seg.bev_segment") == 5,
                'Health Conscious'
            )
            .when(
                sqlf.col("seg.bev_segment") == 6,
                'Fun Coffee Drinker'
            )
            .otherwise(None)
        )
        .groupBy("Product", "Beverage_Segment")
        .agg(
            (sqlf.sum(sqlf.col("GrossLineItemQty"))/sqlf.countDistinct(sqlf.col("AccountId"))).alias("units_cust")
        )
    )

    return result


def flavor_segmentation(spark, product, cohort=None):
    """
        This is a function that returns the flavor segmentation metrics.
        :param spark: spark object
        :param product: dictionary
        :param cohort: spark table
        :return: flavor segmentation spark table

        Examples:
        >>> product = {
        >>>   "Promo_Start_Date": "2019-04-30",
        >>>   "Promo_End_Date": "2019-06-24",
        >>>   "Product_Name": "Flavored Ice Tea and Refreshers",
        >>>   "EPH_level": "NotionalProductlId",
        >>>   "Id": refreshers_summer1_visual + refreshers_summer1_line + ice_tea_summer1_line,
        >>>   "Purchased_Freq_Min": 1,
        >>>   "Purchased_Freq_Max": 999999
        >>> }
        >>> df = beverage_segmentation(spark, promo, cohort)
    """
    pos = (
        spark
        .table("fkwan.pos_line_item")
        .filter(
            sqlf.col("AccountId").isNotNull() &
            sqlf.col("BusinessDate").between(product["Promo_Start_Date"], product["Promo_End_Date"]) &
            sqlf.col(product["EPH_level"]).isin(product["Id"])
        )
    )
    if cohort:
        pos = (
            pos.alias("result")
            .join(
                cohort.alias("cohort"),
                sqlf.col("result.AccountId") == sqlf.col("cohort.Id"),
                how="inner"
            )
        )

    result = (
        pos
        .alias("pos")
        .join(
            spark
            .table("ttran.customer_product_segments_1y_fy19q2_v2")
            .alias("seg"),
            sqlf.col("pos.AccountId") == sqlf.col("seg.GuidId"),
            how="inner"
        )
        .withColumn(
            "Product",
            sqlf.lit(product["Product_Name"])
        )
        .withColumn(
            "Flavor_Segment",
            sqlf.when(
                sqlf.col("seg.flavor_segment") == 0,
                'Matcha'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 1,
                'Caramel'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 2,
                'WhiteChocolateMocha'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 3,
                'Cinnamon'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 4,
                'Chai'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 5,
                'GreenTea'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 6,
                'BlackTea'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 7,
                'Explorer'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 8,
                'Vanilla'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 10,
                'Strawberry'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 11,
                'Mocha'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 12,
                'Fruit'
            )
            .when(
                sqlf.col("seg.flavor_segment") == 16,
                'NoFlavor'
            )
            .otherwise('Other')
        )
        .groupBy("Product", "Flavor_Segment")
        .agg(
            (sqlf.sum(sqlf.col("GrossLineItemQty"))/sqlf.countDistinct(sqlf.col("AccountId"))).alias("units_cust")
        )
    )

    return result
