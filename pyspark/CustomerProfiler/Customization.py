from ...libraries import *
from .Customer import *


def add_customization(spark, df, date_range=None):
    """
        This is a function that append customizations to transaction table.
        :param spark: spark object
        :param df: spark table
        :param date_range: date range. e.g. ("2018-07-02", "2019-06-30")
        :return: beverage segmentation spark table

        Note: The table should have the following columns:
              ["TransactionId", "TransactionLineNumber", "ContainerId", "ContainerChildSequenceNumber", "ItemNumber"]
    """
    if not date_range:
        custom_order = (
            spark
            .table("edap_pub_customersales.customized_orders")
        )
    else:
        custom_order = (
            spark
            .table("edap_pub_customersales.customized_orders")
            .filter(
                sqlf.col("BusinessDate").between(str(Date(date_range[0])-3), str(Date(date_range[1])+3))
            )
        )

    powder_mod = (
        df
        .filter(
            sqlf.col("SellableItemDescription").like("%Powder%") &
            (sqlf.col("ProductTypeDescription") == 'Modifier')
        )
        .select(["SellableItemDescription", "transactionid", "containerid"])
        .distinct()
    )

    drizzle_mod = (
        df
        .filter(
            sqlf.col("SellableItemDescription").like("%Drizzle%") &
            (sqlf.col("ProductTypeDescription") == 'Modifier')
        )
        .select(["SellableItemDescription", "transactionid", "containerid"])
        .distinct()
    )

    df = (
        df.alias("A")
        .join(
            powder_mod.alias("powder"),
            (sqlf.col("A.TransactionId") == sqlf.col("powder.TransactionId")) &
            (sqlf.col("A.containerid") == sqlf.col("powder.containerid")) &
            (sqlf.col("A.containerchildsequencenumber") == 0),
            how="left"
        )
        .join(
            drizzle_mod.alias("drizzle"),
            (sqlf.col("A.TransactionId") == sqlf.col("drizzle.TransactionId")) &
            (sqlf.col("A.containerid") == sqlf.col("drizzle.containerid")) &
            (sqlf.col("A.containerchildsequencenumber") == 0),
            how="left"
        )
        .groupBy(["A." + x for x in df.schema.names])
        .agg(
            sqlf.collect_set(sqlf.col("powder.SellableItemDescription")).alias("powder_modification"),
            sqlf.collect_set(sqlf.col("drizzle.SellableItemDescription")).alias("drizzle_modification")
        )
    )

    df_1 = (
        custom_order.alias("A")
        .join(
            spark.table("edap_pub_customersales.pos_transaction_header").alias("B"),
            sqlf.col("A.SalesTransactionId") == sqlf.col("B.SalesTransactionId"),
            how="left"
        )
        .withColumn(
            "tmp",
            sqlf.explode_outer(
                sqlf.arrays_zip(
                    "A.parentrecipeflavorquantity",
                    "A.beverageflavorquantity",
                    "A.parentrecipesweetnessquantity",
                    "A.beveragesweetnessquantity"
                )
            )
        )
        .withColumn(
            "FlavorModification",
            sqlf.when(sqlf.col("A.ParentRecipeFlavorType") != sqlf.col("A.FingerprintFlavorType"), "Modified Flavor")
                .otherwise("Default Flavor")
        )
        .withColumn(
            "parent_flavor_quantity",
            sqlf.coalesce(sqlf.col("tmp.parentrecipeflavorquantity"), sqlf.lit(0))
        )
        .withColumn(
            "beverage_flavor_quantity",
            sqlf.coalesce(sqlf.col("tmp.beverageflavorquantity"), sqlf.lit(0))
        )
        .withColumn(
            "parent_sweetness_quantity",
            sqlf.coalesce(sqlf.col("tmp.parentrecipesweetnessquantity"), sqlf.lit(0))
        )
        .withColumn(
            "beverage_sweetness_quantity",
            sqlf.coalesce(sqlf.col("tmp.beveragesweetnessquantity"), sqlf.lit(0))
        )
        .withColumn(
            "TransactionLineNumber",
            sqlf.regexp_replace(sqlf.col("A.OrderedProductId"), '^(.*?)\-', '')
        )
        .groupBy(["A.OrderedProductId",
                  "A.SalesTransactionId",
                  "TransactionLineNumber",
                  "B.TranId",
                  "BeverageParentSKUNumber",
                  "FlavorModification",
                  "ParentRecipeMilkType",
                  "FingerprintMilkType",
                  "ParentRecipeWhipType",
                  "FingerprintWhipType"
                ])
        .agg(
            sqlf.sum(sqlf.col("parent_flavor_quantity")).alias("parent_flavor_quantity"),
            sqlf.sum(sqlf.col("beverage_flavor_quantity")).alias("beverage_flavor_quantity"),
            sqlf.sum(sqlf.col("parent_sweetness_quantity")).alias("parent_sweetness_quantity"),
            sqlf.sum(sqlf.col("beverage_sweetness_quantity")).alias("beverage_sweetness_quantity")
        )
    )

    custom_order = (
        df_1
        .withColumn(
            "DefaultFlavorType",
            sqlf.when(sqlf.col("parent_flavor_quantity") == 0, 'Unflavored')
                .otherwise("Flavored")
        )
        .withColumn(
            "FlavorPumpsModification",
            sqlf.when(sqlf.col("beverage_flavor_quantity") > sqlf.col("parent_flavor_quantity"), 'Increase pumps')
                .when((sqlf.col("beverage_flavor_quantity") < sqlf.col("parent_flavor_quantity")) & (
                        sqlf.col("beverage_flavor_quantity") == 0), 'Decrease to Unflavored')
                .when(sqlf.col("beverage_flavor_quantity") < sqlf.col("parent_flavor_quantity"), 'Decrease pumps')
                .otherwise("Default pumps")
        )
        .withColumn(
            "DefaultSugarType",
            sqlf.when(sqlf.col("parent_sweetness_quantity") == 0, 'Sugar Free')
                .otherwise("Sweetness")
        )
        .withColumn(
            "SugarModification",
            sqlf.when(sqlf.col("beverage_sweetness_quantity") > sqlf.col("parent_sweetness_quantity"),
                      'Modified Up Sweetness')
                .when((sqlf.col("beverage_sweetness_quantity") < sqlf.col("parent_sweetness_quantity")) & (
                        sqlf.col("beverage_sweetness_quantity") == 0), 'Modified Down to Sugar Free')
                .when(sqlf.col("beverage_sweetness_quantity") < sqlf.col("parent_sweetness_quantity"),
                      'Modified Down Sweetness')
                .otherwise("Default")
        )
        .withColumn(
            "DefaultMilkType",
            sqlf.coalesce(sqlf.col("ParentRecipeMilkType"), sqlf.lit("No Milk"))
        )
        .withColumn(
            "MilkModification",
            sqlf.when(
                sqlf.col("FingerprintMilkType") != sqlf.col("ParentRecipeMilkType"),
                sqlf.coalesce(sqlf.col("FingerprintMilkType"), sqlf.lit("No Milk"))
            )
            .otherwise("Default")
        )
        .withColumn(
            "DefaultWhipType",
            sqlf.coalesce(sqlf.col("ParentRecipeWhipType"), sqlf.lit("No Whip"))
        )
        .withColumn(
            "WhipModification",
            sqlf.when(
                sqlf.col("FingerprintWhipType") != sqlf.col("ParentRecipeWhipType"),
                sqlf.coalesce(sqlf.col("FingerprintWhipType"), sqlf.lit("No Whip"))
            )
            .otherwise("Default")
        )
        .select(["OrderedProductId",
                 "SalesTransactionId",
                 "TransactionLineNumber",
                 "TranId",
                 "BeverageParentSKUNumber",
                 "DefaultFlavorType",
                 "FlavorModification",
                 "FlavorPumpsModification",
                 "DefaultSugarType",
                 "SugarModification",
                 "DefaultMilkType",
                 "MilkModification",
                 "DefaultWhipType",
                 "WhipModification"
                ])
        .distinct()
    )

    df = (
        df
        .withColumn(
            "TransactionLineNumber_2",
            sqlf.row_number().over(Window.partitionBy("TransactionId", "containerId").orderBy("TransactionId","containerId"))
        )
    )

    df = (
        df.alias("A")
        .join(
            custom_order.alias("custom"),
            (sqlf.col("A.TransactionId") == sqlf.col("custom.TranId")) &
            ((sqlf.col("A.TransactionLineNumber_2")-1) == sqlf.col("custom.TransactionLineNumber")) &
            (sqlf.col("A.ItemNumber") == sqlf.col("custom.BeverageParentSKUNumber")),
            how="left"
        )
        .select(
            ["A." + x for x in df.schema.names] +
            [
                "DefaultFlavorType",
                "FlavorModification",
                "FlavorPumpsModification",
                "DefaultSugarType",
                "SugarModification",
                "DefaultMilkType",
                "MilkModification",
                "DefaultWhipType",
                "WhipModification"
            ]
        )
    )

    return df
