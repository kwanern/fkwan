# Databricks Functions

This package includes functions for python and pyspark that are used in databricks.

## Documentation

Link: https://kwanern.github.io/fkwan/

## Dependencies

 - Spark 2.4. 
 - A recent version of pandas. 
 - Python 3.5+.

## Getting Started

`fkwan` is available at the [Github](https://github.com/kwanern/fkwan), for databricks
```
/databricks/python/bin/pip install git+https://github.com/kwanern/fkwan.git
cd /databricks/python3/lib/python3.5/site-packages
git clone https://github.com/kwanern/fkwan.git
```

After installing the packages, you can import the package:
```
import fkwan
```
Customer Profiler Packages:

![Image of Customer Profiler](./img/customer_profiler.png)

## Versions

* [Version 1.3.5] Customer Segmentation - Reformat code, add food segmentation
* [Version 1.3.6] Cohort Metric - Engagement takes customer class
* [Version 1.4.0] LTV - Validation Metrics and Data Pull
* [Version 1.4.1] udf - Add describe spark dataframe
* [Version 1.4.2] LTV - MAPE and MAE functions
* [Version 1.4.3] Naive past X months column

## Authors

* **Fu Ern Kwan** - *Initial work*

