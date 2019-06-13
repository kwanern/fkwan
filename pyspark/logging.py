from datetime import datetime

from ..libraries import *


def log_array(start_time, end_time, description):
    return pd.DataFrame(
        [
            [
                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                description,
                str((end_time - start_time).seconds // 3600) + ' hrs ' + \
                str(((end_time - start_time).seconds // 60) % 60) + ' mins ' + \
                str((end_time - start_time).seconds) + ' secs'
            ]
        ],
        columns=["Time", "Description", "TimeTaken"]
    )


class TimeLog(object):

    def __init__(self, spark, path):
        """
            This is a class that log progress, but in spark that enables delta table streaming.

            :param spark: spark initialization object
            :param path: string
            :return: TimeLog class object

            Examples:
            >>> time_log = TimeLog(spark, path = "/mnt/workspaces/customeranalytics/dev/fkwan/log/delta_log")
        """
        self.current = datetime.now()
        self.log_file = log_array(self.current, datetime.now(), "Start logging")
        self.file_path = path
        self.spark = spark
        self.spark \
            .createDataFrame(self.log_file) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .save(self.file_path)
        self.log = (
            self.spark
                .readStream
                .format("delta")
                .load(self.file_path)
        )
        self.log.createOrReplaceTempView("TimeLog")

    def logging(self, description):
        """
            This method append current streaming log files

            :param description: string

            Examples:
            >>> time_log.logging("Start of function: " func.__name__)
        """
        log_append = log_array(self.current, datetime.now(), description)
        self.current = datetime.now()
        self.log_file = self.log_file.append(log_append)
        self.spark \
            .createDataFrame(log_append) \
            .write \
            .format("delta") \
            .mode("append") \
            .save(self.file_path)

    def streaming(self):
        """
            This method display streaming log file

            Examples:
            >>> time_log.streaming()
        """
        df = self.spark.sql("SELECT * FROM TimeLog")
        return display(df)
