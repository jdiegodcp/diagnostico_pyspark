import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession, parameter: int):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df.printSchema()
        # method 5
        df = self.method5(df, parameter)
        df.show(n=100, truncate=False)
        # method 1
        df = self.method1(df)
        df.show(n=100, truncate=False)
        # method 2
        df = self.method2(df)
        df.show(n=100, truncate=False)
        # method 3
        df = self.method3(df)
        df.show(n=100, truncate=False)
        # method 4
        df = self.method4(df)
        df.show(n=100, truncate=False)
        # Generating files required in exercise 6
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

    def method1(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            long_name.column(),
            age.column(),
            height_cm.column(),
            weight_kg.column(),
            nationality.column(),
            club_name.column(),
            overall.column(),
            potential.column(),
            team_position.column()
        )
        return df

    def method2(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        column_list = ["nationality", "team_position"]

        w: WindowSpec = Window \
            .partitionBy([f.col(x) for x in column_list]) \
            .orderBy(overall.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 4, "A") \
            .when(rank < 6, "B") \
            .when(rank < 11, "C") \
            .otherwise("D")

        df = df.withColumn(player_cat.name, rule)
        return df

    def method3(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        df = df.withColumn(potential_vs_overall.name, (f.col("potential") / f.col("overall")))
        return df

    def method4(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (player_cat.column().isin("A", "B")) |
            ((player_cat.column().isin("C")) & (potential_vs_overall.column() > 1.15)) |
            ((player_cat.column().isin("D")) & (potential_vs_overall.column() > 1.25))
        )
        return df

    def method5(self, df: DataFrame, parameter: int) -> DataFrame:
        """
        :param parameter:
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        if parameter == 1:
            df = df.filter(
                (age.column() < 23)
            )
        return df