
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer


class test_Transformer(Writer):
    def __init__(self, spark: SparkSession, parameter: int):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        # method 5   step 1 Exercise 5
        df = self.method5(df, parameter)
        # method 1   step 2 Exercise 1
        df = self.method1(df)
        # method 2   step 3 Exercise 2
        df = self.method2(df)
        # method 3   step 4 Exercise 3
        df = self.method3(df)
        # method 4   step 5 Exercise 4
        df = self.method4(df)
        #This show the total of elements after regular flow made by all exercises.
        print(df.count())
        df = self.test_method4(df)
        #after this part of code we can see a matrix with the details where we can sum all elements and if it matches with the total quantity
        #obtained above then we can say is ok, if both quantities were different then it would be enough evidence to say there is an error.

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

    def test_method4(self, df: DataFrame) -> DataFrame:
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

        cnt_cond = lambda cond: f.sum(f.when(cond, 1).otherwise(0))
        df.groupBy(player_cat.column()).agg(
            cnt_cond((player_cat.column().isin("A", "B"))).alias('group_one'),
            cnt_cond(((player_cat.column().isin("C")) & (potential_vs_overall.column() > 1.15))).alias('group_two'),
            cnt_cond(((player_cat.column().isin("D")) & (potential_vs_overall.column() > 1.25))).alias('group_two'),
        ).show(n=100, truncate=False)
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
