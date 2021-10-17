import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df.printSchema()
        #exercise 1
        df = self.exercise1(df)
        df.show(n=5, truncate=False)
        #df = self.clean_data_solved(df)
        #exercise 2
        df = self.exercise2(df)
        df.show(n=30, truncate=False)
        # exercise 3
        df = self.exercise3(df)
        df.show(n=50, truncate=False)

        df = self.column_selection_solved(df)

        # for show 100 records after your transformations and show the DataFrame schema
        # Here it is shown what is required in the exercise part 1
        df.show(n=1, truncate=False)
        df.printSchema()

        # Uncomment when you want write your final output
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (short_name.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def clean_data_solved(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (long_name.column().isNotNull()) &
            (age.column().isNotNull()) &
            (weight_kg.column().isNotNull()) &
            (nationality.column().isNotNull()) &
            (club_name.column().isNotNull()) &
            (potential.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            overall.column(),
            height_cm.column(),
            team_position.column()
            #catHeightByPosition.column()
        )
        return df

    def exercise1(self, df: DataFrame) -> DataFrame:
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

    def column_selection_solved(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            long_name.column(),
            age.column(),
            weight_kg.column(),
            nationality.column(),
            club_name.column(),
            potential.column(),
            overall.column(),
            height_cm.column(),
            team_position.column()
            #catHeightByPosition.column()
        )
        return df

    def exercise2(self, df: DataFrame) -> DataFrame:
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

    def exercise3(self, df: DataFrame) -> DataFrame:
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