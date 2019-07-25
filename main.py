from pyspark import SparkContext

__all__ = ["LoanAnalytics"]


class LoanAnalytics(object):
    """
    Creates a member of the Castle Kilmere School of Magic
    """

    def __init__(self):
        self.version = 1

    @staticmethod
    def mapByStratumKey(row):
        """ a custom mapper that creates stratum level keys """
        field = row[0].split(",")
        network = field[1].strip("'")
        date = field[2].strip("'").split("-")
        month = date[-2]
        year = date[-1]
        product = field[3].strip("'")
        amount = float(field[4])
        # define stratificationKey
        stratum_key = (network, product, year, month)  # partition by year then month to scale even better
        return (stratum_key, (1, amount))

    @staticmethod
    def reduceByStratumKey(x, y):
        """ A custim reducer that performs strum level sum an counts """
        stratum_count = x[0] + y[0]
        stratum_sum = x[1] + y[1]
        return (stratum_count, stratum_sum)

    @staticmethod
    def mergeColumnMap(col):
        """ A map function that merges disjointed tuples """
        return (col[0] + col[1])

    @staticmethod
    def rddToCsv(rdd, sc, csvPath='Output.csv'):
        """ converts an RDD into CSV file """

        def toCsvLine(data):
            return ','.join(str(d) for d in data)

        header = sc.parallelize([("Network", "Product", "Year",
                                 "Month", "Count", "Amount")])
        header.union(rdd).map(toCsvLine).coalesce(1, True).saveAsTextFile(csvPath)

    def rddMapReducer(self, rdd):
        """ This is a custom map-reduce function

        Takes in an RDD and performs necessary transformations
        by calling 2 main functions: 1. mapByStratumKey
        and 2. reduceByStratumKey.

        Args:
          rdd: RDD of an imported dataset

        Returns:
          rdd: transformed RDD
        """
        return rdd \
            .zipWithIndex() \
            .filter(lambda x: x[1] > 1) \
            .map(lambda row: self.mapByStratumKey(row)) \
            .reduceByKey(lambda x, y: self.reduceByStratumKey(x, y)) \
            .map(lambda col: self.mergeColumnMap(col))

    @staticmethod
    def printRddDiagnostic(sc, rdd):
        """ prints diagnostics information of an RDD """
        print("Default parallelism: {}".format(sc.defaultParallelism))
        print("Number of partitions: {}".format(rdd.getNumPartitions()))
        print("Partitioner: {}".format(rdd.partitioner))
        print("Partitions structure: {}".format(rdd.glom().collect()))


if __name__ == "__main__":
    # intialize prerequisites
    sparkContext = SparkContext.getOrCreate()
    loan = LoanAnalytics()
    # CSV File locations
    inputCsv = "./data/input.csv"
    bigData = "./data/100000000.csv"
    # fetch csv as RDD
    csv = sparkContext.textFile(inputCsv)
    #  transform RDD using map reduce
    rdd = loan.rddMapReducer(csv)
    # print RDD details
    loan.printRddDiagnostic(sparkContext, rdd)
    # save RDD as
    loan.rddToCsv(rdd, sparkContext, './Output.csv')
