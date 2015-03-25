import csv
import sys

import pyspark
import pyspark.sql


def main():
    if len(sys.argv) < 3:
        print >>sys.stderr, "usage: sparkstagram.py <parquet-file> <csv-output-file>"
        return 1

    parquet_file = sys.argv[1]
    output = sys.argv[2]

    sc = pyspark.SparkContext()
    sqlContext = pyspark.sql.SQLContext(sc)

    df = sqlContext.parquetFile(parquet_file)
    df.registerTempTable("instagram")

    records = sqlContext.sql("select * from instagram")

    def write_row(recs, filename):
        count = [0]
        fields = recs.columns

        csv.writer(open(filename, "a+b")).writerow(fields)

        def go(row):
            row = row.asDict()
            data = [row[f].encode("utf-8") for f in fields]
            csv.writer(open(filename, "a+b")).writerow(data)

            count[0] = count[0] + 1
            if count[0] % 5000 == 0:
                print >>sys.stderr, "processed %d records" % (count[0])

        return go

    # Truncate the file.
    with open(output, "wb"):
        pass

    records.foreach(write_row(records, output))

    return 0


if __name__ == "__main__":
    sys.exit(main())
