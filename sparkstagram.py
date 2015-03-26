import csv
import sys

import pyspark
import pyspark.sql


def main():
    if len(sys.argv) < 3:
        print >>sys.stderr, "usage: sparkstagram.py <csv-output-file> <parquet-file> [<parquet-file> ...]"
        return 1

    output = sys.argv[1]
    parquet_files = sys.argv[2:]

    sc = pyspark.SparkContext()
    sqlContext = pyspark.sql.SQLContext(sc)

    df = sqlContext.parquetFile(parquet_files[0])
    df.registerTempTable("instagram")

    records = sqlContext.sql("select * from instagram limit 1")
    csv.writer(open(output, "w+b")).writerow(records.columns)

    def write_row(recs, filename):
        count = [0]
        fields = recs.columns

        def go(row):
            row = row.asDict()
            data = [row[f].encode("utf-8") for f in fields]
            csv.writer(open(filename, "a+b")).writerow(data)

            count[0] = count[0] + 1
            if count[0] % 5000 == 0:
                print >>sys.stderr, "processed %d records" % (count[0])

        return go

    query = """\
select * from instagram
where longitude between -74.0852737 and -73.7468033 and
      latitude between 40.6166715 and 40.8799574 and
      posted_date between '2013-01-01T00:00:00' and '2013-12-31T23:59:59'
"""

    for p in parquet_files:
        print >>sys.stderr, "processing %s" % (p)

        df = sqlContext.parquetFile(p)
        df.registerTempTable("instagram")

        records = sqlContext.sql(query)

        records.foreach(write_row(records, output))

    return 0


if __name__ == "__main__":
    sys.exit(main())
