import csv
import datetime
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


if __name__ == '__main__':
    print("Parser App")

input_path = sys.argv[0]
output_path = sys.argv[1]

file = "LPLTIP08NAA.TXT"  # acct conv

# position
'''
seqStartPos = 96
seqEndPos = 98
'''
# dividend
'''
seqStartPos = 106
seqEndPos = 108
'''
# acctconv,activities

seqStartPos = 3
seqEndPos = 6

f = open(file, "r")
for i in range(1):
    next(f)

fname = "FM-NewAccountActivity-layout.txt"
fobj = open(fname, "r")

row_count = sum(1 for row in fobj)
fobj.seek(0)
print(row_count)


def compute_blanks(a):
    Blank = ""
    for key in a:
        Blank += "|"
    return Blank


def parse_fields(layoutList, x):
    Fields = ""
    for key in layoutList:
        pos = layoutList[key].split("-")
        Fields += x[int(pos[0]):int(pos[1])] + "|"
    return Fields


csvreader = csv.DictReader(fobj, delimiter='|')

seq = []
o = {}
i = {}
ln = "001"
ln1 = 0
rt = ""
cnt1 = 1
header = ""

for line in csvreader:
    seq.append(line["rtype"] + "-" + line["line"])
    header += line["var"] + "|"
    if line["line"] != ln or line["rtype"] != rt:
        if rt == "":
            rt = line["rtype"]

        o[rt + "-" + ln] = i
        i = {}
        i[line["var"]] = line["spos"] + "-" + line["epos"]
    else:
        i[line["var"]] = line["spos"] + "-" + line["epos"]

    ln = line["line"]
    rt = line["rtype"]

    if cnt1 == row_count - 1:
        o[line["rtype"] + "-" + ln] = i

    cnt1 += 1


def parser(x, seqs):
    global parsed_fields
    key = ""
    st1 = ""
    RecordType = x[0:3]
    Rtype = RecordType
    Rseq = x[seqStartPos:seqEndPos]
    record = ""

    if (Rseq == "  "):
        Rseq = "001"

    key = Rtype + "-" + Rseq
    if (key in seqs):
        if (Rseq == "001"):
            parsed_fields = {}

            for y1 in seq:
                parsed_fields[y1] = compute_blanks(o[y1])

        parsed_fields[key] = parse_fields(o[key], x)
        # print(parsedFields)
        for y in seq:
            st1 += parsed_fields[y]
            if (y == (key)):
                break
        # print(parsedFields)
        if (st1 != ""):
            record = str(Rtype + "|" + st1.replace('\r', ''))
            if (st1 != ""):
                record = str(Rtype + "|" + st1.replace('\r', '').replace('\n', ''))
    return record


spark = SparkSession.builder \
    .appName("Parser") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

seq_broadcast = spark.sparkContext.broadcast(seq)

in_df = spark.read.text(input_path)

parserUDF = udf(lambda z, seq: parser(z, seq), StringType())

formatted_df = in_df.withColumn("formatted_record", parserUDF(F.col("value"), seq_broadcast.value))
currentDT = datetime.datetime.now()

formatted_df.write.mode("overwrite").format("text").save(output_path + "/" + currentDT.strftime("%Y%m%d%H%M%S"))
