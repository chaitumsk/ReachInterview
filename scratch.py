import boto3
import gzip
import shutil
import psycopg2

conn = psycopg2.connect(host="localhost", database="postgres", user="Sindhu", password="")
cur = conn.cursor()
print("Connected to DB")
s3 = boto3.client('s3')

keys = []
resp = s3.list_objects_v2(Bucket='reach-solutions-interview-data')
for obj in resp['Contents']:

    print("Processing file:" + obj['Key'])
    # Downloading from S3 bucket to local machine
    s3.download_file('reach-solutions-interview-data', obj['Key'], obj['Key'])
    # Unzip the .gz file
    with gzip.open(obj['Key'], 'rb') as f_in:
        with open(obj['Key'].replace('.gz',''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    # Load to events table
    cur.execute("""copy events from '/Users/Sindhu/Library/Application Support/JetBrains/PyCharmCE2020.1/scratches/""" + obj['Key'].replace('.gz','') + "';")

    cur.execute("commit;")
    keys.append(obj['Key'])

cur.execute("select count(*) from events;")
print("Total records inserted : "+ str(cur.fetchone()[0] ))
# print(keys)
# s3.download_file('reach-solutions-interview-data', '10d491d6-b9cb-4ab8-aaaa-a8432344b3cb.gz', '10d491d6-b9cb-4ab8-aaaa-a8432344b3cb.gz')