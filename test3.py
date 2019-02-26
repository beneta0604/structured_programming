import psycopg2

conn = psycopg2.connect("dbname=data_selection user=postgres password=123456")
# create a new cursor
cur = conn.cursor()
# execute the Select statement om namen van kolomen die binnen specific tabel zijn te krijgen
cur.execute("SELECT 'r341OVf8e7pTGvSkCWCBRuLyvr3M2HuJTjn0ek2hV08c9fTclYYE05AB0Id0qhSgdutY' FROM buids;")
for i in range(100):
    row = cur.fetchone()
print(row)