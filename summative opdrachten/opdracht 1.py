import pymongo
import psycopg2
import pprint
def sessions_values(inhoud,colomen,keys):
    for products in collection.find():
        print(products)
        a=a+1
    return()



def buids_values(inhoud,colomen,keys):
    values=[]
    products=dict(inhoud[0])
    if colomen[0] not in products or len(products['buids'])==0:
        return(0)
    for i in range(len(products['buids'])):
        values.append(products['buids'][i])
    return([values,len(products['buids'])])





def products_values(inhoud,colomen,keys):
    print('prodcuts_values')
    values=[]
    products=dict(inhoud[0])

    if 'recommendable' in keys:
        if products['recommendable'] == False:
            return(0)
    else:
        return(0)
    if type(products['category']) == list:
        products['sub_category'] = products['category'][1]
        products['sub_sub_category'] = products['category'][2]
        products['category'] = products['category'][0]

    for i in range(len(colomen)):
        if colomen[i] not in keys:
            products[colomen[i]] = 'data is missing'
        if type(products[colomen[i]]) == type(None):
            products[colomen[i]] = 'None'

        if colomen[i] == 'price' or colomen[i] == 'properties':
            if 'selling_price' in products[colomen[i]]:
                values.append(products[colomen[i]]['selling_price'])
            else:
                key = products[colomen[i]].keys()

                if 'doelgroep' not in key:
                    products[colomen[i]]['doelgroep'] = 'data is missing'

                if type(products[colomen[i]]['doelgroep']) == type(None):
                    products[colomen[i]]['doelgroep'] = 'None'

                if '\'' in products[colomen[i]]['doelgroep']:
                    values.append(products[colomen[i]]['doelgroep'].replace('\'', ''))
                else:
                    values.append(products[colomen[i]]['doelgroep'])
        else:
            if colomen[i] == 'recommendable':
                values.append(str(products[colomen[i]]))
            else:
                if '\'' in products[colomen[i]]:
                    values.append(products[colomen[i]].replace('\'', ''))
                else:
                    values.append(products[colomen[i]])
    return([values,1])

def databases():
    client = pymongo.MongoClient()
    conn = psycopg2.connect("dbname=data_selection user=postgres password=123456")
    cur = conn.cursor()
    tabel_all_values(client, conn, cur)
    # close communication with the database
    cur.close()
    conn.close()


def tabel_all_values(client, conn, cur):
    collection = client.data_selection.products
    colomen = ['_id','brand','category','gender','price','properties','recommendable','sub_category','sub_sub_category']
    tabel='products'
    c=0
    for products in collection.find():
        keys = products.keys()
        inhoud =[]
        inhoud.append(products)
        if tabel=='products':
            tempvalues=products_values(inhoud,colomen,keys)
        if tabel=='buids':
            tempvalues = buids_values(inhoud, colomen, keys)


        if tempvalues==0:
            continue
        for i in range(tempvalues[1]):
            if tempvalues[1]>1:
                values = str(tempvalues[0][i])
                values= '\'' + values + '\''
            else:
                values=str(tempvalues[0])
            print(tempvalues)
            values = values.strip('[]')
            values = values.replace('\"', '\'')

            insert(cur, conn, values,tabel)
            c+=1

def insert(cur, conn, values,tabel):
    try:
        sql = 'INSERT INTO ' + tabel + ' VALUES (' + values + ')'
        print(sql)
        cur.execute(sql)
        conn.commit()
    except psycopg2.IntegrityError:
        conn.commit()

databases()
# colomen products '_id','brand','category','gender','price','properties','recommendable','sub_category','sub_sub_category']
# colomen buids 'buids'



