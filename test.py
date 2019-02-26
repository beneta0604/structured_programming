import pymongo
import psycopg2
import pprint

def profile_values(inhoud,colomen):
    product = dict(inhoud[0])
    values=[str(product[colomen[0]])]
    return([values,1])

def sessions_values(inhoud,colomen):
    product = dict(inhoud[0])
    values=[]
    time=[]
    for i in range(len(colomen)):
        if 'buid' in colomen[i]:
            values.append(str(product[colomen[i]][0]))
        elif 'session_start'in colomen[i] or 'session_end' in colomen[i]:
            a = str(product[colomen[i]])
            a = a.split(' ')
            a = a[1].split(':')
            a[2] = a[2].split('.')
            a[2] = a[2][0]
            time.append(a)
        elif 'has_sale' in colomen[i]:
            g=[]
            c=0
            for b in range(len(time[0])):
                time[0][b] = int(time[0][b])
                time[1][b] = int(time[1][b])
                if c == 1:
                    g.append((60 - time[0][b] + time[1][b]))
                    g[b - 1] = g[b - 1] - 1
                    if g[b] >= 60:
                        g[b - 1] = g[b - 1] + 1
                        g[b] = g[b] - 60
                else:
                    g.append((time[1][b] - time[0][b]))
                if c == 0:
                    if time[0][b] == time[1][b]:
                        c = 0
                    else:
                        c = 1
            v = 3600
            for b in range(len(g) - 1):
                g[b] = g[b] * v
                v = v / 60
                v = int(v)
            time = g[0] + g[1] + g[2]
            values.append(time)
            values.append(str(product[colomen[i]]))
        else:
            values.append(product[colomen[i]])
    return([values,1])



def buids_values(inhoud,colomen):
    values=[]
    products=dict(inhoud[0])
    if colomen[0] not in products or len(products['buids'])==0:
        return(0)
    for i in range(len(products['buids'])):
        values.append(products['buids'][i])
    return([values,len(products['buids'])])





def product_values(inhoud,colomen,keys):
    print('prodcut_values')
    values=[]
    product=dict(inhoud[0])

    if 'recommendable' in keys:
        if product['recommendable'] == False:
            return(0)
    else:
        return(0)
    if type(product['category']) == list:
        product['sub_category'] = product['category'][1]
        product['sub_sub_category'] = product['category'][2]
        product['category'] = product['category'][0]

    for i in range(len(colomen)):
        if colomen[i] not in keys:
            product[colomen[i]] = 'data is missing'
        if type(product[colomen[i]]) == type(None):
            product[colomen[i]] = 'None'

        if colomen[i] == 'price' or colomen[i] == 'properties':
            if 'selling_price' in product[colomen[i]]:
                values.append(product[colomen[i]]['selling_price'])
            else:
                key = product[colomen[i]].keys()

                if 'doelgroep' not in key:
                    product[colomen[i]]['doelgroep'] = 'data is missing'

                if type(product[colomen[i]]['doelgroep']) == type(None):
                    product[colomen[i]]['doelgroep'] = 'None'

                if '\'' in product[colomen[i]]['doelgroep']:
                    values.append(product[colomen[i]]['doelgroep'].replace('\'', ''))
                else:
                    values.append(product[colomen[i]]['doelgroep'])
        else:
            if colomen[i] == 'recommendable':
                values.append(str(product[colomen[i]]))
            else:
                if '\'' in product[colomen[i]]:
                    values.append(product[colomen[i]].replace('\'', ''))
                else:
                    values.append(product[colomen[i]])
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
    collection = client.data_selection.profiles
    colomen = ['_id']
    tabel='profiles'
    c=0
    for product in collection.find():
        keys = product.keys()
        inhoud =[]
        inhoud.append(product)
        if tabel=='products':
            tempvalues=product_values(inhoud,colomen,keys)
        if tabel=='buids':
            tempvalues = buids_values(inhoud, colomen)
        if tabel=='sessions':
            tempvalues=sessions_values(inhoud,colomen)
        if tabel=='profiles':
            tempvalues=profile_values(inhoud,colomen)
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
# colomen sessions '_id,buid,session_start,session_end,has_sale'



