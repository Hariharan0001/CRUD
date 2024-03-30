from flask import Flask,request
import psycopg2
from dotenv import load_dotenv
import os

app=Flask(__name__)

load_dotenv()

#establish the connection when the function called
def connection():
    return psycopg2.connect(
        database=os.getenv("DB_NAME", 'test'),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


#Get all the table names
@app.route('/getalltable',methods=['GET'])
def hel():
   con=connection()
   cur=con.cursor()
   cur.execute("select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE' ")
   data=cur.fetchall()
   cur.close()
   con.close()
   return data
   

#Get all the values from the given table name  
@app.route('/gettablevalue/<tablename>',methods=['GET'])
def val(tablename):
    con=connection()
    cur=con.cursor()
    cur.execute(f"select * from {tablename} where deleted=false")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    result = [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
    cur.close()
    con.close()
    retunval={"result":result}
    return retunval


#create the table with the given json value
@app.route('/create',methods=['POST'])
def create():
    data= request.json
    table_name=data.get('table_name',0)
    if not table_name:
        return "enter the table name"
    con=connection()
    cur=con.cursor()
    cur.execute(f"select exists ( select * from information_schema.tables where table_name='{table_name}')")
    table_exist=cur.fetchall()
    if table_exist[0][0]:
        cur.close()
        con.close()
        return "The table name already exist"
    columns=data.get('columns')
    colum_arr=[]
    for key,val in columns.items():
        colum_arr.append(f'{key} {val}')
    col_str=', '.join(colum_arr)
    cur.execute(f"create table {table_name}({col_str})")
    con.commit()
    cur.close()
    con.close()
    return "Table created successfully"


#update the table with the given json value
@app.route('/update',methods=['PUT'])
def put():
    data=request.json
    table_name=data.get('table_name',0)
    if not table_name:
        return "Enter the table name"
    con=connection()
    cur=con.cursor()
    cur.execute(f"select exists ( select * from information_schema.tables where table_name='{table_name}')")
    table_exist=cur.fetchall()
    if not table_exist[0][0]:
        cur.close()
        con.close()
        return "Please enter the exsist table name or to create a new table with another endpoint"
    valjson=data.get('values')
    schjson=data.get('sch')
    columns = ', '.join(schjson.values())
    values_list = [tuple(val.values()) for val in valjson.values()]
    values_str = ', '.join([str(val) for val in values_list])
    cur.execute(f"insert into {table_name} ({columns}) values {values_str}")
    con.commit()
    cur.close()
    con.close()
    return "value insert successfully"


#single get that is retrive one value from the database with the  
#input value based on the endpoint  
@app.route('/singleget/<tablename>/<key>/<value>',methods=['GET'])
def findtable(tablename,key,value):
    con=connection()
    cur=con.cursor()
    cur.execute(f"select exists ( select * from information_schema.tables where table_name='{tablename}')")
    table_exist=cur.fetchall()
    if not table_exist[0][0]:
        cur.close()
        con.close()
        return "The table name is not in the database"
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tablename}'")
    columns = [row[0] for row in cur.fetchall()]
    if key not in columns:
        cur.close()
        con.close()
        return "The column is not present in the table"
    cur.execute(f"select deleted from {tablename} where {key}={value}")
    delval=cur.fetchall()[0][0]
    if(delval):
        cur.close()
        con.close()
        return "The value is delete from the table"
    cur.execute(f"select * from {tablename} where {key}={value}")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    result = [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
    returnval={"result":result[0]}
    return returnval


#perform soft delete on the database with the input based on the endpoint 
@app.route('/delete/<tablename>/<key>/<value>',methods=['DELETE'])
def deleteval(tablename,key,value):
    con=connection()
    cur=con.cursor()
    cur.execute(f"select exists ( select * from information_schema.tables where table_name='{tablename}')")
    table_exist=cur.fetchall()
    if not table_exist[0][0]:
        cur.close()
        con.close()
        return "The table name is not in the database"
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tablename}'")
    columns = [row[0] for row in cur.fetchall()]
    if key not in columns:
        cur.close()
        con.close()
        return "The column is not present in the table"
    cur.execute(f"update {tablename} set deleted=true where {key}='{value}'")
    con.commit()
    cur.close()
    con.close()
    return "row delete successfully"

if __name__=='__main__':
    app.run()
