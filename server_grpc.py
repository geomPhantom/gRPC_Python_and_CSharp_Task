import socket
import json
import psycopg2
from pyDes import *
from cryptography.fernet import Fernet 
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
import struct
import threading
import grpc
import server_pb2
import server_pb2_grpc
from concurrent import futures


HOST = '127.0.0.1'
#HOST = '0.0.0.0'  # Standard loopback interface address (localhost)
PORT = 7777        # Port to listen on (non-privileged ports are > 1023)


def Create_DB_and_Tables():
    conn = psycopg2.connect(host='localhost', port='5432',user='postgres',password='12345')
    conn.autocommit = True
    cur = conn.cursor()
    #cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname='normalizeddb';")
    cur.execute("SELECT * FROM pg_catalog.pg_database;")
    new_db_name = 'normalized_db_' + str(cur.rowcount)
    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname=\'" +new_db_name + "\';")
    if not bool(cur.rowcount):
        m_conn_ = psycopg2.connect(host='localhost', port='5432',user='postgres',password='12345')
        m_conn_.autocommit = True
        cur_ = m_conn_.cursor()
        cur_.execute("CREATE DATABASE " + new_db_name + " WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1;")
        cur_.close()
        m_conn_.close()

    m_conn = psycopg2.connect(host='localhost', port='5432',user='postgres',password='12345',dbname=new_db_name)
    m_conn.autocommit = True
    cur = m_conn.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS customers(id INT PRIMARY KEY, name TEXT NOT NULL, surname TEXT NOT NULL, middle_name TEXT NOT NULL, birth_date TEXT NOT NULL);')

    cur.execute('CREATE TABLE IF NOT EXISTS optics(id INT PRIMARY KEY, name TEXT NOT NULL, address TEXT NOT NULL, phone TEXT NOT NULL);')

    cur.execute('CREATE TABLE IF NOT EXISTS products(id INT PRIMARY KEY,frame_id INT NOT NULL,lens_manufacturer_id INT NOT NULL,od_sph REAL NOT NULL,od_cyl REAL NOT NULL,od_ax INTEGER NOT NULL,os_sph REAL NOT NULL,os_cyl REAL NOT NULL,os_ax INTEGER NOT NULL,pd INTEGER NOT NULL);')

    cur.execute('CREATE TABLE IF NOT EXISTS optic_customer(optics_id INT NOT NULL,customer_id INT NOT NULL,PRIMARY KEY (optics_id, customer_id),FOREIGN KEY (optics_id) REFERENCES optics (id),FOREIGN KEY (customer_id) REFERENCES customers (id));')

    cur.execute('CREATE TABLE IF NOT EXISTS purchases(purchase_date TEXT NOT NULL,optics_id INT NOT NULL,customer_id INT NOT NULL,product_id INT NOT NULL,price REAL NOT NULL,PRIMARY KEY (purchase_date, optics_id),FOREIGN KEY (optics_id) REFERENCES optics (id),FOREIGN KEY (customer_id) REFERENCES customers (id),FOREIGN KEY (product_id) REFERENCES products (id));')

    cur.close()
    m_conn.close()
    return new_db_name


def FillTableWithName(table_name, tables, conn):
    cur = conn.cursor()
    cur.execute("TRUNCATE " + table_name + " CASCADE")

    args_str = ''
    for row in tables[table_name]:
        args_str += '('
        for key in row:
            is_str = isinstance(row[key],str)
            val_fixed = str(row[key]).replace(',','.')
            if is_str: args_str+='\''
            args_str+= val_fixed
            if is_str: args_str+='\''
            args_str+=','
        args_str = args_str[:-1]
        args_str += '),'
    
    args_str = args_str[:-1]
    #print(args_str)
    cur.execute('INSERT INTO ' + table_name + ' VALUES ' + args_str)
    cur.close()


def FillTables(tables, db_name):
    conn = psycopg2.connect(host='localhost', port='5432',user='postgres',password='12345',dbname=db_name)
    conn.autocommit = True
    for table_name in tables:
        FillTableWithName(table_name, tables, conn)
    conn.close()

def SaveToPostgreSQL(tables):
    db_name = Create_DB_and_Tables()
    FillTables(tables, db_name)


class DB_LOADERServicer(server_pb2_grpc.DB_LOADERServicer):
    data = b''
    keyPair = None

    def transfer_DB(self, request_iterator, context):
        #self.data = request.data
        for chunk in request_iterator:
            self.data += chunk.data
        print('Received data: ' + str(len(self.data)) + ' bytes')

        self.keyPair = RSA.generate(3072)

        publicKeyToExport = self.keyPair.publickey().export_key(format='PEM')
        #print(len(publicKeyToExport))
        #conn.sendall(publicKeyToExport)
        #conn.sendall(b'OPENRSAKEY')

        print('Sending public RSA key')

        response = server_pb2.Text()
        response.data = publicKeyToExport
        return response

    def transfer_DES_key(self, request, context):
        encrypted_des_key = request.data
        print('Encrypted DES key received')
        print(len(encrypted_des_key))

        decryptor = PKCS1_OAEP.new(self.keyPair)
        decrypted = decryptor.decrypt(encrypted_des_key)

        print(decrypted)
        print("Decrypted: " + decrypted.decode())
        print(decrypted.decode().encode())
        print(type(decrypted))
        print(type(self.data))
        decoded_str = Fernet(decrypted).decrypt(bytes(self.data)).decode()
        tables = json.loads(decoded_str)
        #print(tables)
        SaveToPostgreSQL(tables)

        print('Data successfully added to PostgreSQL database')
        response = server_pb2.Text()
        response.data = b'Data successfully added to PostgreSQL database'
        return response
        


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    server_pb2_grpc.add_DB_LOADERServicer_to_server(DB_LOADERServicer(), server)

    print('Starting server on port 7777.')
    server.add_insecure_port('0.0.0.0:7777')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
        
        
