using System;  
using System.Net;  
using System.Net.Sockets;  
using System.Threading;  
using System.Diagnostics;
using System.Text;  
using System.Data;
using System.Data.SQLite;
using Npgsql;
using System.IO;
using Newtonsoft.Json;
using System.Security.Cryptography;
using System.Net.Http;
using System.Threading.Tasks;
using Grpc.Net.Client;
using Google.Protobuf;



public class Tables{
    public DataSet tables = null;

    public Tables(string SQLite_DB_Path){
        tables = Read_SQLite_DB(SQLite_DB_Path);
    }

    private DataSet Read_SQLite_DB(string SQLite_DB_Path){
        string fullPath = Directory.GetCurrentDirectory() + SQLite_DB_Path;
        SQLiteConnection conread = new SQLiteConnection("Data Source=" + fullPath);
        conread.Open();

        string selectSQL = "SELECT * FROM purchases";
        SQLiteCommand selectCommand = new SQLiteCommand(selectSQL, conread);
        SQLiteDataReader dataReader = selectCommand.ExecuteReader();
        DataSet ds = new DataSet();
        DataTable dt = new DataTable("purchases");
        
        dt.Load(dataReader);
        ds.Tables.Add(dt);

        conread.Close();
        return ds;
    }

    public void Normalize(){
        DataSet ds = new DataSet();

        DataView view = new DataView(tables.Tables[0]);

        DataTable customers_table = view.ToTable(true, "customer_id", "customer_name", "customer_surname", 
        "customer_middle_name", "customer_birth_date");
        customers_table.TableName = "customers";

        DataTable optics_table = view.ToTable(true, "optics_id", "optics_name", "optics_address","optics_phone");
        optics_table.TableName = "optics";

        DataTable products_table = view.ToTable(true, "product_id", "product_frame_id", "product_lens_manufacturer_id", 
        "product_od_sph", "product_od_cyl", "product_od_ax", "product_os_sph", "product_os_cyl", "product_os_ax", "product_pd");
        products_table.TableName = "products";

        DataTable optic_customer_table = view.ToTable(true, "optics_id", "customer_id");
        optic_customer_table.TableName = "optic_customer";

        DataTable purchases_table = view.ToTable(true, "purchase_date", "optics_id", "customer_id", "product_id", "price");
        purchases_table.TableName = "purchases";

        ds.Tables.Add(customers_table);
        ds.Tables.Add(optics_table);
        ds.Tables.Add(products_table);
        ds.Tables.Add(optic_customer_table);
        ds.Tables.Add(purchases_table);

        tables = ds;
    }

    // private void Create_DB_and_Tables(){
    //     string connStr = "Server=localhost;Port=5432;User Id=postgres;Password=12345;";

    //     bool dbExists;

    //     using (NpgsqlConnection conn = new NpgsqlConnection(connStr))
    //     {
    //         conn.Open();
    //         string cmdText = "SELECT 1 FROM pg_catalog.pg_database WHERE datname='normalizeddb';";
    //         using (NpgsqlCommand cmd = new NpgsqlCommand(cmdText, conn))
    //         {
    //             dbExists = cmd.ExecuteScalar() != null;
    //         }
    //     }

    //     if(!dbExists){
    //         var m_conn_ = new NpgsqlConnection(connStr);
    //         var m_createdb_cmd = new NpgsqlCommand(@"
    //             CREATE DATABASE normalizeddb
    //             WITH OWNER = postgres
    //             ENCODING = 'UTF8'
    //             CONNECTION LIMIT = -1;
    //             ", m_conn_);
    //         m_conn_.Open();
    //         m_createdb_cmd.ExecuteNonQuery();
    //         m_conn_.Close();
    //     }

    //     connStr = "Server=localhost;Port=5432;User Id=postgres;Password=12345;Database=normalizeddb";
    //     var m_conn = new NpgsqlConnection(connStr);
    //     var m_createtbl_cmd = new NpgsqlCommand(@"CREATE TABLE IF NOT EXISTS customers(
    //         id INT PRIMARY KEY,
    //         name TEXT NOT NULL,
    //         surname TEXT NOT NULL,
    //         middle_name TEXT NOT NULL,
    //         birth_date TEXT NOT NULL
    //         );",
    //     m_conn);
    //     m_conn.Open();
    //     m_createtbl_cmd.ExecuteNonQuery();

    //     m_createtbl_cmd = new NpgsqlCommand(@"CREATE TABLE IF NOT EXISTS optics(
    //         id INT PRIMARY KEY,
    //         name TEXT NOT NULL,
    //         address TEXT NOT NULL,
    //         phone TEXT NOT NULL
    //         );",
    //     m_conn);
    //     m_createtbl_cmd.ExecuteNonQuery();

    //     m_createtbl_cmd = new NpgsqlCommand(@"CREATE TABLE IF NOT EXISTS products(
    //         id INT PRIMARY KEY,
    //         frame_id INT NOT NULL,
    //         lens_manufacturer_id INT NOT NULL,
    //         od_sph REAL NOT NULL,
    //         od_cyl REAL NOT NULL,
    //         od_ax INTEGER NOT NULL,
    //         os_sph REAL NOT NULL,
    //         os_cyl REAL NOT NULL,
    //         os_ax INTEGER NOT NULL,
    //         pd INTEGER NOT NULL
    //         );",
    //     m_conn);
    //     m_createtbl_cmd.ExecuteNonQuery();

    //     m_createtbl_cmd = new NpgsqlCommand(@"CREATE TABLE IF NOT EXISTS optic_customer(
    //         optics_id INT NOT NULL,
    //         customer_id INT NOT NULL,
    //         PRIMARY KEY (optics_id, customer_id),
    //         FOREIGN KEY (optics_id) REFERENCES optics (id),
    //         FOREIGN KEY (customer_id) REFERENCES customers (id)
    //         );",
    //     m_conn);
    //     m_createtbl_cmd.ExecuteNonQuery();

    //     m_createtbl_cmd = new NpgsqlCommand(@"CREATE TABLE IF NOT EXISTS purchases(
    //         purchase_date TEXT NOT NULL,
    //         optics_id INT NOT NULL,
    //         customer_id INT NOT NULL,
    //         product_id INT NOT NULL,
    //         price REAL NOT NULL,
    //         PRIMARY KEY (purchase_date, optics_id),
    //         FOREIGN KEY (optics_id) REFERENCES optics (id),
    //         FOREIGN KEY (customer_id) REFERENCES customers (id),
    //         FOREIGN KEY (product_id) REFERENCES products (id)
    //         );",
    //     m_conn);
    //     m_createtbl_cmd.ExecuteNonQuery();

    //     m_conn.Close();
    // }

    // private void FillTableWithName(string name, NpgsqlConnection conn){
    //     //Console.WriteLine(name);
    //     var truncate_command = new NpgsqlCommand("TRUNCATE " + name + " CASCADE", conn);
    //     truncate_command.ExecuteNonQuery();

    //     object[] outRow = new object[tables.Tables[name].Columns.Count];
    //     using (var writer = conn.BeginTextImport("COPY " + name + " FROM STDIN")) {
    //         foreach (DataRow rw in tables.Tables[name].Rows)
    //         {
    //                 for (int col = 0; col < tables.Tables[name].Columns.Count; col++)
    //                 outRow[col] = rw[col];

    //             string cur = string.Join("\t", outRow);
    //             cur = cur.Replace("," , "."); //fix for real values separator
    //             writer.WriteLine(cur);
    //         }
    //     }
    // }

    // private void FillTables(){
    //     var connStr = "Server=localhost;Port=5432;User Id=postgres;Password=12345;Database=normalizeddb";
    //     var conn = new NpgsqlConnection(connStr);
    //     conn.Open();
    //     foreach(DataTable table in tables.Tables){
    //         FillTableWithName(table.TableName, conn);
    //     }
    //     conn.Close();
    // }
    // public void SaveToPostgreSQL(){
    //     Create_DB_and_Tables();

    //     FillTables();
    // }

    public static void WriteToExcel(string cmd, string args){
        //Console.WriteLine(args);
        ProcessStartInfo start = new ProcessStartInfo();
        start.FileName = Directory.GetCurrentDirectory() + "/python/bin/python3";
        //Console.WriteLine(start.FileName);
        start.Arguments = string.Format("{0} {1}", cmd, args);
        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        using(Process process = Process.Start(start))
        {
            using(StreamReader reader = process.StandardError)
            {
                string error = reader.ReadToEnd();
                Console.Write(error);
            }
            Console.WriteLine("");
            using(StreamReader reader = process.StandardOutput)
            {
                string result = reader.ReadToEnd();
                Console.Write(result);
            }
        }
    }

    public void printTables(){
        foreach(DataTable table in tables.Tables){
            Console.WriteLine(table.TableName);
            foreach (DataColumn column in table.Columns){ Console.Write(column.ColumnName + " "); }
            Console.WriteLine("");
            foreach(DataRow row in table.Rows){
                foreach (DataColumn column in table.Columns){
                    Console.Write(row[column]+" ");
                }
                Console.WriteLine("");
            }
        }
    }
}


public class gRPC_Client {  
    const int chunkSize = 1000;
    public static (string des_key, string tables_ciphered) CallPythonEncryptByDES(string cmd, string args){
        //Console.WriteLine(args);
        ProcessStartInfo start = new ProcessStartInfo();
        //start.FileName = "/usr/bin/python";
        start.FileName = "/Applications/Xcode.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.7/Resources/Python.app/Contents/MacOS/Python";
        //Console.WriteLine(start.FileName);
        start.Arguments = string.Format("{0} {1}", cmd, args);
        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        string des_key = "";
        string tables_ciphered = "";
        //test new branch
        using(Process process = Process.Start(start))
        {
            using(StreamReader reader = process.StandardError)
            {
                string error = reader.ReadToEnd();
                Console.Write(error);
            }
            Console.WriteLine("");
            using(StreamReader reader = process.StandardOutput)
            {
                des_key = reader.ReadLine();
                tables_ciphered = reader.ReadLine();
                //result = reader.ReadToEnd();
                //Console.Write(result);
            }
        }
        return (des_key, tables_ciphered);
    }

    public static void CallPythonEncryptDESKeyByRSA(string cmd, string args){
        //Console.WriteLine(args);
        ProcessStartInfo start = new ProcessStartInfo();
        //start.FileName = "/usr/bin/python";
        start.FileName = "/Applications/Xcode.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.7/Resources/Python.app/Contents/MacOS/Python";
        //Console.WriteLine(start.FileName);
        start.Arguments = string.Format("{0} {1}", cmd, args);
        start.UseShellExecute = false;
        start.CreateNoWindow = true;
        start.RedirectStandardOutput = true;
        start.RedirectStandardError = true;
        string des_key_ciphered = "";
        using(Process process = Process.Start(start))
        {
            using(StreamReader reader = process.StandardError)
            {
                string error = reader.ReadToEnd();
                Console.Write(error);
            }
            Console.WriteLine("");
            using(StreamReader reader = process.StandardOutput)
            {
                des_key_ciphered = reader.ReadToEnd();
                //Console.Write(result);
            }
        }
        //return des_key_ciphered;
    }

    public static async Task StartClient(Tables t) {  
        // Data buffer for incoming data.  
        byte[] bytes = new byte[16384];  
  
        try {  

            String jsonified_tables = JsonConvert.SerializeObject(t.tables, Formatting.Indented);
            //Console.WriteLine(jsonified_tables);
            (String des_key, String jsonified_tables_ciphered) = gRPC_Client.CallPythonEncryptByDES(Directory.GetCurrentDirectory() + "/encryptDES.py", "\""+jsonified_tables.Replace("\"","\\\"")+"\"");
            // Send test data to the remote device.  
            byte[] msg = Encoding.ASCII.GetBytes(jsonified_tables_ciphered);


            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            var httpHandler = new HttpClientHandler();
            httpHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
            
            var channel = GrpcChannel.ForAddress("*removed*", new GrpcChannelOptions { HttpHandler = httpHandler });
            var client =  new DB_LOADER.DB_LOADERClient(channel);
            Console.WriteLine("Sending data.");

            using var transfer_DB_request = client.transfer_DB();
            //int chunkSize = 1000;
            byte[] chunk;
            for(int i=0;i<msg.Length;i+=chunkSize){
                if(i+chunkSize > msg.Length){
                      chunk = new byte[msg.Length-i];
                      Buffer.BlockCopy(msg,i,chunk,0,msg.Length-i);
                }else{
                      chunk = new byte[chunkSize];
                      Buffer.BlockCopy(msg,i,chunk,0,chunkSize);
                }
                await transfer_DB_request.RequestStream.WriteAsync(new Text{ Data = ByteString.CopyFrom(chunk)});
            }
            Console.WriteLine("S");
            await transfer_DB_request.RequestStream.CompleteAsync();

            var reply = await transfer_DB_request.ResponseAsync;

            Console.WriteLine("Public RSA key received");

            byte[] public_rsa_bytes = reply.Data.ToByteArray();


            var fileName = "public_rsa_key.pem";
            FileStream fs = File.OpenWrite(fileName);
            fs.Write(public_rsa_bytes, 0, public_rsa_bytes.Length);
            fs.Close();

            var fileName2 = "des_key.txt";
            File.WriteAllText(fileName2, des_key);

            gRPC_Client.CallPythonEncryptDESKeyByRSA(Directory.GetCurrentDirectory() + "/encryptDESKeyByRSA.py", "");

            Console.WriteLine("DES key: "+ des_key);
            byte[] msg2 = File.ReadAllBytes("encrypted_des_key");   //!!! encrypted_DES_key -> des_key
            
        
                
            Console.WriteLine("Seding encrypted DES key.");
            reply = client.transfer_DES_key(new Text{ Data = ByteString.CopyFrom(msg2) });

            Console.WriteLine("Encrypted DES key sent.");
            //int bytesRec2 = sender.Receive(bytes);  
            Console.WriteLine("Response received: {0}", reply.Data.ToString(Encoding.ASCII));
  
        } catch (Exception e) {  
            Console.WriteLine( e.ToString());  
        }  
    }  

    public static async Task Main(String[] args) {  
        Tables table = new Tables("/purchases_db.db");

        //table.printTables();

        table.Normalize();
        //table.printTables();
        //StartClient(table);  
        await gRPC_Client.StartClient(table);
        //AsynchronousClient.CallPythonEncrypt(Directory.GetCurrentDirectory() + "/encryptDES.py", "\"i want, to encrypt! . this))\"");
        //return 0;  
    }  
   
}  