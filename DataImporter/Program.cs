using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace DataImporter
{
  class Program
  {
    const string connectionString =
    "server=localhost;" +
    "uid=root;" +
    "pwd=bloodrayne;" +
    "database=pol;" +
    "connection timeout=1000;" +
    "command timeout=1000;";

    static void LoadFile(string path)
    {
      var tableName = Path.GetFileNameWithoutExtension(path);

      MySqlHelper.ExecuteNonQuery(connectionString, "DROP TABLE IF EXISTS " + tableName);

      using (var reader = new StreamReader(path, Encoding.GetEncoding("iso-8859-2")))
      {
        string header = reader.ReadLine();
        string[] fields = header.Split('\t');

        string createTableString = "CREATE TABLE " + tableName + " (";
        foreach (var field in fields)
        {
          string type = field.StartsWith("nr_") ? "int" : "varchar(255)";
          createTableString += field + " " +  type + ",\n";
        }

        // Remove last ,
        createTableString = createTableString.Remove(createTableString.Length - 2);

        createTableString += ")";

        createTableString = createTableString.Replace("t1&t1p", "t1_t1p");

        MySqlHelper.ExecuteNonQuery(connectionString, createTableString);

        string line;
        while ((line = reader.ReadLine()) != null)
        {
          string[] values = line.Split('\t');

          Debug.Assert(values.Length == fields.Length);

          string insertString = "INSERT INTO " + tableName + " VALUES(";

          foreach (var value in values)
          {
            string valueToInsert;
            if (value == "")
              valueToInsert = "null";
            else
            {
              valueToInsert = "'" + value.Replace("'", "''") + "'";
            }
            insertString += valueToInsert + ", ";
          }

          // Remove last ,
          insertString = insertString.Remove(insertString.Length - 2);

          insertString += ")";

          MySqlHelper.ExecuteNonQuery(connectionString, insertString);
        }
      }
    }
    static void Main(string[] args)
    {
      Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
      List<Task> readerTasks = new List<Task>();

      readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\POL\miasta.txt"))); 
      readerTasks.Add(Task.Run(() => LoadFile(@"G:\SQL\POL\ulice.txt")));

      Task.WaitAll(readerTasks.ToArray());
    }
  }
}
