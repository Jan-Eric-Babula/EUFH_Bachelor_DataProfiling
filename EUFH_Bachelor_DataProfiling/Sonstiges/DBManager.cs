using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;

namespace EUFH_Bachelor_DataProfiling.Sonstiges
{
	class DBManager
	{

		private static string connectionString = @"Data Source=localhost;Initial Catalog=EMEA_GKM_COPY;Integrated Security=True";
		private static SqlConnection connection = new SqlConnection(connectionString);

		public static SqlConnection Instance
		{
			get
			{
				return GetConection();
			}
		}

		private static SqlConnection GetConection()
		{
			if (connection.State != System.Data.ConnectionState.Open)
			{
				connection.Open();
			}
			return connection;
		}

		public static SqlDataReader ExecuteRead(string sqlCmd)
		{
			File.AppendAllLines("sql.log", new string[] { $"{DateTime.Now}: { sqlCmd.Replace(System.Environment.NewLine, " ") }" }, Encoding.UTF8);
			SqlDataReader ret = null;
			try
			{
				SqlCommand cmd = new SqlCommand(sqlCmd, DBManager.Instance);
				cmd.CommandTimeout = 90;
				ret = cmd.ExecuteReader();
			}
			catch (SqlException e)
			{
				Console.WriteLine($"Exception: {e.Message}\n{e.StackTrace}");
				File.AppendAllLines("sql.log", new string[] {
					"####################################################",
					$"Error ({DateTime.Now})\n",
					$"Message: {e.Message}\nStackTrace:\n{e.StackTrace}\n",
					$"Inner Exception:\n{e.InnerException}"
				}, Encoding.UTF8);
			}
			return ret;
		}

		public static void CloseConnection()
		{
			connection.Close();
		}

		public static void ResetLog()
		{
			if (File.Exists("sql.log"))
			{
				File.Delete("sql.log");
			}
			File.WriteAllLines("sql.log", new string[] { $"Reset SQL call log on {DateTime.Now}", "####################################################", "" }, Encoding.UTF8);
		}

	}
}
