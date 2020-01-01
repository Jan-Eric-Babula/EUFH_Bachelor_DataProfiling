using System;
using System.IO;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Reflection;
using System.Data;

namespace EUFH_Bachelor_DataProfiling_V2.HelperClasses
{
	class DBManager
	{
		private static readonly string ConfigPath = $@"connection_cfg.json";
		private static string _ConnectionString;
		
		public static string ConnectionString
		{
			get
			{
				return _ConnectionString;
			}
		}

		private static bool _Init
		{
			get
			{
				return _Connection == null ? false : 
					(
					_Connection.State == ConnectionState.Executing || 
					_Connection.State == ConnectionState.Fetching || 
					_Connection.State == ConnectionState.Open ? true : false
					);
			}
		}

		private static DBConnectionConfig _Config;
		private class DBConnectionConfig
		{
			public string Server
			{
				get; set;
			}
			public string Database
			{
				get; set;
			}
		}

		private static SqlConnection _Connection;
		public static SqlConnection Instance
		{
			get
			{
				return _Init ? _Connection : null;
			}
		}		

		public static void CloseConnection()
		{
			_Connection.Close();
		}

		public static void Init()
		{
			LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

			if (File.Exists(ConfigPath))
			{
				string _json = File.ReadAllText(ConfigPath);
				_Config = JsonConvert.DeserializeObject<DBConnectionConfig>(_json);

				if (!string.IsNullOrWhiteSpace(_Config.Server) && !string.IsNullOrWhiteSpace(_Config.Database))
				{
					_ConnectionString = 
						$"Data Source={_Config.Server};"+
						$"Initial Catalog={_Config.Database};"+
						"Integrated Security=True;"+
						"MultipleActiveResultSets=True;";

					_Connection = new SqlConnection(_ConnectionString);

					_Connection.Open();
				}
				else
				{
					throw new FormatException($"Config values from '{ConfigPath}' are corrupted or empty!");
				}
			}
			else
			{
				throw new FileNotFoundException("Database connection configuration file not found!", ConfigPath);
			}
		}

		public static SqlDataReader ExecuteRead(string sqlCmd)
		{
			LogHelper.LogSQL(sqlCmd);

			SqlConnection _Con = DBManager.Instance;
			SqlDataReader _Ret = null;

			try
			{
				if (_Con == null)
				{
					throw new ArgumentNullException("Database connection instance is not initialized!");
				}
				else
				{
					SqlCommand _Cmd = new SqlCommand(sqlCmd, _Con);
					_Cmd.CommandTimeout = 150;

					_Ret = _Cmd.ExecuteReader();
				}
			}
			catch(Exception e)
			{
				LogHelper.LogAppError(e);
			}

			return _Ret;
		}

	}
}
