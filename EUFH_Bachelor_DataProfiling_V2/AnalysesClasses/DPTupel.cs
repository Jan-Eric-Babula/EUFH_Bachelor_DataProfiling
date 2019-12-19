using EUFH_Bachelor_DataProfiling_V2.HelperObjects;
using EUFH_Bachelor_DataProfiling_V2.ResultObjects;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.AnalysesClasses
{
	class DPTupel
	{

		public static AErgTupel Analysis(string Database, string Relation)
		{
			AErgTupel _Ret = new AErgTupel(Database, Relation);

			_Ret.Size = DPTupel_Helper.Get_Size(_Ret.Relation);

			_Ret.FunctionalDependencyGrid = DPTupel_Helper.CreateDependencyMatrix(_Ret.Relation);

			_Ret.DocumentedKey = DPTupel_Helper.Get_DocumentedKeys(Relation);



			return _Ret;
		}

		private class DPTupel_Helper
		{

			public static long Get_Size(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT COUNT_BIG(*) FROM {Relation};
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? size = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

				_DR.Close();

				if (!size.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return size.Value;
			}

			public static Dictionary<string, Dictionary<string, bool>> CreateDependencyMatrix(string Relation)
			{

				Dictionary<string, Dictionary<string, bool>> _Ret = new Dictionary<string, Dictionary<string, bool>>();

				List<string> Attributes = Get_Attributes(Relation);

				foreach (string A1 in Attributes)
				{
					Dictionary<string, bool> _tmp = new Dictionary<string, bool>();

					foreach (string A2 in Attributes)
					{
						_tmp.Add(A2, CheckDependency(Relation, A1, A2));
					}

					_Ret.Add(A1, _tmp);
				}

				return _Ret;

			}

			public static List<string> Get_Attributes(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				List<string> _Ret = new List<string>();

				string sql_cmd_str = $@"
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' = '{Relation}' ORDER BY ORDINAL_POSITION;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					_Ret.Add(_DR.GetString(0));
				}

				_DR.Close();

				if (_Ret.Count == 0)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _Ret;
			}

			public static bool CheckDependency(string Relation, string Attribute1, string Attribute2)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT
	CAST(CASE WHEN COUNT(K.A) > 0 THEN 0 ELSE 1 END AS BIT)
FROM (
SELECT TOP 1 [{Attribute1}] A
FROM {Relation}
GROUP BY [{Attribute1}]
HAVING COUNT (DISTINCT [{Attribute2}]) > 1) K;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				bool? fd = _DR.IsDBNull(0) ? (bool?)null : _DR.GetBoolean(0);

				_DR.Close();

				if (!fd.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return fd.Value;
			}

			public static PossibleKey Get_DocumentedKeys(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				List<string> _cols = new List<string>();

				string sql_cmd_str = $@"
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU ON TC.CONSTRAINT_NAME=CCU.CONSTRAINT_NAME
WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND '['+TC.TABLE_SCHEMA+'].['+TC.TABLE_NAME+']' = '{Relation}';
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					_cols.Add(_DR.GetString(0));
				}

				_DR.Close();

				PossibleKey _ret = null;

				if (_cols.Count > 0)
				{
					_ret = new PossibleKey(Relation);
					_ret.Attribute = _cols;
				}

				return _ret;
			}

		}

	}
}
