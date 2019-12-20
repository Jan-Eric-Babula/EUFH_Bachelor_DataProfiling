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
			AErgTupel _Ret = new AErgTupel(Database, Relation, DPAnalysis.AttributAnalyse_Results[Relation].First().Count_Rows.Value);

			_Ret.DocumentedKey = DPTupel_Helper.Get_DocumentedKey(Relation);

			_Ret.FunctionalDependencyGrid = DPTupel_Helper.CreateDependencyMatrix(_Ret.Relation);

			



			return _Ret;
		}

		private class DPTupel_Helper
		{

			public static Dictionary<List<string>, Dictionary<string, bool>> CreateDependencyMatrix(string Relation)
			{

				Dictionary<List<string>, Dictionary<string, bool>> _Ret = CheckSingleAttributeDependencies(Relation);

				Dictionary<List<string>, Dictionary<string, bool>> _multi = CheckMutiAttributeDependencies(Relation);

				foreach (var kvp in _multi)
				{
					_Ret.Add(kvp.Key, kvp.Value);
				}

				return _Ret;

			}

			private static Dictionary<List<string>, Dictionary<string, bool>> CheckSingleAttributeDependencies(string Relation)
			{
				Dictionary<List<string>, Dictionary<string, bool>> _Ret = new Dictionary<List<string>, Dictionary<string, bool>>();

				foreach (AErgAttribut A1 in DPAnalysis.AttributAnalyse_Results[Relation])
				{
					Dictionary<string, bool> _tmp = new Dictionary<string, bool>();

					foreach (AErgAttribut A2 in DPAnalysis.AttributAnalyse_Results[Relation])
					{
						if (A1 != A2)
						{
							_tmp.Add(A2.AttributeName, CheckDependency(Relation, new List<string>(new string[] { A1.AttributeName }), A2.AttributeName));
						}
					}

					_Ret.Add(new List<string>(new string[] { A1.AttributeName }), _tmp);
				}

				return _Ret;
			}

			private static Dictionary<List<string>, Dictionary<string, bool>> CheckMutiAttributeDependencies(string Relation)
			{
				Dictionary<List<string>, Dictionary<string, bool>> _Ret = new Dictionary<List<string>, Dictionary<string, bool>>();

				//TODO

				return _Ret;
			}

			//TODO Implement query builder
			private static bool CheckDependency(string Relation, List<string> Attribute1, string Attribute2)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT
	CAST(CASE WHEN COUNT(K.A) > 0 THEN 0 ELSE 1 END AS BIT)
FROM (
SELECT TOP 1 [{Attribute1.First()}] A
FROM {Relation}
GROUP BY [{Attribute1.First()}]
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

			public static PossibleKey Get_DocumentedKey(string Relation)
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
					_ret.Attributes = _cols;
				}

				return _ret;
			}

		}

	}
}
