﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Data.SqlClient;
using System.Reflection;
using EUFH_Bachelor_DataProfiling_V2.HelperObjects;
using EUFH_Bachelor_DataProfiling_V2.AnalysesClasses;
using EUFH_Bachelor_DataProfiling_V2.ResultObjects;

namespace EUFH_Bachelor_DataProfiling_V2.HelperClasses
{
	class DPAnalysis
	{

		public static string Database;
		public static List<string> Relations;

		public static void Run()
		{

			InitAnalyse();

			AttributAnalyse();

			TupelAnalyse();

			RelationenAnalyse();

			Export();

		}

		private static void InitAnalyse()
		{

			LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

			Database = DPAnalysis_Helper.GetDBName();

			Relations = DPAnalysis_Helper.GetDBRelations();

			DPAnalysis_Helper.GenerateOutputStructure(Database, Relations);

		}

		public static Dictionary<string, List<AErgAttribut>> AttributAnalyse_Results = new Dictionary<string, List<AErgAttribut>>();

		private static void AttributAnalyse()
		{
			int i = 0, j = 0;
			
			foreach (string Relation in Relations)
			{
				LogHelper.LogApp($"{i+1}) {Relation}");

				long RowCount = DPAnalysis_Helper.GetRelationRowCount(Relation);

				List<AttributeBaseData> L_ABD = DPAnalysis_Helper.GetAttributesBaseData(Relation, RowCount);

				List<AErgAttribut> _tmp = new List<AErgAttribut>();

				j = 0;

				foreach (AttributeBaseData ABD in L_ABD)
				{


					LogHelper.LogApp($"{i + 1}.{j + 1}) {Relation}.[{ABD.AttributeName}]");

					try
					{
						AErgAttribut _loc = DPAttribut.Analysis(Database, Relation, ABD.AttributeName, ABD);

						_tmp.Add(_loc);
					}
					catch (Exception e)
					{
						LogHelper.LogAppError(e);
					}

					j++;
				}

				AttributAnalyse_Results.Add(Relation, _tmp);
				i++;
			}
		}

		public static List<AErgTupel> TupelAnalyse_Results = new List<AErgTupel>();
		public static Dictionary<string, Dictionary<string, AErgAttribut>> AttributAnalyse_Results_Sort = new Dictionary<string, Dictionary<string, AErgAttribut>>();

		private static void TupelAnalyse()
		{
			//Reformat
			foreach (var _a in AttributAnalyse_Results.Keys)
			{
				Dictionary<string, AErgAttribut> _tmp = new Dictionary<string, AErgAttribut>();

				foreach (var _b in AttributAnalyse_Results[_a])
				{
					_tmp.Add(_b.AttributeName, _b);
				}

				AttributAnalyse_Results_Sort.Add(_a, _tmp);
			}
			//\\

			int i = 0;
			foreach (string rel in Relations)
			{
				LogHelper.LogApp($"{i}) {rel}");
				TupelAnalyse_Results.Add(DPTupel.Analysis(Database, rel));

				i++;
			}
		}

		public static AErgRelationen RelationenAnalysis_Result = null;
		public static Dictionary<string, AErgTupel> TupelAnalyse_Result_Sort = new Dictionary<string, AErgTupel>();

		private static void RelationenAnalyse()
		{
			LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

			//Reformat
			foreach (var _a in TupelAnalyse_Results)
			{
				TupelAnalyse_Result_Sort.Add(_a.Relation, _a);
			}

			RelationenAnalysis_Result = DPRelationen.Analysis(Database);

		}

		private static void Export()
		{

			LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

			foreach (List<AErgAttribut> resultList in AttributAnalyse_Results.Values)
			{
				ExportHelper.Export_AttributeAnalyis(resultList);
			}

			ExportHelper.Export_TupelAnalysis(TupelAnalyse_Results);

			ExportHelper.Export_RelationAnalysis(RelationenAnalysis_Result);

		}

		private class DPAnalysis_Helper
		{
			public static string GetDBName()
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"SELECT DB_NAME();";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				string _Ret = _DR.IsDBNull(0) ? null : _DR.GetString(0);

				_DR.Close();

				if (_Ret == null)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _Ret;
			}

			public static List<string> GetDBRelations()
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT 
'['+TABLE_SCHEMA+'].['+TABLE_NAME+']'
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_SCHEMA, TABLE_NAME;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				List<string> _Ret = new List<string>();
				string _tmp = null;

				while (_DR.Read())
				{
					if (!_DR.IsDBNull(0))
					{
						_tmp = _DR.GetString(0);
						if (!string.IsNullOrWhiteSpace(_tmp))
						{
							_Ret.Add(_tmp);
						} 
					}
				}

				_DR.Close();

				if (_Ret.Count == 0)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL/Empty value!");
				}

				return _Ret;
			}

			public static void GenerateOutputStructure(string Database, List<string> Relations)
			{
				if (!Directory.Exists(Database))
				{
					Directory.CreateDirectory(Database);
				}

				string _tmp = null;

				foreach (string _Relation in Relations)
				{
					_tmp = $@"{Database}\{_Relation}";

					if (Directory.Exists(_tmp))
					{
						Directory.CreateDirectory(_tmp);
					}
				}

				foreach (string _Path in Directory.GetDirectories(Database))
				{
					if (!Relations.Contains(_Path.Split(new char[] { '\\' }).Last()))
					{
						Directory.Delete(_Path, true);
					}
				}
			}

			public static long GetRelationRowCount(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"SELECT COUNT_BIG(*) FROM {Relation};";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? _Ret = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

				_DR.Close();

				if (!_Ret.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _Ret.Value;
			}

			public static List<AttributeBaseData> GetAttributesBaseData(string Relation, long RowCount)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT
	COLUMN_NAME 
,	ORDINAL_POSITION
,	COLUMN_DEFAULT
,	IS_NULLABLE
,	DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' = '{Relation}' 
ORDER BY ORDINAL_POSITION ASC;
";

				List<AttributeBaseData> _Ret = new List<AttributeBaseData>();

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					_Ret.Add(
						new AttributeBaseData(
							_DR.GetString(0),
							_DR.GetInt32(1),
							_DR.IsDBNull(2) ? null : _DR.GetString(2),
							_DR.GetString(3)=="YES",
							_DR.GetString(4),
							RowCount
						)
					);
				}

				_DR.Close();

				return _Ret;
			}
		}
	}
}
