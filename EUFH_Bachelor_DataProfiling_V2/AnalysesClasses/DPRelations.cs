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
	class DPRelationen
	{

		public static AErgRelationen Analysis(string Database)
		{
			AErgRelationen _Ret = new AErgRelationen(Database);

			_Ret.DocumentedReferences = DPRelationen_Helper.Get_DocumentedReferences();

			Dictionary<string, PossibleKey> RelationsWithKeys = DPRelationen_Helper.Find_RelationsWithKeys();
			int _i = 0;
			//Primary Key required for Foreign Key
			if (RelationsWithKeys.Count > 0)
			{
				_Ret.FoundReferences = new List<PossibleReference>();
				foreach (string _r in RelationsWithKeys.Keys)
				{
					LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name} - {_i}/{RelationsWithKeys.Count} - {_r}");
					
					//Only test keys with 1 Attribute - - Double cross to complex
					if (RelationsWithKeys[_r].Attributes.Count == 1)
					{
						string _a = RelationsWithKeys[_r].Attributes.First();
						ColumnBasicProperties _a_p = DPRelationen_Helper.Get_ColumnBasicProps(_r, _a);
						int _j = 0, _v = 0;
						Dictionary<string, List<string>> _a_similar = DPRelationen_Helper.Get_SimilarColumns(_a_p);
						_a_similar.Remove(_r);

						foreach (string __r in _a_similar.Keys)
						{
							_v = 0;
							foreach (string __a in _a_similar[__r])
							{
								LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name} - {_i}/{RelationsWithKeys.Count} - {_r} - Checking {_j} ({__r}) on {_v}({__a})");
								_Ret.FoundReferences.Add(DPRelationen_Helper.Test_Reference(RelationsWithKeys[_r], __r, __a));
								_v++;
								
							}
							_j++;
						}
					}
					_i++;
				}

			}

			return _Ret;
		}

		private class DPRelationen_Helper
		{
			public static List<PossibleReference> Get_DocumentedReferences()
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				Dictionary<string, PossibleReference> RefNameToObject = new Dictionary<string, PossibleReference>();

				List<string> AllDocRefNames = Get_AllDocRefNames();

				if (AllDocRefNames.Count > 0)
				{
					foreach (string s in Get_AllDocRefNames())
					{
						PossibleReference _tmp = new PossibleReference()
						{
							PK_Relation = Get_DocRef_PK_Relation(s),
							FK_Relation = Get_DocRef_FK_Relation(s),
							FK_Attributes = Get_DocRef_FK_Attributes(s)
						};

						_tmp.Childless = Get_PRStat_Childless(_tmp);
						_tmp.Parents = Get_PRStat_Parents(_tmp);
						_tmp.Orphans = 0;

						RefNameToObject.Add(s, _tmp);
					}
				}

				return RefNameToObject.Values.ToList();
			}

			private static List<string> Get_AllDocRefNames()
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				List<string> _Ret = new List<string>();

				string sql_cmd_str = $@"
SELECT CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					if (!_DR.IsDBNull(0))
					{
						_Ret.Add(_DR.GetString(0));
					}
				}

				_DR.Close();

				return _Ret;
			}

			private static PossibleKey Get_DocRef_PK_Relation(string DocRefName)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT TOP 1 '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' 
FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE 
WHERE CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME = (
	SELECT TOP 1 UNIQUE_CONSTRAINT_SCHEMA+'.'+UNIQUE_CONSTRAINT_NAME 
	FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS 
	WHERE CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME = '{DocRefName}'
);
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				string _tmp = _DR.IsDBNull(0) ? null : _DR.GetString(0);

				_DR.Close();

				if (string.IsNullOrWhiteSpace(_tmp))
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return DPAnalysis.TupelAnalyse_Result_Sort[_tmp].DocumentedKey;
			}

			private static string Get_DocRef_FK_Relation(string DocRefName)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' FROM INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE 
WHERE CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME='{DocRefName}';
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				string _tmp = _DR.IsDBNull(0) ? null : _DR.GetString(0);

				_DR.Close();

				if (string.IsNullOrWhiteSpace(_tmp))
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _tmp;
			}

			private static List<string> Get_DocRef_FK_Attributes(string DocRefName)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				List<string> _Ret = new List<string>();

				string sql_cmd_str = $@"
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE 
WHERE CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME = '{DocRefName}';
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					if (!_DR.IsDBNull(0))
					{
						_Ret.Add(_DR.GetString(0));
					}
				}

				_DR.Close();

				if (_Ret.Count == 0)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _Ret;
			}

			private static string Get_PRStat_Select(PossibleReference _PR)
			{
				int i = 0, j = 0;
				string _select = "";
				foreach (string _a in _PR.FK_Attributes)
				{
					_select += $"T_FK.[{_a}] FK{i},";
					i++;
				}
				foreach (string _a in _PR.PK_Relation.Attributes)
				{
					_select += $"T_PK.[{_a}] PK{j}{(_a == _PR.PK_Relation.Attributes.Last() ? "" : ",")}";
					j++;
				}
				return _select;
			}

			private static string Get_PRStat_JoinOn(PossibleReference _PR)
			{
				string _joinon = "";

				for (int i = 0; i < _PR.FK_Attributes.Count; i++)
				{
					_joinon += $"T_FK.[{_PR.FK_Attributes[i]}] = T_PK.[{_PR.PK_Relation.Attributes[i]}] {(i < _PR.FK_Attributes.Count-1 ? "AND" : " ")}";
				}

				return _joinon;
			}

			private static long Get_PRStat_Childless(PossibleReference _PR)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string _select = Get_PRStat_Select(_PR);

				string _joinon = Get_PRStat_JoinOn(_PR);
				
				string _where = "";
				for (int i = 0; i < _PR.FK_Attributes.Count; i++)
				{
					_where += $"FK{i} IS NULL {(i < _PR.FK_Attributes.Count-1 ? "AND" : " ")}";
				}

				string sql_cmd_str = $@"
SELECT 
	COUNT_BIG(*)
FROM (
	SELECT 
		{_select}
	FROM 
		{_PR.FK_Relation} T_FK 
		RIGHT OUTER JOIN 
		{_PR.PK_Relation.Relation} T_PK 
	ON 
		{_joinon}
) K
WHERE {_where}
;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? _c = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

				_DR.Close();

				if (!_c.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _c.Value;
			}
			private static long Get_PRStat_Parents(PossibleReference _PR)
			{

				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string _select = Get_PRStat_Select(_PR);

				string _joinon = Get_PRStat_JoinOn(_PR);

				string sql_cmd_str = $@"
SELECT 
	COUNT_BIG(*)
FROM (
	SELECT 
		{_select}
	FROM 
		{_PR.FK_Relation} T_FK 
		INNER JOIN 
		{_PR.PK_Relation.Relation} T_PK 
	ON 
		{_joinon}
) K
;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? _c = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

				_DR.Close();

				if (!_c.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _c.Value;
			}
			private static long Get_PRStat_Orphans(PossibleReference _PR)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string _select = Get_PRStat_Select(_PR);

				string _joinon = Get_PRStat_JoinOn(_PR);

				string _where = "";
				for (int i = 0; i < _PR.FK_Attributes.Count; i++)
				{
					_where += $"PK{i} IS NULL {(i < _PR.FK_Attributes.Count - 1 ? "AND" : " ")}";
				}

				string sql_cmd_str = $@"
SELECT 
	COUNT_BIG(*)
FROM (
	SELECT 
		{_select}
	FROM 
		{_PR.FK_Relation} T_FK 
		LEFT OUTER JOIN 
		{_PR.PK_Relation.Relation} T_PK 
	ON 
		{_joinon}
) K
WHERE {_where}
;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? _c = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

				_DR.Close();

				if (!_c.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _c.Value;
			}

			public static Dictionary<string, PossibleKey> Find_RelationsWithKeys()
			{
				Dictionary<string, PossibleKey> _Ret = new Dictionary<string, PossibleKey>();

				foreach (var _a in DPAnalysis.TupelAnalyse_Results)
				{
					if (_a.DocumentedKey != null)
					{
						_Ret.Add(_a.Relation, _a.DocumentedKey);
					}
					else if (_a.PossibleKeys != null)
					{
						if (_a.PossibleKeys.Count == 1)
						{
							if (_a.PossibleKeys.First().Coverage == 1)
							{
								_Ret.Add(_a.Relation, _a.PossibleKeys.First());
							}
						}
					}
				}

				return _Ret;
			}

			public static ColumnBasicProperties Get_ColumnBasicProps(string Relation, string Attribute)
			{
				ColumnBasicProperties _Ret = new ColumnBasicProperties();

				string sql_cmd_str = $@"
SELECT
	DATA_TYPE,
	CHARACTER_MAXIMUM_LENGTH,
	CHARACTER_OCTET_LENGTH,
	NUMERIC_PRECISION,
	NUMERIC_PRECISION_RADIX,
	NUMERIC_SCALE,
	DATETIME_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' = '{Relation}' AND COLUMN_NAME = '{Attribute}';
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				_Ret.Datatype = _DR.IsDBNull(0) ? null : _DR.GetString(0);
				_Ret.CharMaxLen = _DR.IsDBNull(1) ? (int?)null : _DR.GetInt32(1);
				_Ret.CharOctLen = _DR.IsDBNull(2) ? (int?)null : _DR.GetInt32(2);
				_Ret.NumPrecision = _DR.IsDBNull(3) ? (byte?)null : _DR.GetByte(3);
				_Ret.NumPrecisionRad = _DR.IsDBNull(4) ? (short?)null : _DR.GetInt16(4);
				_Ret.NumScale = _DR.IsDBNull(5) ? (int?)null : _DR.GetInt32(5);
				_Ret.DTPrecision = _DR.IsDBNull(6) ? (short?)null : _DR.GetInt16(6);

				_DR.Close();

				if (string.IsNullOrWhiteSpace(_Ret.Datatype))
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return _Ret;
			}

			public static Dictionary<string,List<string>> Get_SimilarColumns(ColumnBasicProperties _CBP)
			{
				Dictionary<string, List<string>> _Ret = new Dictionary<string, List<string>> ();

				string sql_cmd_str = $@"
SELECT
	'['+CS.TABLE_SCHEMA+'].['+CS.TABLE_NAME+']',
	CS.COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS CS
LEFT JOIN INFORMATION_SCHEMA.TABLES TS
ON CS.TABLE_SCHEMA=TS.TABLE_SCHEMA AND CS.TABLE_NAME=TS.TABLE_NAME
WHERE 
CS.DATA_TYPE = '{_CBP.Datatype}' AND
CS.CHARACTER_MAXIMUM_LENGTH {(_CBP.CharMaxLen.HasValue ? $"= {_CBP.CharMaxLen.Value}" : $"IS NULL")} AND
CS.CHARACTER_OCTET_LENGTH {(_CBP.CharOctLen.HasValue ? $"= {_CBP.CharOctLen.Value}" : $"IS NULL")} AND
CS.NUMERIC_PRECISION {(_CBP.NumPrecision.HasValue ? $"= {_CBP.NumPrecision.Value}" : $"IS NULL")} AND
CS.NUMERIC_PRECISION_RADIX {(_CBP.NumPrecisionRad.HasValue ? $"= {_CBP.NumPrecisionRad.Value}" : $"IS NULL")} AND
CS.NUMERIC_SCALE {(_CBP.NumScale.HasValue ? $"= {_CBP.NumScale.Value}" : $"IS NULL")} AND
CS.DATETIME_PRECISION {(_CBP.DTPrecision.HasValue ? $"= {_CBP.DTPrecision.Value}" : $"IS NULL")} AND
CS.TABLE_SCHEMA+CS.TABLE_NAME+CS.COLUMN_NAME NOT IN (
	SELECT CCU.TABLE_SCHEMA+CCU.TABLE_NAME+CCU.COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS  TC
	JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU
	ON TC.CONSTRAINT_SCHEMA+TC.CONSTRAINT_NAME=CCU.CONSTRAINT_SCHEMA+CCU.CONSTRAINT_NAME
	WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
)
AND TS.TABLE_TYPE = 'BASE TABLE';
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					string _rel = _DR.IsDBNull(0) ? null : _DR.GetString(0);
					string _atr = _DR.IsDBNull(1) ? null : _DR.GetString(1);

					if (string.IsNullOrWhiteSpace(_rel) || string.IsNullOrWhiteSpace(_atr))
					{
						throw new ArgumentNullException("SQL Query returned invalid NULL value!");
					}

					if (_Ret.Keys.Contains(_rel))
					{
						_Ret[_rel].Add(_atr);
					}
					else
					{
						_Ret.Add(_rel, new List<string>(new string[]{ _atr }));
					}
				}

				_DR.Close();


				return _Ret;

			}

			public static PossibleReference Test_Reference(PossibleKey PK, string R_FK, string C_FK)
			{
				PossibleReference _tmp = new PossibleReference()
				{
					PK_Relation = PK,
					FK_Relation = R_FK,
					FK_Attributes = new List<string>(new string[] { C_FK })
				};

				_tmp.Childless = Get_PRStat_Childless(_tmp);
				_tmp.Parents = Get_PRStat_Parents(_tmp);
				_tmp.Orphans = Get_PRStat_Orphans(_tmp);

				return _tmp;
			}
		}
	}
}
