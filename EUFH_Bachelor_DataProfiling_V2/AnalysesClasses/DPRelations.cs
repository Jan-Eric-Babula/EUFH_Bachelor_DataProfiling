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

			//Primary Key required for Foreign Key
			if (RelationsWithKeys.Count > 0)
			{



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
		{_joinon}d
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



			private static string Make_AttributeList(List<string> Attributes)
			{
				string _ret = "";

				foreach (string s in Attributes)
				{
					_ret += $"[{s}]";
					_ret += s == Attributes.Last() ? "" : ",";
				}

				return _ret;
			}
		}
	}
}
