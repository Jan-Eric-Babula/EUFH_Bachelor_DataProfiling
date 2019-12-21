using EUFH_Bachelor_DataProfiling_V2.HelperObjects;
using EUFH_Bachelor_DataProfiling_V2.ResultObjects;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using System.IO;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.AnalysesClasses
{
	class DPTupel
	{

		public static AErgTupel Analysis(string Database, string Relation)
		{
			AErgTupel _Ret = new AErgTupel(Database, Relation, DPAnalysis.AttributAnalyse_Results[Relation].First().Count_Rows.Value);

			_Ret.DocumentedKey = DPTupel_Helper.Get_DocumentedKey(Relation);

			_Ret.DocumentedDpenedency = DPTupel_Helper.Get_DocumentedDependencies(Relation);

			_Ret.FunctionalDependencyGrid = DPTupel_Helper.CreateDependencyMatrixA(_Ret.Relation);

			if (_Ret.FunctionalDependencyGrid != null)
			{



			}

			return _Ret;
		}

		private class DPTupel_Helper
		{

			public static Dictionary<string, Dictionary<string, bool?>> CreateDependencyMatrix(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				if (DPAnalysis.AttributAnalyse_Results_Sort[Relation].First().Value.Count_Rows > 0)
				{
					Dictionary<string, Dictionary<string, bool?>> _Ret = new Dictionary<string, Dictionary<string, bool?>>();
					List<string> Attributes = DPAnalysis.AttributAnalyse_Results[Relation].Select(i => i.AttributeName).ToList();
					List<List<string>> AttributesCombined = RootCombine(Attributes, null, 2);
					AttributesCombined.Sort((a, b) => a.Count.CompareTo(b.Count));

					foreach (List<string> PotKeyComb in AttributesCombined)
					{
						string PotKeyComb_str = Make_AttributeList(PotKeyComb);
						_Ret.Add(PotKeyComb_str, new Dictionary<string, bool?>());

						foreach (string LocDep in Attributes)
						{
							if (DPAnalysis.AttributAnalyse_Results_Sort[Relation][LocDep].Count_Attribute > 0)
							{
								if (PotKeyComb.Contains(LocDep))
								{
									_Ret[PotKeyComb_str].Add(LocDep, (bool?)null);
								}
								else
								{
									_Ret[PotKeyComb_str].Add(LocDep, CheckDependency(Relation, PotKeyComb, LocDep));
								}
							}
						}
					}

					return _Ret;
				}
				else
				{
					return null;
				}
				
			}

			private static readonly int parallelConnections = 50;

			public static Dictionary<string, Dictionary<string, bool?>> CreateDependencyMatrixA(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				

				if (DPAnalysis.AttributAnalyse_Results_Sort[Relation].First().Value.Count_Rows > 0)
				{
					List<Task<DependencyResult>> _task_queue = new List<Task<DependencyResult>>();
					

					Dictionary<string, Dictionary<string, bool?>> _Ret = new Dictionary<string, Dictionary<string, bool?>>();
					List<string> Attributes = DPAnalysis.AttributAnalyse_Results[Relation].Select(i => i.AttributeName).ToList();
					List<List<string>> AttributesCombined = RootCombine(Attributes, null, 2);
					AttributesCombined.Sort((a, b) => a.Count.CompareTo(b.Count));

					long count = (long)Attributes.Count * (long)AttributesCombined.Count;
					long j = 0;

					DateTime _task_start = DateTime.Now;

					foreach (List<string> PotKeyComb in AttributesCombined)
					{
						string PotKeyComb_str = Make_AttributeList(PotKeyComb);
						_Ret.Add(PotKeyComb_str, new Dictionary<string, bool?>());

						foreach (string LocDep in Attributes)
						{
							
							if (DPAnalysis.AttributAnalyse_Results_Sort[Relation][LocDep].Count_Attribute > 0)
							{
								if (PotKeyComb.Contains(LocDep))
								{
									_Ret[PotKeyComb_str].Add(LocDep, (bool?)null);
								}
								else
								{
									var _loc_task = Task.Run(async () => await CheckDependencyA(Relation, PotKeyComb, LocDep));
									_task_queue.Add(_loc_task);

									if (_task_queue.Count >= parallelConnections)
									{
										LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: Awaiting last {_task_queue.Count} requests... ( {j} / {count} )");
										Task.WhenAll(_task_queue).Wait();

										foreach (var _t in _task_queue)
										{
											_Ret[_t.Result.Keys].Add(_t.Result.Dependant, _t.Result.Result );
											LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: {Relation} - Checked [{_t.Result.Dependant}] for {_t.Result.Keys}");

										}
										LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: {_task_queue.Count} Tasks took {DateTime.Now - _task_start}");
										_task_queue.Clear();
										_task_start = DateTime.Now;
									}
								}
							}
							j++;
						}
					}

					LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: Resolving last requests ({_task_queue.Count})... ( {j} / {count} )");
					Task.WhenAll(_task_queue).Wait();
					foreach (var _t in _task_queue)
					{
						_Ret[_t.Result.Keys].Add(_t.Result.Dependant, _t.Result.Result);
						LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: {Relation} - Checked [{_t.Result.Dependant}] for {_t.Result.Keys}");

					}
					LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: {_task_queue.Count} Tasks took {DateTime.Now - _task_start}");
					_task_queue.Clear();

					return _Ret;
				}
				else
				{
					return null;
				}

			}

			private class DependencyResult
			{
				public bool Result
				{
					get;
					set;
				}
				public string Keys
				{
					get;
					set;
				}
				public string Dependant
				{
					get;
					set;
				}
			}

			private static List<List<string>> RootCombine(List<string> _inp, List<string> _pre = null, int maxlen = -1)
			{
				_inp = new List<string>(_inp);
				_pre = _pre == null ? null : new List<string>(_pre);
				List<List<string>> _out = new List<List<string>>();

				//Cancel | End of Root
				if (_inp.Count == 1)
				{

					if (_pre == null)
					{
						_out.Add(_inp);
					}
					else
					{
						_pre.AddRange(_inp);
						_out.Add(_pre);
					}

				}
				//Grow Root
				else
				{
					if (_pre == null)
					{
						_pre = new List<string>();
					}

					//Create new Prefix
					_pre.Add(_inp.First());
					//Remove First from inp
					_inp.Remove(_inp.First());
					//Add self to Output
					_out.Add(_pre);

					if (maxlen == -1 || _pre.Count < maxlen)
					{
						//Grow Vertical | Grow Deeper
						_out.AddRange(GrowDeeper(_inp, _pre, maxlen));

						//Grow Horizontal
						if (_pre.Count - 1 == 0)
						{
							_out.AddRange(RootCombine(_inp, null, maxlen));
						}
					}
				}

				return _out;
			}

			private static List<List<string>> GrowDeeper(List<string> _inp, List<string> _pre, int maxlen = -1)
			{
				_inp = new List<string>(_inp);
				_pre = new List<string>(_pre);
				List<List<string>> _out = new List<List<string>>();

				int max = _inp.Count;
				for (int i = 0; i < max; i++)
				{

					_out.AddRange(RootCombine(_inp, _pre, maxlen));
					_inp.Remove(_inp.First());

				}

				return _out;
			}

			//TODO Implement query builder
			private static bool CheckDependency(string Relation, List<string> Keys, string Dependant)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}: Checking [{Dependant}] for {Make_AttributeList(Keys)}");

				string str_keys = Make_AttributeList(Keys);
				if (string.IsNullOrWhiteSpace(str_keys))
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				string sql_cmd_str = $@"
SELECT
	CAST(CASE WHEN COUNT(K.A) > 0 THEN 0 ELSE 1 END AS BIT)
FROM (
SELECT TOP 1 {str_keys} A
FROM {Relation}
GROUP BY {str_keys}
HAVING COUNT (DISTINCT [{Dependant}]) > 1) K;
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

			private static async Task<DependencyResult> CheckDependencyA(string Relation, List<string> Keys, string Dependant)
			{
				string str_keys = Make_AttributeList(Keys);
				if (string.IsNullOrWhiteSpace(str_keys))
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				bool? fd = (bool?)null;

				string sql_cmd_str = $@"
SELECT
	CAST(CASE WHEN COUNT(K.A) > 0 THEN 0 ELSE 1 END AS BIT)
FROM (
SELECT TOP 1 {str_keys} A
FROM {Relation}
GROUP BY {str_keys}
HAVING COUNT (DISTINCT [{Dependant}]) > 1) K;
";

				using (SqlConnection _con = new SqlConnection(DBManager.ConnectionString))
				{
					_con.Open();
					using (SqlCommand _cmd = new SqlCommand(sql_cmd_str, _con) { CommandTimeout = 150 })
					{
						using (SqlDataReader _dr = _cmd.ExecuteReader())
						{
							_dr.Read();

							fd = _dr.IsDBNull(0) ? (bool?)null : _dr.GetBoolean(0);

							
						}
					}
				}

				if (!fd.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				return new DependencyResult() { Result = fd.Value, Keys = str_keys, Dependant = Dependant } ;
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

			public static List<PossibleDependency> Get_DocumentedDependencies(string Relation)
			{
				List<string> _InternalNames = Get_DocumentedDependencies_Names(Relation);
				List<PossibleDependency> _Ret = new List<PossibleDependency>();

				foreach (string _in in _InternalNames)
				{
					_Ret.Add(Get_DocumentedDependencies_Details(_in, Relation));
				}

				return _Ret;
			}

			private static List<string> Get_DocumentedDependencies_Names(string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				List<string> _Ret = new List<string>();

				string sql_cmd_str = $@"
SELECT CONSTRAINT_SCHEMA+'.'+CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
WHERE CONSTRAINT_TYPE = 'CHECK' AND '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' = '{Relation}';
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
			private static PossibleDependency Get_DocumentedDependencies_Details(string DependencyName, string Relation)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				PossibleDependency _Ret = new PossibleDependency(DPAnalysis.Database, Relation);
				List<string> _Tmp = new List<string>();

				string sql_cmd_str = $@"
SELECT CCU.COLUMN_NAME,CC.CHECK_CLAUSE FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU
JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS CC ON CCU.CONSTRAINT_SCHEMA+'.'+CCU.CONSTRAINT_NAME = CC.CONSTRAINT_SCHEMA+'.'+CC.CONSTRAINT_NAME
WHERE CCU.CONSTRAINT_SCHEMA+'.'+CCU.CONSTRAINT_NAME = '{DependencyName}';
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					if (!_DR.IsDBNull(0))
					{
						_Tmp.Add(_DR.GetString(0));
					}
					if (!_DR.IsDBNull(1) && _Ret.Note == null)
					{
						_Ret.Note = _DR.GetString(1);
					}
				}

				_DR.Close();

				return _Ret;
			}
		}

	}
}
