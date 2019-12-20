using EUFH_Bachelor_DataProfiling_V2.ResultObjects;
using EUFH_Bachelor_DataProfiling_V2.HelperObjects;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.AnalysesClasses
{
	class DPAttribut
	{
		public static readonly int DOMAIN_SIMPLE_BORDER = 25;

		public static AErgAttribut Analysis(string Database, string Relation, string Attribute, AttributeBaseData _ABD)
		{
			AErgAttribut _Ret = new AErgAttribut(Database, Relation, Attribute, _ABD.OrdinalPosition)
			{
				Count_Rows = _ABD.RowCount,
				Empty_Allowed = _ABD.IsNullable,
				Datatype_Documented = _ABD.DataType,
				DefaultValue = _ABD.ColumnDefault
			};

			if (_Ret.Count_Rows > 0)
			{

				DPAttribut_Helper.Get_Count(_Ret);

				if (_Ret.Count_Attribute > 0)
				{

					DPAttribut_Helper.Get_Count_Distinct(_Ret);

					if (_Ret.Datatype_Documented != "timestamp" && _Ret.Datatype_Documented != "image")
					{
						DPAttribut_Helper.Get_StringLength(_Ret);
					}

					string upscale = DPAttribut_Helper.Get_NumUpscale(_Ret.Datatype_Documented);

					if (upscale != null)
					{

						DPAttribut_Helper.Get_Statistics(_Ret, upscale);

						DPAttribut_Helper.Get_Histogramm(_Ret);

						DPAttribut_Helper.Get_Quartile(_Ret);

						DPAttribut_Helper.Get_Benford(_Ret);
					}
					else
					{
						DPAttribut_Helper.Get_Text_Order(_Ret);
					}

					DPAttribut_Helper.Get_ModeValue(_Ret);

					_Ret.Datatype_Primitive = DPAttribut_Helper.Get_PrimitiveDatatype(_Ret.Datatype_Documented);

					if (_Ret.Datatype_Primitive == "TEXT")
					{

						DPAttribut_Helper.Get_PrimitiveDatatypeProperties(_Ret);

						if (_Ret.Datatype_Primitive_Properties["NUM"] > 0)
						{

							DPAttribut_Helper.Get_TextToNumKonversions(_Ret);

						}

						DPAttribut_Helper.Get_TrimValues(_Ret);

					}

					DPAttribut_Helper.Get_SimpleDomain(_Ret);
				}

			}

			return _Ret;
		}

		private class DPAttribut_Helper
		{

			public static void Get_Count(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				if (_Ret.Empty_Allowed.Value)
				{
					string sql_cmd_str = $@"
SELECT COUNT_BIG(T.[{_Ret.AttributeName}]) [C_A] FROM {_Ret.Relation} T;
";

					SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

					_DR.Read();

					long? ca_tot = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

					_DR.Close();

					if (!ca_tot.HasValue)
					{
						throw new ArgumentNullException("SQL Query returned invalid NULL value!");
					}

					_Ret.Count_Attribute = ca_tot;
					_Ret.Count_Attribute_Relative = ((decimal)ca_tot) / ((decimal)_Ret.Count_Rows);

					_Ret.Count_Empty = _Ret.Count_Rows - _Ret.Count_Attribute;
					_Ret.Count_Empty_Relative = ((decimal)_Ret.Count_Empty) / ((decimal)_Ret.Count_Rows);
				}
				else
				{
					_Ret.Count_Attribute = _Ret.Count_Rows;
					_Ret.Count_Attribute_Relative = (decimal)1.0;
				}
			}

			public static void Get_Count_Distinct(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT COUNT_BIG(*) [C] FROM (
SELECT DISTINCT T.[{_Ret.AttributeName}] FROM { _Ret.Relation} T) V;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? cd_tot = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);

				_DR.Close();

				if (!cd_tot.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.Count_Distinct = cd_tot.Value;
				_Ret.Count_Distinct_Relative = ((decimal)cd_tot.Value) / ((decimal)_Ret.Count_Rows.Value);
			}

			public static void Get_StringLength(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT 
	CAST(MIN(K.[LEN]) AS BIGINT)
,	CAST(AVG(K.[LEN]) AS DECIMAL)
,	CAST(MAX(K.[LEN]) AS BIGINT) FROM (
SELECT LEN(V.A_T) [LEN] FROM (
SELECT CAST(T.[{_Ret.AttributeName}] AS NVARCHAR) [A_T] FROM {_Ret.Relation} T) V) K;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? sl_min = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);
				decimal? sl_avg = _DR.IsDBNull(1) ? (decimal?)null : _DR.GetDecimal(1);
				long? sl_max = _DR.IsDBNull(2) ? (long?)null : _DR.GetInt64(2);

				_DR.Close();

				if (!sl_min.HasValue || !sl_avg.HasValue || !sl_max.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.StringLength_Min = sl_min;
				_Ret.StringLength_Avg = sl_avg;
				_Ret.StringLength_Max = sl_max;
			}

			private static readonly Dictionary<string, string> upscale = new Dictionary<string, string>() {
				{ "real" ,"float" },{ "float" ,"float" },

			{ "tinyint" ,"bigint" },{ "smallint" ,"bigint" },
			{ "int" ,"bigint" },{ "bigint" ,"bigint" },

			{ "smallmoney" ,"money" },{ "money" ,"money" },

			{ "numeric" ,"numeric" },{ "decimal" ,"decimal" }
			};

			public static string Get_NumUpscale(string Datatype)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				return upscale.Keys.Contains(Datatype) ? upscale[Datatype] : null;
			}

			public static void Get_Statistics(AErgAttribut _Ret, string _Upscale)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT 
	CAST(MIN(V.U) AS FLOAT)
,	CAST(AVG(V.U) AS FLOAT)
,	CAST(MAX(V.U) AS FLOAT)
,	CAST(STDEV(V.U) AS FLOAT) 
,	CAST(SUM(V.U) AS FLOAT)
FROM (
SELECT CAST(T.[{_Ret.AttributeName}] AS {_Upscale}) AS [U] FROM {_Ret.Relation} T) V;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				decimal? st_min = _DR.IsDBNull(0) ? (decimal?)null : (decimal)_DR.GetDouble(0);
				decimal? st_avg = _DR.IsDBNull(1) ? (decimal?)null : (decimal)_DR.GetDouble(1);
				decimal? st_max = _DR.IsDBNull(2) ? (decimal?)null : (decimal)_DR.GetDouble(2);
				decimal? st_stv = _DR.IsDBNull(3) ? (decimal?)null : (decimal)_DR.GetDouble(3);
				decimal? st_sum = _DR.IsDBNull(4) ? (decimal?)null : (decimal)_DR.GetDouble(4);

				_DR.Close();

				if (!st_min.HasValue || !st_avg.HasValue || !st_max.HasValue || !st_stv.HasValue || !st_sum.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.Statistics_Min = st_min;
				_Ret.Statistics_Avg = st_avg;
				_Ret.Statistics_Max = st_max;
				_Ret.Statistics_Stv = st_stv;
				_Ret.Statistics_Sum = st_sum;
			}

			public static void Get_Histogramm(AErgAttribut _Ret)
			{
				if (_Ret.Count_Distinct > DPAttribut.DOMAIN_SIMPLE_BORDER)
				{
					Get_Histogramm_Calculated(_Ret);
				}
				else
				{
					Get_Histogramm_Select(_Ret);
				}
			}

			private static void Get_Histogramm_Select(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT CAST(T.[{_Ret.AttributeName}] AS NVARCHAR), COUNT_BIG(*) FROM {_Ret.Relation} T 
GROUP BY T.[{_Ret.AttributeName}] ORDER BY T.[{_Ret.AttributeName}];
";

				Dictionary<string, long> hg = new Dictionary<string, long>();

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					hg.Add( _DR.IsDBNull(0) ? "NULL" : _DR.GetString(0), _DR.GetInt64(1) );
				}

				_DR.Close();

				_Ret.Histogramm = hg;
			}

			private static void Get_Histogramm_Calculated(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string min = $"{_Ret.Statistics_Min.Value}".Replace(',', '.');
				string max = $"{_Ret.Statistics_Max.Value}".Replace(',', '.');

				string sql_cmd_str = $@"
DECLARE @b FLOAT = CAST({min} AS FLOAT); 
DECLARE @u FLOAT = CAST({max} AS FLOAT); 
DECLARE @m FLOAT = (@u - @b)/10.0;
DECLARE @true BIGINT = 1;
DECLARE @false BIGINT = 0;

IF @b <> @u

SELECT 
	SUM(V.[0_1])	[0_1]
,	SUM(V.[1_2])	[1_2]
,	SUM(V.[2_3])	[2_3]
,	SUM(V.[3_4])	[3_4]
,	SUM(V.[4_5])	[4_5]
,	SUM(V.[5_6])	[5_6]
,	SUM(V.[6_7])	[6_7]
,	SUM(V.[7_8])	[7_8]
,	SUM(V.[8_9])	[8_9]
,	SUM(V.[9_X])	[9_X]

,	@b + 0 * @m	[0]
,	@b + 1 * @m	[1]
,	@b + 2 * @m	[2]
,	@b + 3 * @m	[3]
,	@b + 4 * @m	[4]
,	@b + 5 * @m	[5]
,	@b + 6 * @m	[6]
,	@b + 7 * @m	[7]
,	@b + 8 * @m	[8]
,	@b + 9 * @m	[9]
,	@b + 10 * @m [X]

,	(SELECT COUNT_BIG(*) FROM {_Ret.Relation} WHERE [{_Ret.AttributeName}] IS NULL) AS [N]
FROM (
	SELECT
		CASE WHEN K.BIN <= 1 THEN @true ELSE @false END [0_1]
	,	CASE WHEN K.BIN >= 1 AND K.BIN <= 2 THEN @true ELSE @false END [1_2]
	,	CASE WHEN K.BIN >= 2 AND K.BIN <= 3 THEN @true ELSE @false END [2_3]
	,	CASE WHEN K.BIN >= 3 AND K.BIN <= 4 THEN @true ELSE @false END [3_4]
	,	CASE WHEN K.BIN >= 4 AND K.BIN <= 5 THEN @true ELSE @false END [4_5]
	,	CASE WHEN K.BIN >= 5 AND K.BIN <= 6 THEN @true ELSE @false END [5_6]
	,	CASE WHEN K.BIN >= 6 AND K.BIN <= 7 THEN @true ELSE @false END [6_7]
	,	CASE WHEN K.BIN >= 7 AND K.BIN <= 8 THEN @true ELSE @false END [7_8]
	,	CASE WHEN K.BIN >= 8 AND K.BIN <= 9 THEN @true ELSE @false END [8_9]
	,	CASE WHEN K.BIN >= 9 THEN @true ELSE @false END [9_X]
	FROM (
		SELECT
			T.[{_Ret.AttributeName}]
		,	(T.[{_Ret.AttributeName}] - @b) / @m [BIN]
		FROM {_Ret.Relation} T
	) K
) V
;
ELSE SELECT NULL;
";

				Dictionary<string, long> hg = new Dictionary<string, long>();

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				if (_DR.IsDBNull(0))
				{
					hg = null;
				}
				else
				{
					double bin0 = _DR.GetDouble(10);
					double bin1 = _DR.GetDouble(11);
					double bin2 = _DR.GetDouble(12);
					double bin3 = _DR.GetDouble(13);
					double bin4 = _DR.GetDouble(14);
					double bin5 = _DR.GetDouble(15);
					double bin6 = _DR.GetDouble(16);
					double bin7 = _DR.GetDouble(17);
					double bin8 = _DR.GetDouble(18);
					double bin9 = _DR.GetDouble(19);
					double binX = _DR.GetDouble(20);

					hg.Add("NULL", _DR.GetInt64(21));
					hg.Add($"Von {bin0} bis {bin1}", _DR.GetInt64(0));
					hg.Add($"Von {bin1} bis {bin2}", _DR.GetInt64(1));
					hg.Add($"Von {bin2} bis {bin3}", _DR.GetInt64(2));
					hg.Add($"Von {bin3} bis {bin4}", _DR.GetInt64(3));
					hg.Add($"Von {bin4} bis {bin5}", _DR.GetInt64(4));
					hg.Add($"Von {bin5} bis {bin6}", _DR.GetInt64(5));
					hg.Add($"Von {bin6} bis {bin7}", _DR.GetInt64(6));
					hg.Add($"Von {bin7} bis {bin8}", _DR.GetInt64(7));
					hg.Add($"Von {bin8} bis {bin9}", _DR.GetInt64(8));
					hg.Add($"Von {bin9} bis {binX}", _DR.GetInt64(9));
				}

				_DR.Close();

				_Ret.Histogramm = hg;
			}

			public static void Get_Quartile(AErgAttribut _Ret)
			{

				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT  TOP 1
	CAST(PERCENTILE_CONT(0.00) WITHIN GROUP (ORDER BY T.[{_Ret.AttributeName}]) OVER () AS NVARCHAR)
,	CAST(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY T.[{_Ret.AttributeName}]) OVER () AS NVARCHAR)
,	CAST(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY T.[{_Ret.AttributeName}]) OVER () AS NVARCHAR)
,	CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY T.[{_Ret.AttributeName}]) OVER () AS NVARCHAR)
,	CAST(PERCENTILE_CONT(1.00) WITHIN GROUP (ORDER BY T.[{_Ret.AttributeName}]) OVER () AS NVARCHAR)
FROM {_Ret.Relation} T;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				string q_00 = _DR.IsDBNull(0) ? null : _DR.GetString(0);
				string q_25 = _DR.IsDBNull(1) ? null : _DR.GetString(1);
				string q_50 = _DR.IsDBNull(2) ? null : _DR.GetString(2);
				string q_75 = _DR.IsDBNull(3) ? null : _DR.GetString(3);
				string q_XX = _DR.IsDBNull(4) ? null : _DR.GetString(4);

				_DR.Close();

				if (string.IsNullOrWhiteSpace(q_00) || string.IsNullOrWhiteSpace(q_25) || string.IsNullOrWhiteSpace(q_50) || string.IsNullOrWhiteSpace(q_75) || string.IsNullOrWhiteSpace(q_XX) )
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.Quartile = new Dictionary<string, string>() {
					{ "Quartil 0%", q_00},
					{ "Quartil 25%", q_25},
					{ "Quartil 50%", q_50},
					{ "Quartil 75%", q_75},
					{ "Quartil 100%", q_XX}
				};
			}

			public static void Get_Benford(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				Dictionary<string, long> ben = new Dictionary<string, long>();

				string sql_cmd_str = $@"
SELECT V.LEADCHAR, COUNT_BIG(*) FROM
(SELECT LEFT(CAST(T.[{_Ret.AttributeName}] AS VARCHAR), 1) AS [LEADCHAR] FROM {_Ret.Relation} T) V
GROUP BY V.LEADCHAR ORDER BY V.LEADCHAR ASC;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				while (_DR.Read())
				{
					ben.Add(_DR.IsDBNull(0) ? "NULL" : _DR.GetString(0), _DR.IsDBNull(1) ? -1 : _DR.GetInt64(1));
				}

				_DR.Close();

				if (ben.Values.Contains(-1))
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.Benford = ben;
			}

			public static void Get_Text_Order(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT TOP 1
	CAST(FIRST_VALUE(T.[{_Ret.AttributeName}]) OVER (ORDER BY T.[{_Ret.AttributeName}] ASC) AS NVARCHAR)
,	CAST(FIRST_VALUE(T.[{_Ret.AttributeName}]) OVER (ORDER BY T.[{_Ret.AttributeName}] DESC)AS NVARCHAR)
FROM {_Ret.Relation} T;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				string t_a = _DR.IsDBNull(0) ? null : _DR.GetString(0);
				string t_d = _DR.IsDBNull(1) ? null : _DR.GetString(1);

				_DR.Close();

				if (t_a == null && t_d == null)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.Text_First = t_a;
				_Ret.Text_Last = t_d;
			}

			/*
			 public string ModeValue { get; set; } = null;
		public long? ModeValue_Total { get; set; } = null;
		public decimal? ModeValue_Relative { get; set; } = null;
			 */

			public static void Get_ModeValue(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SELECT TOP 1 CAST(T.[{_Ret.AttributeName}] AS NVARCHAR), COUNT_BIG(*) FROM {_Ret.Relation} T GROUP BY T.[{_Ret.AttributeName}];
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				string mv = _DR.IsDBNull(0) ? "NULL" : _DR.GetString(0);
				long? mv_tot = _DR.IsDBNull(1) ? (long?)null : _DR.GetInt64(1);

				_DR.Close();

				if (!mv_tot.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.ModeValue = mv;
				_Ret.ModeValue_Total = mv_tot;
				_Ret.ModeValue_Relative = ((decimal)(mv_tot.Value)) / ((decimal)(_Ret.Count_Rows.Value));
			}

			private static readonly Dictionary<string, string> ConvertToPrimitive = new Dictionary<string, string>() {
			{"bigint", "NUM"},
			{"bit", "NUM"},
			{"decimal", "NUM"},
			{"int", "NUM"},
			{"money", "NUM"},
			{"numeric", "NUM"},
			{"smallint", "NUM"},
			{"smallmoney", "NUM"},
			{"tinyint", "NUM"},
			{"float", "NUM"},
			{"real", "NUM"},

			{"date", "DATETIME"},
			{"datetime", "DATETIME"},
			{"datetime2", "DATETIME"},
			{"datetimeoffset", "DATETIME"},
			{"smalldatetime", "DATETIME"},
			{"time", "DATETIME"},

			{"char", "TEXT" },
			{"varchar", "TEXT" },
			{"text", "TEXT" },
			{"nchar", "TEXT" },
			{"ntext", "TEXT" },
			{"nvarchar", "TEXT" }

		};
			public static string Get_PrimitiveDatatype(string Datatype)
			{
				if (ConvertToPrimitive.Keys.Contains(Datatype))
				{
					return ConvertToPrimitive[Datatype];
				}
				else
				{
					return "OTHER";
				}
			}

			public static void Get_PrimitiveDatatypeProperties(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
DECLARE @true BIGINT = 1;
DECLARE @false BIGINT = 0;
SELECT
	SUM(V.NUM) AS [NUM_TOT]
,	SUM(V.APH) AS [APH_TOT]
,	SUM(V.OTH) AS [OTH_TOT]
FROM
(SELECT
    CASE WHEN T.[{_Ret.AttributeName}] LIKE '%[0-9]%' THEN @true ELSE @false END [NUM]
    ,CASE WHEN T.[{_Ret.AttributeName}] LIKE '%[a-zA-Z]%' THEN @true ELSE @false END [APH]
    ,CASE WHEN T.[{_Ret.AttributeName}] LIKE '%[^0-9a-zA-Z]%' THEN @true ELSE @false END [OTH]
FROM {_Ret.Relation} T) V;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();
				
				long? prp_aph = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);
				long? prp_num = _DR.IsDBNull(1) ? (long?)null : _DR.GetInt64(1);
				long? prp_oth = _DR.IsDBNull(2) ? (long?)null : _DR.GetInt64(2);

				_DR.Close();

				if (!prp_aph.HasValue || !prp_num.HasValue || !prp_oth.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				Dictionary<string, long> prp_tot = new Dictionary<string, long>();

				prp_tot.Add("NUM", prp_aph.Value);
				prp_tot.Add("APH", prp_num.Value);
				prp_tot.Add("OTH", prp_oth.Value);

				Dictionary<string, decimal> prp_rel = new Dictionary<string, decimal>();

				prp_rel.Add("NUM", ((decimal)prp_tot["NUM"]) / ((decimal)_Ret.Count_Attribute.Value));
				prp_rel.Add("APH", ((decimal)prp_tot["APH"]) / ((decimal)_Ret.Count_Attribute.Value));
				prp_rel.Add("OTH", ((decimal)prp_tot["OTH"]) / ((decimal)_Ret.Count_Attribute.Value));

				_Ret.Datatype_Primitive_Properties = prp_tot;
				_Ret.Datatype_Primitive_Properties_Relative = prp_rel;
			}

			public static void Get_TextToNumKonversions(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
SET ARITHABORT OFF; 
SET ARITHIGNORE ON; 
SET ANSI_WARNINGS OFF; 

DECLARE @true BIGINT = 1;
DECLARE @false BIGINT = 0;

SELECT 
	SUM(V.[BIGINT])		[TOT_BIGINT]
,	SUM(V.[BIT])		[TOT_BIT]
,	SUM(V.[DECIMAL])	[TOT_DECIMAL]
,	SUM(V.[INT])		[TOT_INT]
,	SUM(V.[MONEY])		[TOT_MONEY]
,	SUM(V.[NUMERIC])	[TOT_NUMERIC]
,	SUM(V.[SMALLINT])	[TOT_SMALLINT]
,	SUM(V.[SMALLMONEY])	[TOT_SMALLMONEY]
,	SUM(V.[TINYINT])	[TOT_TINYINT]
,	SUM(V.[FLOAT])		[TOT_FLOAT] 
,	SUM(V.[REAL])		[TOT_REAL]
,	SUM(V.[DATE])		[TOT_DATE]
FROM (
	SELECT
		CASE WHEN K.A = CAST(TRY_CAST(K.A AS BIGINT)		AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [BIGINT]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS BIT)			AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [BIT]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS DECIMAL)		AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [DECIMAL]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS INT)			AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [INT]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS MONEY)			AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [MONEY]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS NUMERIC)		AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [NUMERIC]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS SMALLINT)		AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [SMALLINT]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS SMALLMONEY)	AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [SMALLMONEY]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS TINYINT)		AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [TINYINT]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS FLOAT)			AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [FLOAT]
	,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS REAL)			AS {_Ret.Datatype_Documented}) THEN @true ELSE @false END [REAL]
	,	CASE WHEN ISDATE(K.A) = 1					THEN @true ELSE @false END [DATE]
	FROM (
		SELECT 
			CAST(T.[{_Ret.AttributeName}] AS {_Ret.Datatype_Documented}) A 
		FROM 
			{_Ret.Relation} T
		WHERE 
			T.[{_Ret.AttributeName}] IS NOT NULL
	) K
) V
;

SET ARITHABORT ON; 
SET ARITHIGNORE OFF; 
SET ANSI_WARNINGS ON;
";
				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? kv_bigint		= _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);
				long? kv_bit		= _DR.IsDBNull(1) ? (long?)null : _DR.GetInt64(1);
				long? kv_decimal	= _DR.IsDBNull(2) ? (long?)null : _DR.GetInt64(2);
				long? kv_int		= _DR.IsDBNull(3) ? (long?)null : _DR.GetInt64(3);
				long? kv_money		= _DR.IsDBNull(4) ? (long?)null : _DR.GetInt64(4);
				long? kv_numeric	= _DR.IsDBNull(5) ? (long?)null : _DR.GetInt64(5);
				long? kv_smallint	= _DR.IsDBNull(6) ? (long?)null : _DR.GetInt64(6);
				long? kv_smallmoney	= _DR.IsDBNull(7) ? (long?)null : _DR.GetInt64(7);
				long? kv_tinyint	= _DR.IsDBNull(8) ? (long?)null : _DR.GetInt64(8);
				long? kv_float		= _DR.IsDBNull(9) ? (long?)null : _DR.GetInt64(9);
				long? kv_real		= _DR.IsDBNull(10) ? (long?)null : _DR.GetInt64(10);
				long? kv_date		= _DR.IsDBNull(11) ? (long?)null : _DR.GetInt64(11);

				_DR.Close();

				if (!kv_int.HasValue || !kv_bit.HasValue || !kv_decimal.HasValue || !kv_int.HasValue || !kv_money.HasValue || !kv_numeric.HasValue || !kv_smallint.HasValue || !kv_smallmoney.HasValue || !kv_tinyint.HasValue || !kv_float.HasValue || !kv_real.HasValue || !kv_date.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				Dictionary<string, long> kv_all = new Dictionary<string, long>();
				kv_all.Add("bigint", kv_bigint.Value);
				kv_all.Add("bit",kv_bit.Value);
				kv_all.Add("decimal",kv_decimal.Value);
				kv_all.Add("int",kv_int.Value);
				kv_all.Add("money",kv_money.Value);
				kv_all.Add("numeric",kv_numeric.Value);
				kv_all.Add("smallint",kv_smallint.Value);
				kv_all.Add("smallmoney",kv_smallmoney.Value);
				kv_all.Add("tinyint",kv_tinyint.Value);
				kv_all.Add("float",kv_float.Value);
				kv_all.Add("real",kv_real.Value);
				kv_all.Add("date",kv_date.Value);

				List<KeyValuePair<string, long>> _tmp_kvSortList = kv_all.ToList();
				_tmp_kvSortList.Sort((pair1, pair2) => pair1.Value.CompareTo(pair2.Value));

				kv_all.Clear();
				Dictionary<string, decimal> kv_rel = new Dictionary<string, decimal>();

				foreach (KeyValuePair<string, long> kvp in _tmp_kvSortList)
				{

					kv_all.Add(kvp.Key, kvp.Value);
					kv_rel.Add(kvp.Key, ((decimal)kvp.Value) / ((decimal)_Ret.Count_Attribute));

				}

				_Ret.Datatype_Konversion = kv_all;
				_Ret.Datatype_Konversion_Relative = kv_rel;
			}

			public static void Get_TrimValues(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				string sql_cmd_str = $@"
DECLARE @true BIGINT = 1;
DECLARE @false BIGINT = 0;
SELECT 
	SUM(K.[LTRIM])
,	SUM(K.[RTRIM])
,	SUM(K.[BTRIM])
FROM (
	SELECT 
		V.*
	,	CASE WHEN V.[LTRIM] + V.[RTRIM] = 2 THEN @true ELSE @false END [BTRIM]
	FROM (
		SELECT 
			CASE WHEN T.[{_Ret.AttributeName}] = LTRIM(T.[{_Ret.AttributeName}]) THEN @false ELSE @true END [LTRIM]
		,	CASE WHEN T.[{_Ret.AttributeName}] = RTRIM(T.[{_Ret.AttributeName}]) THEN @false ELSE @true END [RTRIM]
		FROM {_Ret.Relation} T
	) V
) K;
";

				SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

				_DR.Read();

				long? tr_l = _DR.IsDBNull(0) ? (long?)null : _DR.GetInt64(0);
				long? tr_r = _DR.IsDBNull(1) ? (long?)null : _DR.GetInt64(1);
				long? tr_b = _DR.IsDBNull(2) ? (long?)null : _DR.GetInt64(2);

				_DR.Close();

				if(!tr_l.HasValue || !tr_r.HasValue || !tr_b.HasValue)
				{
					throw new ArgumentNullException("SQL Query returned invalid NULL value!");
				}

				_Ret.Space_Left = tr_l;
				_Ret.Space_Right = tr_r;
				_Ret.Space_Both = tr_b;

				_Ret.Space_Left_Relative = ((decimal)_Ret.Space_Left) / ((decimal) _Ret.Count_Attribute);
				_Ret.Space_Right_Relative = ((decimal)_Ret.Space_Right) / ((decimal)_Ret.Count_Attribute);
				_Ret.Space_Both_Relative = ((decimal)_Ret.Space_Both) / ((decimal)_Ret.Count_Attribute);
			}

			public static void Get_SimpleDomain(AErgAttribut _Ret)
			{
				LogHelper.LogApp($"{MethodBase.GetCurrentMethod().Name}");

				if (_Ret.Count_Distinct <= DPAttribut.DOMAIN_SIMPLE_BORDER)
				{

					string sql_cmd_str = $@"
SELECT CAST(K.A AS NVARCHAR) FROM ( SELECT DISTINCT T.[{_Ret.AttributeName}] AS [A] FROM {_Ret.Relation} T ) K ORDER BY K.A;
";

					List<string> sd = new List<string>();

					SqlDataReader _DR = DBManager.ExecuteRead(sql_cmd_str);

					while (_DR.Read())
					{
						sd.Add( _DR.IsDBNull(0) ? "NULL" : _DR.GetString(0) );
					}

					_DR.Close();

					if (sd.Count == 0)
					{
						throw new ArgumentNullException("SQL Query returned invalid NULL value!");
					}

					_Ret.SimpleDomain = sd;

				}


			}

		}

	}
}
