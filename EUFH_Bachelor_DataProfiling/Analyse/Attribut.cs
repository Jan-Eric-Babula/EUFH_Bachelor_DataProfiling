using EUFH_Bachelor_DataProfiling.Ergebnis;
using EUFH_Bachelor_DataProfiling.Sonstiges;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.SqlTypes;

namespace EUFH_Bachelor_DataProfiling.Analyse
{
	class Attribut
	{

		public static AErgAttribut Analysieren(string relation, string attribut, int ordinal)
		{
			AErgAttribut ret = new AErgAttribut();
			ret.Datenbank = DPAnalyse.DBName;
			ret.AusgabePfad = $"{relation}";
			ret.Titel = $"Attribut {ordinal} - [{attribut}]";

			Anzahl(ret, relation, attribut);

			if (ret.Anzahl > 0)
			{
				if (AttributHasContent(relation, attribut))
				{
					Anly_Kardinalität(ret, relation, attribut);
					Anly_Werteverteilung(ret, relation, attribut);
					Anly_Datenmuster(ret, relation, attribut);
				}
				else
				{
					FehlendeWerte(ret, relation, attribut);
					FehlendeWerte_Anteilsm(ret, relation, attribut);
				}
			}

			return ret;
		}

		private static bool AttributHasContent(string relation, string attribut)
		{
			Console.Write("\r" + "AttributHasContent" + "()..." + "                               ");

			string sql_cmd_str = $"SELECT CAST(IIF(COUNT(T.[{attribut}])>0,1,0) AS BIT) FROM {relation} T";

			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();

			bool _ret = dr.IsDBNull(0) ? false : dr.GetBoolean(0);

			dr.Close();

			return _ret;
		}

		private static void Anly_Kardinalität(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Anly_Kardinalität" + "()..." + "                               ");

			FehlendeWerte(ret, relation, attribut);
			FehlendeWerte_Anteilsm(ret, relation, attribut);
			Einzigartigkeit(ret, relation, attribut);
			Einzigartigkeit_Aneilsm(ret, relation, attribut);
			Zeichenlänge(ret, relation, attribut);

		}
		#region Anly_Kardinalität

		private static void Anzahl(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Anzahl" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT COUNT_BIG(*) FROM {relation} T";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();
			ret.Anzahl = dr.GetInt64(0);

			dr.Close();
		}

		private static void FehlendeWerte(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "FehlendeWerte" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT COUNT_BIG(*) - COUNT_BIG(T.[{attribut}]) FROM {relation} T";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();
			ret.FehlendeWerte = dr.GetInt64(0);

			dr.Close();
		}

		private static void FehlendeWerte_Anteilsm(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "FehlendeWerte_Anteilsm" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT
       CAST((SELECT COUNT_BIG(*) - COUNT_BIG(T.[{attribut}]) FROM {relation} T) AS DECIMAL) /
       CAST((SELECT COUNT_BIG(*) FROM {relation} T) AS DECIMAL)";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();
			ret.FehlendeWerte_Anteilsm = dr.GetDecimal(0);

			dr.Close();
		}

		private static void Einzigartigkeit(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Einzigartigkeit" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT COUNT_BIG(*) FROM (SELECT DISTINCT T.[{attribut}] FROM {relation} T) V";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();
			ret.Einzigartigkeit = dr.GetInt64(0);

			dr.Close();
		}

		private static void Einzigartigkeit_Aneilsm(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Einzigartigkeit_Aneilsm" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT
       CAST((SELECT COUNT_BIG(*) FROM (SELECT DISTINCT T.[{attribut}] FROM {relation} T) V) AS DECIMAL) /
       CAST((SELECT COUNT_BIG(*) FROM {relation} T) AS DECIMAL)";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();
			ret.Einzigartigkeit_Anteilsm = dr.GetDecimal(0);

			dr.Close();
		}

		private static void Zeichenlänge(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Zeichenlänge" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT CAST(MIN(V.STRLEN) AS BIGINT) AS [MIN] ,CAST(MAX(V.STRLEN) AS BIGINT) AS [MAX] ,CAST(AVG(V.STRLEN) AS DECIMAL) AS [AVG]
FROM (SELECT LEN(CAST(T.[{attribut}] AS VARCHAR)) AS STRLEN FROM {relation} T) V";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			dr.Read();
			ret.Zeichenlänge_Min = dr.IsDBNull(0) ? 0 : dr.GetInt64(0);
			ret.Zeichenlänge_Max = dr.IsDBNull(1) ? 0 : dr.GetInt64(1);
			ret.Zeichenlänge_Avg = dr.IsDBNull(2) ? 0 : dr.GetDecimal(2);

			dr.Close();
		}

		#endregion

		private static Dictionary<string, string> NUM_Casttable = new Dictionary<string, string>()
		{
			{ "real" ,"float" },
			{ "float" ,"float" },

			{ "tinyint" ,"bigint" },
			{ "smallint" ,"bigint" },
			{ "int" ,"bigint" },
			{ "bigint" ,"bigint" },

			{ "smallmoney" ,"money" },
			{ "money" ,"money" },

			{ "numeric" ,"numeric" },
			{ "decimal" ,"decimal" }
		};

		private static void Anly_Werteverteilung(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Anly_Werteverteilung" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS 
WHERE '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' = '{relation}' AND COLUMN_NAME = '{attribut}'";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();
			string datatype = dr.GetString(0);
			dr.Close();

			if (NUM_Casttable.Keys.Contains(datatype))
			{
				Stat(ret, relation, attribut, datatype);
				Frequenzdiagramm(ret, relation, attribut);
				Median(ret, relation, attribut, datatype);
				Benfordsche(ret, relation, attribut);
			}
			else
			{
				Stat_Text(ret, relation, attribut);
			}

			Modalwert(ret, relation, attribut);
			Modalwert_Tot(ret, relation, attribut);
			Modalwert_Anteilsm(ret, relation, attribut);
			Standardwert(ret, relation, attribut);

		}

		#region Anly_Werteverteilung

		#region NUM

		private static void Stat(AErgAttribut ret, string relation, string attribut, string datatype)
		{
			Console.Write("\r" + "Stat" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT CAST(MIN(V.C) AS DECIMAL) AS [MIN] ,CAST(MAX(V.C) AS DECIMAL) AS [MAX] ,CAST(AVG(V.C) AS DECIMAL) AS [AVG] ,CAST(STDEV(V.C) AS DECIMAL) AS [STDEV]
FROM (SELECT CAST(T.[{attribut}] AS {NUM_Casttable[datatype]}) AS C FROM {relation} T) V";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();
			ret.Stat_MIN = dr.GetDecimal(0);
			ret.Stat_MAX = dr.GetDecimal(1);
			ret.Stat_AVG = dr.GetDecimal(2);
			ret.Stat_STDEV = dr.GetDecimal(3);

			dr.Close();
		}

		private static void Frequenzdiagramm(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Frequenzdiagramm" + "()..." + "                               ");

			string sql_cmd_str = $@"DECLARE @b FLOAT = CAST((SELECT MIN(T.[{attribut}]) FROM {relation} T) AS FLOAT); 
DECLARE @u FLOAT = CAST((SELECT MAX(T.[{attribut}]) FROM {relation} T) AS FLOAT); 
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

,	(SELECT COUNT_BIG(*) FROM {relation} WHERE [{attribut}] IS NULL) AS [N]
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
			T.[{attribut}]
		,	(T.[{attribut}] - @b) / @m [BIN]
		FROM {relation} T
	) K
) V
;
ELSE SELECT NULL;";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			Dictionary<string, long> _tmp = null;

			dr.Read();

			if (!dr.IsDBNull(0))
			{
				_tmp = new Dictionary<string, long>();

				double bin0 = dr.GetDouble(10);
				double bin1 = dr.GetDouble(11);
				double bin2 = dr.GetDouble(12);
				double bin3 = dr.GetDouble(13);
				double bin4 = dr.GetDouble(14);
				double bin5 = dr.GetDouble(15);
				double bin6 = dr.GetDouble(16);
				double bin7 = dr.GetDouble(17);
				double bin8 = dr.GetDouble(18);
				double bin9 = dr.GetDouble(19);
				double binX = dr.GetDouble(20);

				_tmp.Add("NULL", dr.GetInt64(21));
				_tmp.Add($"Von {bin0} bis {bin1}", dr.GetInt64(0));
				_tmp.Add($"Von {bin1} bis {bin2}", dr.GetInt64(1));
				_tmp.Add($"Von {bin2} bis {bin3}", dr.GetInt64(2));
				_tmp.Add($"Von {bin3} bis {bin4}", dr.GetInt64(3));
				_tmp.Add($"Von {bin4} bis {bin5}", dr.GetInt64(4));
				_tmp.Add($"Von {bin5} bis {bin6}", dr.GetInt64(5));
				_tmp.Add($"Von {bin6} bis {bin7}", dr.GetInt64(6));
				_tmp.Add($"Von {bin7} bis {bin8}", dr.GetInt64(7));
				_tmp.Add($"Von {bin8} bis {bin9}", dr.GetInt64(8));
				_tmp.Add($"Von {bin9} bis {binX}", dr.GetInt64(9));
			}

			ret.Frequenzdiagramm = _tmp;
			dr.Close();

		}

		private static void Median(AErgAttribut ret, string relation, string attribut, string datatype)
		{
			Console.Write("\r" + "Median" + "()..." + "                               ");

			string sql_cmd_str = $@"DECLARE @c BIGINT = (SELECT COUNT(T.[{attribut}]) FROM {relation} T);
SELECT CAST(AVG(1.0 * val) AS DECIMAL) AS [MEDIAN]
FROM (
    SELECT CAST(T.[{attribut}] AS {datatype}) val FROM {relation} T
     ORDER BY val
     OFFSET (@c - 1) / 2 ROWS
     FETCH NEXT 1 + (1 - @c % 2) ROWS ONLY
) AS x;";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			if (dr.IsDBNull(0))
			{
				ret.Median = null;
			}
			else
			{
				ret.Median = dr.GetDecimal(0);
			}

			dr.Close();
		}

		private static void Benfordsche(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Benfordsche" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT V.LEADCHAR, COUNT_BIG(*) FROM
(SELECT LEFT(CAST(T.[{attribut}] AS VARCHAR), 1) AS [LEADCHAR] FROM {relation} T) V
GROUP BY V.LEADCHAR
ORDER BY V.LEADCHAR ASC;";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			Dictionary<string, long> _tmp = new Dictionary<string, long>();

			while (dr.Read())
			{
				_tmp.Add(dr.IsDBNull(0) ? "NULL" : dr.GetString(0), dr.GetInt64(1));
			}

			ret.BenfordscheVerteilung = _tmp;
			dr.Close();
		}

		#endregion

		#region TXT

		private static void Stat_Text(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Stat_Text" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT
    CAST((SELECT TOP 1 T.[{attribut}] FROM {relation} T ORDER BY T.[{attribut}] ASC) AS NVARCHAR) AS [FIRST]
    ,CAST((SELECT TOP 1 T.[{attribut}] FROM {relation} T ORDER BY T.[{attribut}] DESC) AS NVARCHAR) AS [LAST];";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();
			ret.Text_Start = dr.IsDBNull(0) ? null : dr.GetString(0);
			ret.Text_Ende = dr.IsDBNull(1) ? null : dr.GetString(1);

			dr.Close();
		}

		#endregion

		#region GEN

		private static void Modalwert(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Modalwert" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT TOP 1 CAST(T.[{attribut}] AS NVARCHAR) FROM {relation} T GROUP BY T.[{attribut}] ORDER BY COUNT_BIG(*) DESC;";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			ret.Modalwert = dr.IsDBNull(0) ? null : dr.GetString(0);

			dr.Close();
		}

		private static void Modalwert_Tot(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Modalwert_Tot" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT TOP 1 COUNT_BIG(*) FROM {relation} T GROUP BY T.[{attribut}] ORDER BY COUNT(*) DESC;";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			ret.Modalwert_Tot = dr.GetInt64(0);

			dr.Close();
		}

		private static void Modalwert_Anteilsm(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Modalwert_Anteilsm" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT
       (CAST((SELECT TOP 1 COUNT_BIG(*) FROM {relation} T GROUP BY T.[{attribut}] ORDER BY COUNT_BIG(*) DESC) AS DECIMAL)) /
       (CAST((SELECT COUNT_BIG(T.[{attribut}]) FROM {relation} T) AS DECIMAL));";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			ret.Modalwert_Anteilsm = dr.GetDecimal(0);

			dr.Close();
		}

		private static void Standardwert(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Standardwert" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT CAST(COLUMN_DEFAULT AS NVARCHAR) FROM INFORMATION_SCHEMA.COLUMNS 
WHERE '['+TABLE_SCHEMA+'].['+TABLE_NAME + ']' = '{relation}' AND COLUMN_NAME = '{attribut}';";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			ret.Standardwert = dr.IsDBNull(0) ? null : dr.GetString(0);

			dr.Close();
		}

		#endregion

		#endregion


		private static void Anly_Datenmuster(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Anly_Datenmuster" + "()..." + "                               ");

			Datentyp_PrimitivUndKonversion(ret, relation, attribut);

			SucheDomäne(ret, relation, attribut);
		}

		#region Anly_Datenmuster

		/*NUM, APH, APHNUM, DATETIME, OTHER*/
		private static readonly Dictionary<string, string> PrimitiveKonversion = new Dictionary<string, string>() {
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

			{"char", null },
			{"varchar", null },
			{"text", null },
			{"nchar", null },
			{"ntext", null },
			{"nvarchar", null }

		};

		private static void Datentyp_PrimitivUndKonversion(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "Datentyp_PrimitivUndKonversion" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT CAST(DATA_TYPE AS NVARCHAR) FROM INFORMATION_SCHEMA.COLUMNS 
WHERE '['+TABLE_SCHEMA+'].['+TABLE_NAME+']' = '{relation}' AND COLUMN_NAME = '{attribut}'";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			string datatype_actual = dr.GetString(0);

			dr.Close();

			ret.DokumentierterDatentyp = datatype_actual;

			if (PrimitiveKonversion.Keys.Contains(datatype_actual))
			{
				string konversion = PrimitiveKonversion[datatype_actual];
				if (konversion == null)
				{
					ret.PrimitiverDatentyp = "TEXT";

					PrimitiverDatentypEigenschaften(ret, relation, attribut);

					if (ret.PrimitiverDatentypEigenschaften != null)
					{
						if (ret.PrimitiverDatentypEigenschaften.Keys.Contains("NUM"))
						{
							if (ret.PrimitiverDatentypEigenschaften["NUM"] > 0)
							{
								DatentypVonText(ret, relation, attribut, datatype_actual);
							}
						}
					}

					TrimTrailBetroffene(ret, relation, attribut);
				}
				else
				{
					ret.PrimitiverDatentyp = konversion;
					ret.PrimitiverDatentypEigenschaften_Anteilsm = null;

					ret.DatentypenKonversion = null;
					ret.DatentypenKonversion_Anteilsm = null;

					//TODO IF NUM LÄNGE & PRÄZISIONSTEST
				}
			}
			else
			{
				ret.PrimitiverDatentyp = "OTHER";
				ret.PrimitiverDatentypEigenschaften_Anteilsm = null;

				ret.DatentypenKonversion = null;
				ret.DatentypenKonversion_Anteilsm = null;
			}
		}

		private static void PrimitiverDatentypEigenschaften(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "PrimitiverDatentypEigenschaften" + "()..." + "                               ");

			string sql_cmd_str = $@" DECLARE @d DECIMAL = CAST((SELECT COUNT_BIG(T.[{attribut}]) FROM {relation} T) AS DECIMAL);
SELECT
    CAST(SUM(V.NUM) AS DECIMAL) /  @d AS [NUM]
    ,CAST(SUM(V.APH) AS DECIMAL) / @d AS [APH]
    ,CAST(SUM(V.OTH) AS DECIMAL) / @d AS [OTH]
	,SUM(V.NUM) AS [NUM_TOT]
	,SUM(V.APH) AS [APH_TOT]
	,SUM(V.OTH) AS [OTH_TOT]
FROM
(SELECT
    CAST(CASE WHEN T.[{attribut}] LIKE '%[0-9]%' THEN 1 ELSE 0 END AS BIGINT) AS [NUM]
    ,CAST(CASE WHEN T.[{attribut}] LIKE '%[a-zA-Z]%' THEN 1 ELSE 0 END AS BIGINT) AS [APH]
    ,CAST(CASE WHEN T.[{attribut}] LIKE '%[^0-9a-zA-Z]%' THEN 1 ELSE 0 END AS BIGINT) AS [OTH]
FROM {relation} T) V;";
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			Dictionary<string, decimal> ret_ant = new Dictionary<string, decimal>();
			Dictionary<string, long> ret_tot = new Dictionary<string, long>();

			ret_ant.Add("NUM", dr.GetDecimal(0));
			ret_ant.Add("APH", dr.GetDecimal(1));
			ret_ant.Add("OTH", dr.GetDecimal(2));
			//_tmp.Add("N_NUM", dr.GetDecimal(3));
			//_tmp.Add("N_APH", dr.GetDecimal(4));

			ret_tot.Add("NUM", dr.GetInt64(3));
			ret_tot.Add("APH", dr.GetInt64(4));
			ret_tot.Add("OTH", dr.GetInt64(5));


			dr.Close();

			ret.PrimitiverDatentypEigenschaften = ret_tot;
			ret.PrimitiverDatentypEigenschaften_Anteilsm =  ret_ant;
		}

		private static void DatentypVonText(AErgAttribut ret, string relation, string attribut, string datentyp)
		{
			Console.Write("\r" + "DatentypVonText" + "()..." + "                               ");

			string sql_cmd_str = $@"SET ARITHABORT OFF; 
SET ARITHIGNORE ON; 
SET ANSI_WARNINGS OFF; 

DECLARE @d DECIMAL = CAST((SELECT COUNT_BIG(*) FROM {relation} T) AS DECIMAL);
DECLARE @true BIGINT = 1;
DECLARE @false BIGINT = 0;

SELECT
	CAST(R.[TOT_BIGINT] AS DECIMAL)		/ @d [BIGINT]
,	CAST(R.[TOT_BIT] AS DECIMAL)		/ @d [BIT]
,	CAST(R.[TOT_DECIMAL] AS DECIMAL)	/ @d [DECIMAL]
,	CAST(R.[TOT_INT] AS DECIMAL)		/ @d [INT]
,	CAST(R.[TOT_MONEY] AS DECIMAL)		/ @d [MONEY]
,	CAST(R.[TOT_NUMERIC] AS DECIMAL)	/ @d [NUMERIC]
,	CAST(R.[TOT_SMALLINT] AS DECIMAL)	/ @d [SMALLINT]
,	CAST(R.[TOT_SMALLMONEY] AS DECIMAL) / @d [SMALLMONEY]
,	CAST(R.[TOT_TINYINT] AS DECIMAL)	/ @d [TINYINT]
,	CAST(R.[TOT_FLOAT]  AS DECIMAL)		/ @d [FLOAT]
,	CAST(R.[TOT_REAL] AS DECIMAL)		/ @d [REAL]
,	CAST(R.[TOT_DATE] AS DECIMAL)		/ @d [DATE]
,	R.* 
FROM (
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
			CASE WHEN K.A = CAST(TRY_CAST(K.A AS BIGINT)		AS {datentyp}) THEN @true ELSE @false END [BIGINT]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS BIT)			AS {datentyp}) THEN @true ELSE @false END [BIT]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS DECIMAL)		AS {datentyp}) THEN @true ELSE @false END [DECIMAL]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS INT)			AS {datentyp}) THEN @true ELSE @false END [INT]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS MONEY)			AS {datentyp}) THEN @true ELSE @false END [MONEY]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS NUMERIC)		AS {datentyp}) THEN @true ELSE @false END [NUMERIC]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS SMALLINT)		AS {datentyp}) THEN @true ELSE @false END [SMALLINT]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS SMALLMONEY)	AS {datentyp}) THEN @true ELSE @false END [SMALLMONEY]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS TINYINT)		AS {datentyp}) THEN @true ELSE @false END [TINYINT]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS FLOAT)			AS {datentyp}) THEN @true ELSE @false END [FLOAT]
		,	CASE WHEN K.A = CAST(TRY_CAST(K.A AS REAL)			AS {datentyp}) THEN @true ELSE @false END [REAL]
		,	CASE WHEN ISDATE(K.A) = 1					THEN @true ELSE @false END [DATE]
		FROM (
			SELECT 
				CAST(T.[{attribut}] AS {datentyp}) A 
			FROM 
				{relation} T
			WHERE 
				T.[{attribut}] IS NOT NULL
		) K
	) V
) R
;

SET ARITHABORT ON; 
SET ARITHIGNORE OFF; 
SET ANSI_WARNINGS ON;";

			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			Dictionary<string, long> ret_tot = new Dictionary<string, long>();
			Dictionary<string, long> _tmp_ret_tot = new Dictionary<string, long>();
			Dictionary<string, decimal> ret_ant = new Dictionary<string, decimal>();

			ret_ant.Add("bigint", dr.GetDecimal(0));
			ret_ant.Add("bit", dr.GetDecimal(1));
			ret_ant.Add("decimal", dr.GetDecimal(2));
			ret_ant.Add("int", dr.GetDecimal(3));
			ret_ant.Add("money", dr.GetDecimal(4));
			ret_ant.Add("numeric", dr.GetDecimal(5));
			ret_ant.Add("smallint", dr.GetDecimal(6));
			ret_ant.Add("smallmoney", dr.GetDecimal(7));
			ret_ant.Add("tinyint", dr.GetDecimal(8));
			ret_ant.Add("float", dr.GetDecimal(9));
			ret_ant.Add("real", dr.GetDecimal(10));
			ret_ant.Add("date", dr.GetDecimal(11));

			_tmp_ret_tot.Add("bigint", dr.GetInt64(12));
			_tmp_ret_tot.Add("bit", dr.GetInt64(13));
			_tmp_ret_tot.Add("decimal", dr.GetInt64(14));
			_tmp_ret_tot.Add("int", dr.GetInt64(15));
			_tmp_ret_tot.Add("money", dr.GetInt64(16));
			_tmp_ret_tot.Add("numeric", dr.GetInt64(17));
			_tmp_ret_tot.Add("smallint", dr.GetInt64(18));
			_tmp_ret_tot.Add("smallmoney", dr.GetInt64(19));
			_tmp_ret_tot.Add("tinyint", dr.GetInt64(20));
			_tmp_ret_tot.Add("float", dr.GetInt64(21));
			_tmp_ret_tot.Add("real", dr.GetInt64(22));
			_tmp_ret_tot.Add("date", dr.GetInt64(23));

			dr.Close();

			List<KeyValuePair<string,decimal>> _tmp_antSortList = ret_ant.ToList();
			_tmp_antSortList.Sort((pair1, pair2) => pair1.Value.CompareTo(pair2.Value));

			ret_ant.Clear();
			foreach(var a in _tmp_antSortList)
			{
				ret_ant.Add(a.Key, a.Value);
				ret_tot.Add(a.Key, _tmp_ret_tot[a.Key]);
			}

			ret.DatentypenKonversion = ret_tot;
			ret.DatentypenKonversion_Anteilsm = ret_ant;
		}

		private static void TrimTrailBetroffene(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "TrimTrailBetroffene" + "()..." + "                               ");

			string sql_cmd_str = $@"SELECT
    SUM(V.LTRIM) AS [LTRIM_TOT]
    ,SUM(V.RTRIM) AS [RTRIM_TOT]
    ,CAST(SUM(V.LTRIM) AS DECIMAL) / CAST((SELECT COUNT_BIG(*) FROM {relation} T) AS DECIMAL) AS [LTRIM]
    ,CAST(SUM(V.RTRIM) AS DECIMAL) / CAST((SELECT COUNT_BIG(*) FROM {relation} T) AS DECIMAL) AS [RTRIM]
FROM
(SELECT
    CAST(CASE WHEN T.[{attribut}] = LTRIM(T.[{attribut}]) THEN 0 ELSE 1 END AS BIGINT) AS [LTRIM]
    ,CAST(CASE WHEN T.[{attribut}] = RTRIM(T.[{attribut}]) THEN 0 ELSE 1 END AS BIGINT) AS [RTRIM]
FROM {relation} T) V;";

			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);
			dr.Read();

			ret.TrimBetroffene = dr.GetInt64(0);
			ret.TrailBetroffene = dr.GetInt64(1);
			
			ret.TrimBetroffene_Anteilsm = dr.GetDecimal(2);
			ret.TrailBetroffene_Anteilsm = dr.GetDecimal(3);

			dr.Close();
		}

		private static void SucheDomäne(AErgAttribut ret, string relation, string attribut)
		{
			Console.Write("\r" + "SucheDomäne" + "()..." + "                               ");

			int domänenLimit = 25;

			string sql_cmd_str = $@"IF (SELECT TOP 1 COUNT_BIG(*) FROM (SELECT DISTINCT T.[{attribut}] FROM {relation} T) V) <= {domänenLimit}
    SELECT DISTINCT CAST(T.[{attribut}] AS NVARCHAR) FROM {relation} T ORDER BY CAST(T.[{attribut}] AS NVARCHAR) ASC;
ELSE
    SELECT NULL;";

			List<string> _domäne = new List<string>();
			SqlDataReader dr = DBManager.ExecuteRead(sql_cmd_str);

			string _tmp = null;
			while (dr.Read())
			{
				_tmp = dr.IsDBNull(0) ? null : dr.GetString(0);
				if (_tmp == null)
				{
					ret.Domäne = null;
					break;
				}
				_domäne.Add(_tmp);
			}

			dr.Close();

			ret.Domäne = _domäne;

		}
		

		#endregion
	}
}
