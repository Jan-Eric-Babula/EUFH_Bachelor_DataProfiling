using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using EUFH_Bachelor_DataProfiling.Analyse;
using EUFH_Bachelor_DataProfiling.Ergebnis;
using Newtonsoft.Json;

namespace EUFH_Bachelor_DataProfiling.Sonstiges
{
	class DPAnalyse
	{
		public static string DBName;
		public static Dictionary<string, List<string>> AllTablesColumns;

		public static List<AErgAttribut> Ergebnisse_Attribute = new List<AErgAttribut>();

		public static void DPAnalyseAusführen()
		{
			Console.WriteLine("\n\n##########\nStarte AusgabeVorbereiten...\n##########\n");
			AusgabeVorbereiten();
			Console.WriteLine("\n##########\nAusgabeVorbereiten abgeschlossen!\n##########\n\n");

			Console.WriteLine("\n\n##########\nStarte Attributanalyse...\n##########\n");


			Attributanalyse();
			/*
			try
			{
				Attributanalyse();
			}
			catch(Exception e)
			{
				Console.WriteLine($"{e.Message}\n{e.Source}\n{e.StackTrace}\n\n{e.InnerException}");
			}
			*/

			Console.WriteLine("\n##########\nAttributanalyse abgeschlossen!\n##########\n\n");
		}

		private static void AusgabeVorbereiten()
		{
			DBNamenExtrahieren();

			DBAlleTabellenAttribute();
		}

		#region AusgabeVorbereiten

		private static void DBNamenExtrahieren()
		{
			string sql_dbname = @"SELECT TOP 1 TABLE_CATALOG FROM INFORMATION_SCHEMA.TABLES";
			SqlDataReader dr_dbname = DBManager.ExecuteRead(sql_dbname);
			dr_dbname.Read();
			string db_name = dr_dbname.GetString(0);
			DBManager.CloseConnection();

			if (Directory.Exists(db_name))
			{
				Directory.Delete(db_name, true);
			}
			Directory.CreateDirectory(db_name);
			DBName = db_name;

			Console.WriteLine($"    Starte Analyse der Datenbank '{DBName}'");
		}

		private static void DBAlleTabellenAttribute()
		{
			AllTablesColumns = new Dictionary<string, List<string>>();
			string sql_dbtables =
				@"SELECT '['+C.TABLE_SCHEMA+'].['+C.TABLE_NAME+']',C.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLES T
RIGHT JOIN INFORMATION_SCHEMA.COLUMNS C
ON T.TABLE_NAME = C.TABLE_NAME AND T.TABLE_SCHEMA = C.TABLE_SCHEMA
WHERE T.TABLE_TYPE = 'BASE TABLE'
ORDER BY T.TABLE_SCHEMA,T.TABLE_NAME,C.ORDINAL_POSITION";
			SqlDataReader dr_dbtables = DBManager.ExecuteRead(sql_dbtables);
			while (dr_dbtables.Read())
			{
				string table = dr_dbtables.GetString(0);
				if (!AllTablesColumns.Keys.Contains(table))
				{
					AllTablesColumns.Add(table, new List<string>());
					Directory.CreateDirectory($@"{DBName}\{table}");
				}
				string column = dr_dbtables.GetString(1);
				AllTablesColumns[table].Add(column);
			}
			dr_dbtables.Close();
			Console.WriteLine($"    {AllTablesColumns.Keys.Count} Relationen gefunden!");
		}

		#endregion

		private static void Attributanalyse()
		{
			int i = 0, limit = 100, offset = 35;
			foreach (var r in AllTablesColumns)
			{
				Console.WriteLine($"\r    Starte Relation {i+1}: '{r.Key}'");
				if (i >= offset)
				{
					foreach (var a in r.Value)
					{
						Console.WriteLine($"\r        Starte Attribut '{a}'");
						Ergebnisse_Attribute.Add(Attribut.Analysieren(r.Key, a, r.Value.IndexOf(a)));
					}
				}
				else
				{
					Console.WriteLine($"    Skipped!");
				}
				

				string dump_json = JsonConvert.SerializeObject(Ergebnisse_Attribute, Formatting.Indented);
				if (File.Exists("dump.json"))
				{
					File.Delete("dump.json");
				}
				File.WriteAllText("dump.json", dump_json);

				i++;
				if (i >= limit)
				{
					break;
				}
			}

			Console.WriteLine($"\r    {Ergebnisse_Attribute.Count} Attribute analysiert!");
			
		}
	}
}
