using System;
using System.Text;
using System.IO;

namespace EUFH_Bachelor_DataProfiling_V2.HelperClasses
{
	class LogHelper
	{

		private static readonly string _LogGeneral = $@"app.log";
		private static readonly string _LogSQL = $@"sql.log";

		public static void LogApp(string log)
		{
			Console.WriteLine($"{DateTime.Now}: {log}");
			File.AppendAllText(_LogGeneral, $"{Environment.NewLine}{DateTime.Now}: {log.Replace(Environment.NewLine, $" ")}", Encoding.UTF8);
		}

		public static void LogAppError(Exception e)
		{
			Console.WriteLine($"\nException occured: {e}\n");

			File.AppendAllLines(_LogGeneral, new string[] {
				$"\n\n#################### {DateTime.Now} ####################\n\n",
				$"Exception: {e.Message}",
				$"StackTrace:\n    {e.StackTrace.Replace(Environment.NewLine, $"{Environment.NewLine}    ")}"
			}, Encoding.UTF8);
			if (e.InnerException != null)
			{
				File.AppendAllLines(_LogGeneral, new string[] {
					$"\n    InnerException: {e.InnerException.Message}",
					$"    StackTrace:\n    {e.InnerException.StackTrace.Replace(Environment.NewLine, $"{Environment.NewLine}        ")}"
				}, Encoding.UTF8);
			}

			Program.ExitWithError();
		}

		public static void LogSQL(string sql)
		{
			File.AppendAllText(_LogSQL, $"{Environment.NewLine}{DateTime.Now}: {sql.Replace(Environment.NewLine, $" ")}", Encoding.UTF8);
		}

		public static void ResetLogs()
		{
			if (File.Exists(_LogGeneral))
			{
				File.Delete(_LogGeneral);
			}
			LogApp("Reset Logs!");

			if (File.Exists(_LogSQL))
			{
				File.Delete(_LogSQL);
			}
			LogSQL("Reset Logs!");
		}

	}
}
