using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace EUFH_Bachelor_DataProfiling_V2
{
	class Program
	{

		public static DateTime START = DateTime.Now;
		public static DateTime END;

		static void Main(string[] args)
		{
			try
			{
				LogHelper.ResetLogs();
				DBManager.Init();
			}
			catch(Exception e)
			{
				LogHelper.LogAppError(e);
				Console.WriteLine($"\n##################################################\nException: {e.Message}\nStackTrace:\n{e.StackTrace}");
				ExitWithError();
			}

			DPAnalysis.Run();

			END = DateTime.Now;
			LogHelper.LogApp($"{END}: Application finished! (Took {END - START})");

			Console.ReadLine();
			Environment.Exit(0);
		}

		public static void ExitWithError()
		{
			Console.WriteLine("\nApplication finished with Exception!\nSee Log for more information.");
			Console.ReadLine();
			Environment.Exit(0);
		}
	}
}
