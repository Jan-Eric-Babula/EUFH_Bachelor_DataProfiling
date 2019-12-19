using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using EUFH_Bachelor_DataProfiling.Sonstiges;

namespace EUFH_Bachelor_DataProfiling
{
	class Program
	{
		static void Main(string[] args)
		{
			DateTime start = DateTime.Now;
			DBManager.ResetLog();
			Console.WriteLine($"Start time: {start}\n######################################\n");
			
			DPAnalyse.DPAnalyseAusführen();

			DateTime end = DateTime.Now;
			Console.WriteLine($"\n######################################\nFinished! ({end})\n\nAnalysis took {end-start}!\n");
			Console.ReadLine();
		}
	}
}
