using EUFH_Bachelor_DataProfiling_V2.ResultObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.AnalysesClasses
{
	class DPRelationen
	{

		public static AErgRelationen Analysis(string Database)
		{
			AErgRelationen _Ret = new AErgRelationen(Database);



			return _Ret;
		}

		private class DPRelationen_Helper
		{

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
