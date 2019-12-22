using EUFH_Bachelor_DataProfiling_V2.HelperObjects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.ResultObjects
{
	class AErgRelationen : AnalyseErgebnis
	{

		public List<PossibleReference> DocumentedReferences
		{
			get; set;
		} = null;

		public List<PossibleReference> FoundReferences
		{
			get; set;
		} = null;

		public AErgRelationen(string _Database)
		{
			FilePath = $@"{_Database}";
			FileTitel = $"{_Database} References";
		}

		public AErgRelationen()
		{
		}
	}
}
