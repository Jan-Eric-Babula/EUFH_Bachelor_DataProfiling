using System.Collections.Generic;
using EUFH_Bachelor_DataProfiling_V2.HelperObjects;

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
	}
}
