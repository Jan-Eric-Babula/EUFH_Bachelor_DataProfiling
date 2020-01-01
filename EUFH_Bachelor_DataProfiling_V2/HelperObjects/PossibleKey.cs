using System.Collections.Generic;

namespace EUFH_Bachelor_DataProfiling_V2.HelperObjects
{
	class PossibleKey
	{
		public string Relation
		{
			get;set;
		}
		public List<string> Attributes
		{
			get;set;
		}

		public double Coverage
		{
			get;set;
		}

		public PossibleKey(string RL)
		{
			Relation = RL;
			Attributes = new List<string>();
		}

	}
}
