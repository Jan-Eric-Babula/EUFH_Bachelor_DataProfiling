using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.HelperObjects
{
	class PossibleDependency
	{

		public string Database
		{
			get; set;
		}
		public string Relation
		{
			get; set;
		}

		public string Attribute_Dependant
		{
			get;set;
		}

		public string Attribute_Determinant
		{
			get;set;
		}

		public string Note
		{
			get;set;
		}

		public PossibleDependency(string DB, string RL)
		{
			Database = DB;
			Relation = RL;
			Attribute_Dependant = null;
			Attribute_Determinant = null;
			Note = null;
		}
	}
}
