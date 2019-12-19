using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.ResultObjects
{
	class AErgAttribut : AnalyseErgebnis
	{	

		/* # Database Location # */
		public string Database
		{
			get; set;
		}
		public string Relation
		{
			get; set;
		}
		public string AttributeName
		{
			get; set;
		}
		public int OrdinalPosition
		{
			get; set;
		}

		/* # Kardinalität # */
		public long? Count_Rows { get; set; } = null;

		public long? Count_Attribute { get; set; } = null;
		public decimal? Count_Attribute_Relative { get; set; } = null;

		public bool? Empty_Allowed { get; set; } = null;
		public long? Count_Empty { get; set; } = null;
		public decimal? Count_Empty_Relative { get; set; } = null;

		public long? Count_Distinct { get; set; } = null;
		public decimal? Count_Distinct_Relative { get; set; } = null;

		public long? StringLength_Min { get; set; } = null;
		public decimal? StringLength_Avg { get; set; } = null;
		public long? StringLength_Max { get; set; } = null;

		/* # Werteverteilung # */
		public decimal? Statistics_Min { get; set; } = null;
		public decimal? Statistics_Avg { get; set; } = null;
		public decimal? Statistics_Max { get; set; } = null;
		public decimal? Statistics_Stv { get; set; } = null;
		public decimal? Statistics_Sum { get; set; } = null;

		public string Text_First { get; set; } = null;
		public string Text_Last { get; set; } = null;

		public Dictionary<string, long> Histogramm { get; set; } = null;
		
		public Dictionary<string, string> Quartile {get;set;} = null;

		public Dictionary<string, long> Benford { get; set; } = null;

		public string ModeValue { get; set; } = null;
		public long? ModeValue_Total { get; set; } = null;
		public decimal? ModeValue_Relative { get; set; } = null;

		public string DefaultValue { get; set; } = null;

		/* # Datenmuster # */
		public string Datatype_Documented { get; set; } = null;

		public string Datatype_Primitive { get; set; } = null;
		public Dictionary<string, long> Datatype_Primitive_Properties { get; set; } = null;
		public Dictionary<string, decimal> Datatype_Primitive_Properties_Relative { get; set; } = null;

		public Dictionary<string, long> Datatype_Konversion { get; set; } = null;
		public Dictionary<string, decimal> Datatype_Konversion_Relative { get; set; } = null;

		public long? Space_Left { get; set; } = null;
		public decimal? Space_Left_Relative { get; set; } = null;
		public long? Space_Right { get; set; } = null;
		public decimal? Space_Right_Relative { get; set; } = null;
		public long? Space_Both { get; set; } = null;
		public decimal? Space_Both_Relative { get; set; } = null;

		public List<string> SimpleDomain { get; set; } = null;

		public AErgAttribut(string _Database, string _Relation, string _AttributName, int _OrdinalPosition)
		{
			FilePath = $@"{_Database}\{_Relation}";
			FileTitel = $@"Attribute {_OrdinalPosition} - {_AttributName}";
			Database = _Database;
			Relation = _Relation;
			AttributeName = _AttributName;
			OrdinalPosition = _OrdinalPosition;
		}
	}
}
