using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.HelperObjects
{
	class PossibleReference
	{

		public string PK_Relation
		{
			get; set;
		} = null;

		public List<string> PK_Attributes
		{
			get;set;
		} = null;

		public string FK_Relation
		{
			get;set;
		} = null;

		public List<string> FK_Attributes
		{
			get;set;
		} = null;

		public decimal Childless
		{
			get; set;
		} = -1;

		public decimal Parents
		{
			get;set;
		} = -1;
		public decimal Orphans
		{
			get;set;
		} = -1;

	}
}
