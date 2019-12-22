using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.HelperObjects
{
	class PossibleReference
	{

		public PossibleKey PK_Relation
		{
			get; set;
		} = null;

		public string FK_Relation
		{
			get;set;
		} = null;

		public List<string> FK_Attributes
		{
			get;set;
		} = null;

		public long Childless
		{
			get; set;
		} = -1;

		public long Parents
		{
			get;set;
		} = -1;
		public long Orphans
		{
			get;set;
		} = -1;

	}
}
