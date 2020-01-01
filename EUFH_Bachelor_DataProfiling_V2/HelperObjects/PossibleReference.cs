using System.Collections.Generic;

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

        public decimal GetEvaluation()
        {
            if(Childless!=-1 && Parents != -1 && Orphans != -1)
            {
                double tot = (double)(Childless + Parents + Orphans);
                decimal a = (decimal)((double)(Childless + Parents) / tot);
                decimal b = (decimal)((double)(Parents) / tot);
                return a * b;
            }
            else
            {
                return 0;
            }
        }

	}
}
