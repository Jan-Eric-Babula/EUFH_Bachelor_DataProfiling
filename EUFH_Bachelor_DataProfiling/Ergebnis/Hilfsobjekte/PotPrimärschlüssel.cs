using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling.Ergebnis.Hilfsobjekte
{
	class PotPrimärschlüssel
	{

		public bool Dokumentiert;
		public string Relation;
		public List<string> Attribute;
		public double Einzigartigkeit_Anteilsm;
		public double FehlendeWerte_Anteilsm;
		public string NichtFunktionalAbhängig; /* Liste? */
		public string NichtFunktionalAbhängig_Anteilsm;


	}
}
