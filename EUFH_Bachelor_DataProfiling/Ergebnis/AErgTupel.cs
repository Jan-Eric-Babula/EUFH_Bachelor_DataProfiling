using EUFH_Bachelor_DataProfiling.Ergebnis.Hilfsobjekte;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling.Ergebnis
{
	class AErgTupel : AnalyseErgebnis
	{

		public string Relation;
		public List<PotPrimärschlüssel> Schlüssel;
		public List<PotAbgeleiteteWerte> AbgeleiteteWerte;

	}
}
