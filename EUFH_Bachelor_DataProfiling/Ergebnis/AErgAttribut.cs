using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling.Ergebnis
{
	class AErgAttribut : AnalyseErgebnis
	{

		/*Kardinalität*/
		public long Anzahl;
		public long FehlendeWerte;
		public decimal FehlendeWerte_Anteilsm;
		public long Einzigartigkeit;
		public decimal Einzigartigkeit_Anteilsm;
		public long Zeichenlänge_Min;
		public decimal Zeichenlänge_Avg;
		public long Zeichenlänge_Max;

		/*Werteverteilung*/
		public decimal Stat_MIN; 
		public decimal Stat_MAX; 
		public decimal Stat_AVG;
		public decimal Stat_STDEV;
		public string Text_Start; /*Erster Text*/
		public string Text_Ende; /*Letzter Text*/
		public Dictionary<string, long> Frequenzdiagramm;
		public decimal? Median; /*Quartile?*/
		public Dictionary<string, long> BenfordscheVerteilung;
		public string Modalwert;
		public long Modalwert_Tot;
		public decimal Modalwert_Anteilsm;
		public string Standardwert;

		/*Datenmuster*/
		public string DokumentierterDatentyp;
		public string PrimitiverDatentyp;
		
		public Dictionary<string, long> PrimitiverDatentypEigenschaften;
		public Dictionary<string, decimal> PrimitiverDatentypEigenschaften_Anteilsm;

		public Dictionary<string, long> DatentypenKonversion;
		public Dictionary<string, decimal> DatentypenKonversion_Anteilsm;

		public long TrimBetroffene;
		public long TrailBetroffene;
		public decimal TrimBetroffene_Anteilsm;
		public decimal TrailBetroffene_Anteilsm;

		public List<string> Domäne;
	}
}
