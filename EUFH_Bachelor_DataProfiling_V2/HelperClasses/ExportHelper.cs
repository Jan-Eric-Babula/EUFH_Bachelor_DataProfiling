using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using EUFH_Bachelor_DataProfiling_V2.ResultObjects;
using EUFH_Bachelor_DataProfiling_V2.HelperObjects;

namespace EUFH_Bachelor_DataProfiling_V2.HelperClasses
{
    class ExportHelper
    {

        public static void Export_AttributeAnalyis(List<AErgAttribut> aErgs)
        {
            string fileContent = "";

            foreach(AErgAttribut aErg in aErgs)
            {
                fileContent = "";

                /*Header/Title*/
                fileContent += $"Attributanalyse" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;
                fileContent += $"" + Environment.NewLine;

                /*Kardinalität*/
                fileContent += $"Kardinalitäten" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;

                fileContent += $"Spaltenanzahl;{aErg.Count_Rows}" + Environment.NewLine;
                fileContent += $"Attributanzahl;{aErg.Count_Attribute};{DecimalToPercent(aErg.Count_Attribute_Relative)}" + Environment.NewLine;
                fileContent += $"Anzahl fehlender Werte;{(aErg.Empty_Allowed==true ? $"{aErg.Count_Empty}": $"Nicht erluabt")};{(aErg.Empty_Allowed == true ? $"{DecimalToPercent(aErg.Count_Empty_Relative)}" : $"")};{(aErg.Empty_Allowed == true ? $"{aErg.Count_Empty}" : $"Nicht erluabt")}" + Environment.NewLine;
                fileContent += $"Anzahl einzigartiger Werte;{aErg.Count_Distinct};{DecimalToPercent(aErg.Count_Distinct_Relative)}" + Environment.NewLine;
                fileContent += $"Textlänge" + Environment.NewLine;
                fileContent += $" Mimimum;{aErg.StringLength_Min}" + Environment.NewLine;
                fileContent += $" Durchschnitt;{aErg.StringLength_Avg}" + Environment.NewLine;
                fileContent += $" Maximum;{aErg.StringLength_Max}" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;

                /*Werteverteilung*/
                fileContent += $"Kardinalitäten" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;

                if(aErg.Statistics_Min != null)
                {
                    fileContent += $"Statistiken:" + Environment.NewLine;
                    fileContent += $" Minimum;{aErg.Statistics_Min}" + Environment.NewLine;
                    fileContent += $" Maximum;{aErg.Statistics_Max}" + Environment.NewLine;
                    fileContent += $" Durchschnitt;{aErg.Statistics_Avg}" + Environment.NewLine;
                    fileContent += $" Standardabweichung;{aErg.Statistics_Stv}" + Environment.NewLine;
                    fileContent += $" Summe;{aErg.Statistics_Sum}" + Environment.NewLine;

                    fileContent += $"" + Environment.NewLine;
                    
                    fileContent += $"Histogramm" + Environment.NewLine;
                    foreach(string _k in aErg.Histogramm.Keys)
                    {
                        fileContent += $" {_k};{aErg.Histogramm[_k]}" + Environment.NewLine;
                    }

                    fileContent += $"" + Environment.NewLine;

                    fileContent += $"Quartile" + Environment.NewLine;
                    foreach(string _k in aErg.Quartile.Keys)
                    {
                        fileContent += $" {_k};{aErg.Quartile[_k]}" + Environment.NewLine;
                    }

                    fileContent += $"" + Environment.NewLine;

                    fileContent += $"Benfordsche Verteilung" + Environment.NewLine;
                    foreach (string _k in aErg.Benford.Keys)
                    {
                        fileContent += $" {_k};{aErg.Benford[_k]}" + Environment.NewLine;
                    }

                    fileContent += $"" + Environment.NewLine;
                }
                else
                {
                    fileContent += $"Werteordnung:" + Environment.NewLine;
                    fileContent += $" Erster Wert;{aErg.Text_First}" + Environment.NewLine;
                    fileContent += $" Letzter Wert;{aErg.Text_Last}" + Environment.NewLine;
        
                    fileContent += $"" + Environment.NewLine;

                    if (aErg.Histogramm != null)
                    {
                        fileContent += $"Histogramm" + Environment.NewLine;
                        foreach (string _k in aErg.Histogramm.Keys)
                        {
                            fileContent += $" {_k};{aErg.Histogramm[_k]}" + Environment.NewLine;
                        }

                        fileContent += $"" + Environment.NewLine;
                    }

                }

                fileContent += $"Standardwert;{aErg.DefaultValue}" + Environment.NewLine;
                fileContent += $"Modalwert;{aErg.ModeValue}" + Environment.NewLine;
                fileContent += $"Anzahl Modalwert;{aErg.ModeValue_Total};{DecimalToPercent(aErg.ModeValue_Relative)}" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;

                /*Datenmuster*/
                fileContent += $"Datenmuster" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;
                
                fileContent += $"Datentyp;{aErg.Datatype_Documented} ({aErg.Datatype_Primitive})" + Environment.NewLine;

                if(aErg.Datatype_Primitive_Properties != null)
                {
                    fileContent += $"Primitive Eigenschaften" + Environment.NewLine;
                    foreach(string _k in aErg.Datatype_Primitive_Properties.Keys)
                    {
                        fileContent += $" {_k};{aErg.Datatype_Primitive_Properties[_k]};{DecimalToPercent(aErg.Datatype_Primitive_Properties_Relative[_k])}" + Environment.NewLine;
                    }
                }
                if (aErg.Datatype_Konversion != null)
                {
                    fileContent += $"Datentyp Konvertierung" + Environment.NewLine;
                    foreach(string _k in aErg.Datatype_Konversion.Keys)
                    {
                        fileContent += $" {_k};{aErg.Datatype_Konversion[_k]};{DecimalToPercent( aErg.Datatype_Konversion_Relative[_k])}" + Environment.NewLine;
                    }
                }
                fileContent += $"" + Environment.NewLine;

                if(aErg.Space_Left != null)
                {
                    fileContent += $"Leerzeichen" + Environment.NewLine;
                    fileContent += $" Zu Beginn;{aErg.Space_Left};{DecimalToPercent(aErg.Space_Left_Relative)}" + Environment.NewLine;
                    fileContent += $" Zu Ende;{aErg.Space_Right};{DecimalToPercent(aErg.Space_Right_Relative)}" + Environment.NewLine;
                    fileContent += $" Beide;{aErg.Space_Both};{DecimalToPercent(aErg.Space_Both_Relative)}" + Environment.NewLine;

                    fileContent += $"" + Environment.NewLine;
                }

                if(aErg.SimpleDomain != null)
                {
                    fileContent += $"Simple Domäne" + Environment.NewLine;
                    foreach(string _e in aErg.SimpleDomain)
                    {
                        fileContent += $";'{_e}'," + Environment.NewLine;
                    }
                }

                /*Finish*/
                Export_AErgObject(aErg, fileContent);
            }
        }

        public static void Export_TupelAnalysis(List<AErgTupel> aErgs)
        {
            string fileContent = "";

            foreach(AErgTupel aErg in aErgs)
            {
                /*Header*/
                fileContent += $"Tupelanalyse" + Environment.NewLine;
                fileContent += $"" + Environment.NewLine;
                fileContent += $"" + Environment.NewLine;

                fileContent += $"Dokumentiert:" + Environment.NewLine;
                fileContent += $"" + Environment.NewLine;

                if (aErg.DocumentedKey != null)
                {
                    fileContent += $"Schlüssel" + Environment.NewLine;
                    for(int i = 0; i < aErg.DocumentedKey.Attributes.Count; i++)
                    {
                        fileContent += $" Attribut {i+1}.;{aErg.DocumentedKey.Attributes[i]}" + Environment.NewLine;
                    }

                    fileContent += $"" + Environment.NewLine;
                }

                if(aErg.DocumentedDpenedencies.Count > 0)
                {
                    fileContent += $"Abhängigkeiten" + Environment.NewLine;
                    for(int i = 0; i< aErg.DocumentedDpenedencies.Count; i++)
                    {
                        fileContent += $" Verhältnis {i+1};{{";
                        foreach(string _a in aErg.DocumentedDpenedencies[i].Attributes)
                        {
                            fileContent += $"{_a},";
                        }
                        fileContent += $"}}" + Environment.NewLine;
                        fileContent += $" Regel;{aErg.DocumentedDpenedencies[i].Note}" + Environment.NewLine;
                    }

                    fileContent += $"" + Environment.NewLine;
                }

                fileContent += $"Identifiziert" + Environment.NewLine;
                fileContent += $"" + Environment.NewLine;

                
                if (aErg.DocumentedKey == null)
                {
                    fileContent += $"Schlüssel" + Environment.NewLine;
                    for(int i = 0; i < aErg.PossibleKeys.Count; i++)
                    {
                        fileContent += $" {i}. Möglichkeit;{{";
                        foreach(string _a in aErg.PossibleKeys[i].Attributes)
                        {
                            fileContent += $"{_a},";
                        }
                        fileContent += $"}}" + Environment.NewLine;
                        fileContent += $" Abdeckung;{DecimalToPercent((decimal)aErg.PossibleKeys[i].Coverage)}" + Environment.NewLine;
                    }
                    fileContent += $"" + Environment.NewLine;
                }

                if(aErg.FunctionalDependencyGrid.Count > 0)
                {
                    fileContent += $"Funktionale Abhängigkeiten" + Environment.NewLine;
                    fileContent += $"" + Environment.NewLine;
                    
                    /*Header*/
                    fileContent += $";";
                    foreach (string _nca in aErg.FunctionalDependencyGrid.First().Value.Keys)
                    {
                        fileContent += $"{_nca};";
                    }
                    fileContent += Environment.NewLine;

                    /*Row*/
                    string _res = "";
                    foreach(string _ca in aErg.FunctionalDependencyGrid.Keys)
                    {
                        fileContent += $"{_ca};";
                        foreach(string _a in aErg.FunctionalDependencyGrid[_ca].Keys)
                        {
                            _res = aErg.FunctionalDependencyGrid[_ca][_a].HasValue ? (aErg.FunctionalDependencyGrid[_ca][_a].Value ? "f. Abh." : "n. f. Abh.") : "";
                            fileContent += $"{_res};";
                        }
                        fileContent += Environment.NewLine;
                    }
                }

                Export_AErgObject(aErg, fileContent);
            }
        }

        public static void Export_RelationAnalysis(AErgRelationen aErg)
        {
            string fileContent = "";

            fileContent += $"Referentiellen Integritäten" + Environment.NewLine;
            fileContent += $"" + Environment.NewLine;
            fileContent += $"" + Environment.NewLine;

            fileContent += $"Dokumentiert" + Environment.NewLine;
            foreach(PossibleReference _dri in aErg.DocumentedReferences)
            {
                fileContent += $"Referenz;{_dri.PK_Relation.Relation} {{";
                foreach(string _a in _dri.PK_Relation.Attributes)
                {
                    fileContent += $"{_a},";
                }
                fileContent += $"}};";

                fileContent += $"referenziert von;{_dri.FK_Relation} {{";
                foreach(string _a in _dri.FK_Attributes)
                {
                    fileContent += $"{_a},";
                }
                fileContent += $"}}" + Environment.NewLine;

                fileContent += $"Statistik;{_dri.Childless} Kinderlose, {_dri.Parents} Eltern, {_dri.Orphans} Waisen" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;
            }


            fileContent += $"" + Environment.NewLine;
            fileContent += $"Identifizierte" + Environment.NewLine;
            fileContent += $"" + Environment.NewLine;

            foreach (PossibleReference _dri in aErg.FoundReferences)
            {
                fileContent += $"Referenz;{_dri.PK_Relation.Relation} {{";
                foreach (string _a in _dri.PK_Relation.Attributes)
                {
                    fileContent += $"{_a},";
                }
                fileContent += $"}};";

                fileContent += $"referenziert von;{_dri.FK_Relation} {{";
                foreach (string _a in _dri.FK_Attributes)
                {
                    fileContent += $"{_a},";
                }
                fileContent += $"}}" + Environment.NewLine;

                fileContent += $"Statistik;{_dri.Childless} Kinderlose, {_dri.Parents} Eltern, {_dri.Orphans} Waisen" + Environment.NewLine;

                fileContent += $"" + Environment.NewLine;
            }

            Export_AErgObject(aErg, fileContent);
        }

        private static void Export_AErgObject(AnalyseErgebnis ae, string fileContent)
        {
            string path = (!string.IsNullOrWhiteSpace(ae.FilePath) ? $@"{ae.FilePath}\" : $@"") + ae.FileTitel + ".csv";
            File.WriteAllText(path, fileContent);
        }

        private static string DecimalToPercent(decimal _inp)
        {
            return _inp.ToString("P2");
        }

        private static string DecimalToPercent(decimal? _inp)
        {
            return _inp.HasValue ? DecimalToPercent(_inp.Value) : "-,- %";
        }

    }
}
