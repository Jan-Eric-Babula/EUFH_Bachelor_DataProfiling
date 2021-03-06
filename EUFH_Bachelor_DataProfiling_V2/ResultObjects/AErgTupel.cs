﻿using System.Collections.Generic;
using EUFH_Bachelor_DataProfiling_V2.HelperObjects;

namespace EUFH_Bachelor_DataProfiling_V2.ResultObjects
{
	class AErgTupel : AnalyseErgebnis
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

		/* # Helper Values # */
		public long Size {get; set;}

		public Dictionary<string, Dictionary<string, bool?>> FunctionalDependencyGrid
		{
			get; set;
		} = null;

		/* # Results # */

		public List<PossibleKey> PossibleKeys
		{
			get; set;
		} = null;

		public PossibleKey DocumentedKey
		{
			get; set;
		} = null;

		public List<PossibleDependency> DocumentedDpenedencies
		{
			get; set;
		} = null;

		public AErgTupel(string _Database, string _Relation, long size)
		{
			FilePath = $@"{_Database}\{_Relation}";
			FileTitel = $"{_Relation} ";
			Database = _Database;
			Relation = _Relation;
			Size = size;
		}
	}
}
