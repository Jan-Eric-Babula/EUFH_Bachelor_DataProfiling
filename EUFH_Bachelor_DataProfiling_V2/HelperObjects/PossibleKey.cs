﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EUFH_Bachelor_DataProfiling_V2.HelperObjects
{
	class PossibleKey
	{
		public string Relation
		{
			get;set;
		}
		public List<string> Attribute
		{
			get;set;
		}

		public PossibleKey(string RL)
		{
			Relation = RL;
			Attribute = new List<string>();
		}

	}
}
