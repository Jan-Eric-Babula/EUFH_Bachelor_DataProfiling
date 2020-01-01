namespace EUFH_Bachelor_DataProfiling_V2.HelperObjects
{
	class AttributeBaseData
	{

		public string AttributeName
		{
			get;
		}
		public int OrdinalPosition
		{
			get;
		}
		public string ColumnDefault
		{
			get; 
		}
		public bool IsNullable
		{
			get; 
		}
		public string DataType
		{
			get; 
		}
		public long RowCount
		{
			get; 
		}

		public AttributeBaseData(string name, int ordinalPosition, string columnDefault, bool isNullable, string dataType, long rowCount)
		{
			this.AttributeName = name;
			this.OrdinalPosition = ordinalPosition;
			this.ColumnDefault = columnDefault;
			this.IsNullable = isNullable;
			this.DataType = dataType;
			this.RowCount = rowCount;
		}

	}
}
