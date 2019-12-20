using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CombinationListTest
{
	class Program
	{
		static void Main(string[] args)
		{
			int maxlen = 3;
			List<object> _input = new List<object>(new object[] { "a", "b", "c", "d", "e" });

			List<List<object>> _output = RootCombine(_input, null, maxlen);

			_output.Sort((a, b) => a.Count.CompareTo(b.Count));

			string _write = $"\nResult ( 2^{_input.Count}-1 = {Math.Pow(2,_input.Count)-1} | {_output.Count} ):\n";
			foreach (var _a in _output)
			{
				_write += "{";
				foreach (var _b in _a)
				{
					_write += $"{_b},";
				}
				_write += "},\n";
			}
			Console.WriteLine(_write);

			Console.ReadLine();
		}

		static void GetCombination(List<int> list)
		{
			double count = Math.Pow(2, list.Count);
			for (int i = 1; i <= count - 1; i++)
			{
				string str = Convert.ToString(i, 2).PadLeft(list.Count, '0');
				for (int j = 0; j < str.Length; j++)
				{
					if (str[j] == '1')
					{
						Console.Write(list[j]);
					}
				}
				Console.WriteLine();
			}
		}

		static List<List<int>> GetCombinationOwn(List<int> _inp, List<int> _prefix = null)
		{
			List<List<int>> _ret = new List<List<int>>() { new List<int>() { _inp.First() } };

			//If input only contains 1 value...
			if (_inp.Count == 1)
			{
				//And there is no prefix...
				if (_prefix != null)
				{
					//Vertical Return: Add last input to prefix and return that
					_ret.First().AddRange(_prefix);
				}
			}
			else
			{
				//Create input with first value missing
				List<int> _tmp_rem = new List<int>(_inp.ToList());
				_tmp_rem.Remove(_inp.First());


				if (_prefix == null)
				{
					//Horizontal: Forward List-1. without prefix and add to return
					_ret.AddRange(GetCombinationOwn(_tmp_rem));
				}

				//Vertical
				List<int> _prefix_new = new List<int>() { _inp.First() };
				if (_prefix != null)
				{
					_prefix_new.AddRange(_prefix);
				}

				for (int i = 0; i < _tmp_rem.Count; i++)
				{
					_ret.AddRange(GetCombinationOwn(_tmp_rem, _prefix_new));
					_tmp_rem.Remove(_tmp_rem.First());
				}
			}

			return _ret;
		}


		static List<List<object>> RootCombine(List<object> _inp, List<object> _pre = null, int maxlen = -1)
		{
			_inp = new List<object>(_inp);
			_pre = _pre == null ? null : new List<object>(_pre);
			List<List<object>> _out = new List<List<object>>();

			//Cancel | End of Root
			if (_inp.Count == 1)
			{
				
				if (_pre == null)
				{
					_out.Add(_inp);
				}
				else
				{
					_pre.AddRange(_inp);
					_out.Add(_pre);
				}

			}
			//Grow Root
			else
			{
				if (_pre == null)
				{
					_pre = new List<object>();
				}

				//Create new Prefix
				_pre.Add(_inp.First());
				//Remove First from inp
				_inp.Remove(_inp.First());
				//Add self to Output
				_out.Add(_pre);

				if (maxlen == -1 || _pre.Count < maxlen)
				{
					//Grow Vertical | Grow Deeper
					_out.AddRange(GrowDeeper(_inp, _pre, maxlen));

					//Grow Horizontal
					if (_pre.Count - 1 == 0)
					{
						_out.AddRange(RootCombine(_inp, null, maxlen));
					}
				}
			}

			return _out;
		}

		static List<List<object>> GrowDeeper(List<object> _inp, List<object> _pre, int maxlen = -1)
		{
			_inp = new List<object>(_inp);
			_pre = new List<object>(_pre);
			List<List<object>> _out = new List<List<object>>();

			int max = _inp.Count;
			for (int i = 0; i < max; i++)
			{

				_out.AddRange(RootCombine(_inp, _pre, maxlen));
				_inp.Remove(_inp.First());

			}

			return _out;
		}
	}
}
