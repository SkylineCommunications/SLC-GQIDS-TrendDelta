using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.Trending;

[GQIMetaData(Name = "Trend delta")]
public class TrendDeltaSource : IGQIDataSource, IGQIOnInit, IGQIInputArguments
{
	private readonly GQIIntArgument _dmaIDArg;
	private readonly GQIIntArgument _elementIDArg;
	private readonly GQIIntArgument _parameterIDArg;
	private readonly GQIStringDropdownArgument _timeRangeArg;
	private readonly GQIStringDropdownArgument _intervalArg;

	private readonly GQIDateTimeColumn _startTimeColumn;
	private readonly GQIDateTimeColumn _endTimeColumn;
	private readonly GQIDoubleColumn _startValueColumn;
	private readonly GQIDoubleColumn _endValueColumn;
	private readonly GQIDoubleColumn _deltaColumn;

	private GQIDMS _dms;

	private int _dmaID;
	private int _elementID;
	private ParameterIndexPair[] _parameters;
	private AbsoluteTimeRange _timeRange;
	private IInterval _interval;

	private readonly IInterval[] _intervals = new IInterval[]
	{
		new HourInterval(),
		new DayInterval(),
		new WeekInterval(),
		new MonthInterval(),
		new YearInterval()
	};

	private readonly RelativeTimeRange[] _timeRanges = new RelativeTimeRange[]
	{
		RelativeTimeRange.AllTime,
		new RelativeTimeRange("Last day", utcNow =>
		{
			var localNow = utcNow.ToLocalTime();
			var localEnd = localNow.Date;
			var localStart = localEnd.AddDays(-1);
			var utcStart = localStart.ToUniversalTime();
			var utcEnd = localEnd.ToUniversalTime();
			return new AbsoluteTimeRange(utcStart, utcEnd);
		}),
		new RelativeTimeRange("Last week", utcNow =>
		{
			var localNow = utcNow.ToLocalTime();
			var localEnd = localNow.Date;
			var localStart = localEnd.AddDays(-7);
			var utcStart = localStart.ToUniversalTime();
			var utcEnd = localEnd.ToUniversalTime();
			return new AbsoluteTimeRange(utcStart, utcEnd);
		}),
		new RelativeTimeRange("Last month", utcNow =>
		{
			var localNow = utcNow.ToLocalTime();
			var localEnd = localNow.Date;
			var localStart = localEnd.AddMonths(-1);
			var utcStart = localStart.ToUniversalTime();
			var utcEnd = localEnd.ToUniversalTime();
			return new AbsoluteTimeRange(utcStart, utcEnd);
		}),
		new RelativeTimeRange("Last year", utcNow =>
		{
			var localNow = utcNow.ToLocalTime();
			var localEnd = localNow.Date;
			var localStart = localEnd.AddYears(-1);
			var utcStart = localStart.ToUniversalTime();
			var utcEnd = localEnd.ToUniversalTime();
			return new AbsoluteTimeRange(utcStart, utcEnd);
		})
	};

	public TrendDeltaSource()
	{
		// Arguments
		_dmaIDArg = new GQIIntArgument("DataMiner ID") { IsRequired = true };
		_elementIDArg = new GQIIntArgument("Element ID") { IsRequired = true };
		_parameterIDArg = new GQIIntArgument("Parameter ID") { IsRequired = true };

		var timeRangeOptions = _timeRanges.Select(timeRange => timeRange.Name).ToArray();
		_timeRangeArg = new GQIStringDropdownArgument("Time range", timeRangeOptions)
		{
			IsRequired = true,
			DefaultValue = RelativeTimeRange.AllTime.Name
		};

		var intervalOptions = _intervals.Select(interval => interval.Name).ToArray();
		_intervalArg = new GQIStringDropdownArgument("Interval", intervalOptions)
		{
			IsRequired = true,
			DefaultValue = intervalOptions[1]
		};

		// Columns
		_startTimeColumn = new GQIDateTimeColumn("Start time");
		_endTimeColumn = new GQIDateTimeColumn("End time");
		_startValueColumn = new GQIDoubleColumn("Start value");
		_endValueColumn = new GQIDoubleColumn("End value");
		_deltaColumn = new GQIDoubleColumn("Delta");
	}

	public OnInitOutputArgs OnInit(OnInitInputArgs args)
	{
		_dms = args.DMS;
		return default;
	}

	public GQIArgument[] GetInputArguments()
	{
		return new GQIArgument[]
		{
			_dmaIDArg,
			_elementIDArg,
			_parameterIDArg,
			_timeRangeArg,
			_intervalArg
		};
	}

	public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
	{
		_dmaID = args.GetArgumentValue(_dmaIDArg);
		_elementID = args.GetArgumentValue(_elementIDArg);

		var parameterID = args.GetArgumentValue(_parameterIDArg);
		var parameter = new ParameterIndexPair(parameterID);
		_parameters = new[] { parameter };

		var timeRangeName = args.GetArgumentValue(_timeRangeArg);
		var relativeTimeRange = GetTimeRangeByName(timeRangeName);
		_timeRange = relativeTimeRange.GetAbsolute(DateTime.UtcNow);

		var intervalName = args.GetArgumentValue(_intervalArg);
		_interval = GetIntervalByName(intervalName);

		return default;
	}

	public GQIColumn[] GetColumns()
	{
		return new GQIColumn[]
		{
			_startTimeColumn,
			_endTimeColumn,
			_startValueColumn,
			_endValueColumn,
			_deltaColumn
		};
	}

	public GQIPage GetNextPage(GetNextPageInputArgs args)
	{
		var trendPoints = GetTrendPoints(_timeRange.Start, _timeRange.End);
		var rows = CreateIntervalRows(trendPoints);
		return new GQIPage(rows.ToArray()) { HasNextPage = false };
	}

	private IEnumerable<AverageTrendRecord> GetTrendPoints(DateTime startTime, DateTime endTime)
	{
		var request = GetTrendRequest(startTime, endTime);
		var response = _dms.SendMessage(request) as GetTrendDataResponseMessage;
		if (response is null)
			throw new Exception("Invalid trend data.");
		var records = response.Records[_parameters[0].Key];
		return records.OfType<AverageTrendRecord>();
	}

	private GetTrendDataMessage GetTrendRequest(DateTime startTime, DateTime endTime)
	{
		return new GetTrendDataMessage
		{
			StartTime = startTime,
			EndTime = endTime,
			DataMinerID = _dmaID,
			ElementID = _elementID,
			Parameters = _parameters,
			TrendingType = TrendingType.Average,
			ReturnAsObjects = true,
			RetrievalWithPrimaryKey = true,
			DateTimeUTC = true
		};
	}

	private IEnumerable<GQIRow> CreateIntervalRows(IEnumerable<AverageTrendRecord> records)
	{
		var enumerator = records.GetEnumerator();
		if (!enumerator.MoveNext())
			yield break; // No start point

		// Get current interval start
		var intervalStartTime = GetIntervalStartTime(enumerator.Current.Time);
		// Skip to the first complete interval
		intervalStartTime = _interval.GetIntervalEnd(intervalStartTime);
		var startPoint = MoveToPoint(enumerator, intervalStartTime);

		while (true)
		{
			var intervalEndTime = _interval.GetIntervalEnd(intervalStartTime);
			var endPoint = MoveToPoint(enumerator, intervalEndTime);

			if (startPoint == endPoint)
				yield break; // No end point

			// Found end point
			yield return CreateIntervalRow(intervalStartTime, intervalEndTime, startPoint, endPoint);

			// End point becomes start point
			intervalStartTime = intervalEndTime;
			startPoint = endPoint;
		}
	}

	private DateTime GetIntervalStartTime(DateTime utcPoint)
	{
		var localPoint = utcPoint.ToLocalTime();
		var localStart = _interval.GetIntervalStart(localPoint);
		return localStart.ToUniversalTime();
	}

	private AverageTrendRecord MoveToPoint(IEnumerator<AverageTrendRecord> records, DateTime targetTime)
	{
		AverageTrendRecord point;
		do
		{
			point = records.Current;
		}
		while (records.MoveNext() && records.Current.Time < targetTime);
		return point;
	}

	private GQIRow CreateIntervalRow(DateTime startTime, DateTime endTime, AverageTrendRecord startPoint, AverageTrendRecord endPoint)
	{
		var startValue = startPoint.AverageValue;
		var endValue = endPoint.AverageValue;
		var delta = endValue - startValue;

		var cells = new[]
		{
			new GQICell { Value = startTime },
			new GQICell { Value = endTime },
			new GQICell { Value = startValue },
			new GQICell { Value = endValue },
			new GQICell { Value = delta }
		};
		return new GQIRow(cells);
	}

	private RelativeTimeRange GetTimeRangeByName(string name)
	{
		return _timeRanges.FirstOrDefault(timeRange => timeRange.Name == name) ?? RelativeTimeRange.AllTime;
	}

	private IInterval GetIntervalByName(string name)
	{
		return _intervals.FirstOrDefault(interval => interval.Name == name) ?? _intervals[1];
	}

	private class RelativeTimeRange
	{
		public static RelativeTimeRange AllTime = new RelativeTimeRange("All time", _ =>
		{
			return new AbsoluteTimeRange(DateTime.MinValue, DateTime.MaxValue);
		}
		);

		public string Name { get; }
		public Func<DateTime, AbsoluteTimeRange> GetAbsolute { get; }

		public RelativeTimeRange(string name, Func<DateTime, AbsoluteTimeRange> getAbsolute)
		{
			Name = name;
			GetAbsolute = getAbsolute;
		}
	}

	private struct AbsoluteTimeRange
	{
		public DateTime Start { get; }
		public DateTime End { get; }

		public AbsoluteTimeRange(DateTime start, DateTime end)
		{
			Start = start;
			End = end;
		}

		public bool Contains(DateTime timestamp)
		{
			return Start <= timestamp && timestamp < End;
		}
	}

	private interface IInterval
	{
		string Name { get; }
		DateTime GetIntervalStart(DateTime timestamp);
		DateTime GetIntervalEnd(DateTime intervalStart);
	}

	private struct HourInterval : IInterval
	{
		public string Name => "Hour";

		public DateTime GetIntervalEnd(DateTime intervalStart) => intervalStart.AddHours(1);

		public DateTime GetIntervalStart(DateTime pointTime)
		{
			return new DateTime(pointTime.Year, pointTime.Month, pointTime.Day, pointTime.Hour, 0, 0, pointTime.Kind);
		}
	}

	private struct DayInterval : IInterval
	{
		public string Name => "Day";

		public DateTime GetIntervalEnd(DateTime intervalStart) => intervalStart.AddDays(1);

		public DateTime GetIntervalStart(DateTime pointTime)
		{
			return pointTime.Date;
		}
	}

	private struct WeekInterval : IInterval
	{
		public string Name => "Week";

		public DateTime GetIntervalEnd(DateTime intervalStart) => intervalStart.AddDays(7);

		public DateTime GetIntervalStart(DateTime pointTime)
		{
			var date = pointTime.Date;
			var firstDayOfWeek = CultureInfo.CurrentCulture.DateTimeFormat.FirstDayOfWeek;
			int dayOfTheWeek = (date.DayOfWeek - firstDayOfWeek + 7) % 7;
			return date.AddDays(-dayOfTheWeek);
		}
	}

	private struct MonthInterval : IInterval
	{
		public string Name => "Month";

		public DateTime GetIntervalEnd(DateTime intervalStart) => intervalStart.AddMonths(1);

		public DateTime GetIntervalStart(DateTime pointTime)
		{
			return new DateTime(pointTime.Year, pointTime.Month, 1, 0, 0, 0, pointTime.Kind);
		}
	}

	private struct YearInterval : IInterval
	{
		public string Name => "Year";

		public DateTime GetIntervalEnd(DateTime intervalStart) => intervalStart.AddYears(1);

		public DateTime GetIntervalStart(DateTime pointTime)
		{
			return new DateTime(pointTime.Year, 1, 1, 0, 0, 0, pointTime.Kind);
		}
	}
}