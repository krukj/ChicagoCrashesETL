import pandas as pd
import datetime
import holidays


def make_dim_date(
    start_date: datetime.datetime = datetime.datetime(2016, 1, 1, 0, 0),
    end_date: datetime.datetime = datetime.datetime(2021, 12, 31, 23, 0),
) -> pd.DataFrame:
    """
    Generate a dimension table for dates within the specified range.

    Args:
        start_date (datetime.datetime): The start date for generating the dimension table. Defaults to datetime.date(2016, 1, 1).
        end_date (datetime.datetime): The end date for generating the dimension table. Defaults to datetime.date(2021, 12, 31).

    Returns:
        pd.DataFrame: A DataFrame containing date-related information for each date within the specified range.
    """

    us_holidays = holidays.UnitedStates(years=range(start_date.year, end_date.year + 1))

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_id = int(current_date.strftime("%Y%m%d%H"))
        hour = current_date.hour
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        month_name = current_date.strftime("%B")
        day_of_month = current_date.day
        day_of_week = current_date.isoweekday()
        day_name = current_date.strftime("%A")
        is_weekend = day_of_week >= 6
        week_of_year = current_date.isocalendar()[1]
        is_holiday = current_date.date() in us_holidays
        holiday_name = us_holidays.get(current_date.date()) if is_holiday else "None"


        date_list.append(
            [
                date_id,
                current_date,
                hour,
                year,
                quarter,
                month,
                month_name,
                day_of_month,
                day_of_week,
                day_name,
                is_weekend,
                week_of_year,
                is_holiday,
                holiday_name,
            ]
        )

        current_date += datetime.timedelta(hours=1)

    columns = [
        "date_id",
        "full_date",
        "hour",
        "year",
        "quarter",
        "month",
        "month_name",
        "day_of_month",
        "day_of_week",
        "day_name",
        "is_weekend",
        "week_of_year",
        "is_holiday",
        "holiday_name",
    ]
    df = pd.DataFrame(date_list, columns=columns)

    return df
