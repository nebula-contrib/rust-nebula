use crate::common::{Date, DateTime, Duration, Time};
#[cfg(feature = "chrono")]
use chrono::{NaiveDate, NaiveDateTime};

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct DurationWrapper<'a> {
    duration: &'a Duration,
}

impl<'a> DurationWrapper<'a> {
    pub fn new(duration: &'a Duration) -> Self {
        Self { duration }
    }
    pub fn get_months(&self) -> i32 {
        self.duration.months
    }
    pub fn get_seconds(&self) -> i64 {
        self.duration.seconds
    }
    pub fn get_microseconds(&self) -> i32 {
        self.duration.microseconds
    }
    pub fn get_raw_duration(&self) -> &Duration {
        self.duration
    }

    #[cfg(feature = "chrono")]
    pub fn to_chrono_duration(&self) -> chrono::Duration {
        let months = self.get_months();
        let seconds = self.get_seconds();
        let microseconds = self.get_microseconds();

        let total_seconds = months as i64 * 30 * 24 * 60 * 60 + seconds;
        let total_microseconds = total_seconds * 1_000_000 + microseconds as i64;

        chrono::Duration::microseconds(total_microseconds)
    }
}

use crate::TimezoneInfo;

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct DateWrapper<'a> {
    date: &'a Date,
    timezone_info: &'a TimezoneInfo,
}

impl<'a> DateWrapper<'a> {
    pub fn new(date: &'a Date, timezone_info: &'a TimezoneInfo) -> Self {
        Self {
            date,
            timezone_info,
        }
    }
    pub fn get_year(&self) -> i16 {
        self.date.year
    }
    pub fn get_month(&self) -> i8 {
        self.date.month
    }
    pub fn get_day(&self) -> i8 {
        self.date.day
    }
    pub fn get_raw_date(&self) -> &Date {
        self.date
    }

    #[cfg(feature = "chrono")]
    pub fn to_naive_date(&self) -> NaiveDate {
        NaiveDate::from_ymd_opt(
            self.get_year() as i32,
            self.get_month() as u32,
            self.get_day() as u32,
        )
        .expect("chrono::NaiveDate::from_ymd_opt")
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeWrapper<'a> {
    time: &'a Time,
    timezone_info: &'a TimezoneInfo,
}

impl<'a> TimeWrapper<'a> {
    pub fn new(time: &'a Time, timezone_info: &'a TimezoneInfo) -> Self {
        Self {
            time,
            timezone_info,
        }
    }
    pub fn get_hour(&self) -> i8 {
        self.time.hour
    }
    pub fn get_minute(&self) -> i8 {
        self.time.minute
    }
    pub fn get_second(&self) -> i8 {
        self.time.sec
    }
    pub fn get_microsec(&self) -> i32 {
        self.time.microsec
    }
    pub fn get_raw_time(&self) -> &Time {
        self.time
    }

    #[cfg(feature = "chrono")]
    pub fn to_naive_date_time(&self) -> chrono::NaiveDateTime {
        let d =
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("chrono::NaiveDate::from_ymd_opt");
        let t = chrono::NaiveTime::from_hms_micro_opt(
            self.get_hour() as u32,
            self.get_minute() as u32,
            self.get_second() as u32,
            self.get_microsec() as u32,
        )
        .expect("chrono::NaiveTime::from_hms_micro_opt");
        NaiveDateTime::new(d, t)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTimeWrapper<'a> {
    date_time: &'a DateTime,
    timezone_info: &'a TimezoneInfo,
}

impl<'a> DateTimeWrapper<'a> {
    pub fn new(date_time: &'a DateTime, timezone_info: &'a TimezoneInfo) -> Self {
        Self {
            date_time,
            timezone_info,
        }
    }
    pub fn get_year(&self) -> i16 {
        self.date_time.year
    }
    pub fn get_month(&self) -> i8 {
        self.date_time.month
    }
    pub fn get_day(&self) -> i8 {
        self.date_time.day
    }
    pub fn get_hour(&self) -> i8 {
        self.date_time.hour
    }
    pub fn get_minute(&self) -> i8 {
        self.date_time.minute
    }
    pub fn get_second(&self) -> i8 {
        self.date_time.sec
    }
    pub fn get_microsec(&self) -> i32 {
        self.date_time.microsec
    }
    pub fn get_raw_date(&self) -> &DateTime {
        self.date_time
    }

    #[cfg(feature = "chrono")]
    pub fn to_naive_date_time(&self) -> chrono::NaiveDateTime {
        let d = chrono::NaiveDate::from_ymd_opt(
            self.get_year() as i32,
            self.get_month() as u32,
            self.get_day() as u32,
        )
        .expect("chrono::NaiveDate::from_ymd_opt");
        let t = chrono::NaiveTime::from_hms_micro_opt(
            self.get_hour() as u32,
            self.get_minute() as u32,
            self.get_second() as u32,
            self.get_microsec() as u32,
        )
        .expect("chrono::NaiveTime::from_hms_micro_opt");
        chrono::NaiveDateTime::new(d, t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "chrono")]
    use chrono::NaiveTime;

    #[test]
    #[cfg(feature = "chrono")]
    fn chrono_for_duration() {
        let duration = Duration {
            months: 1,
            seconds: 2,
            microseconds: 3,
            ..Default::default()
        };
        assert_eq!(
            DurationWrapper::new(&duration).to_chrono_duration(),
            chrono::Duration::new(1 * 30 * 24 * 60 * 60 + 2, 3 * 1000).unwrap()
        );
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn chrono_for_date() {
        let date = Date {
            year: 2020,
            month: 1,
            day: 2,
            ..Default::default()
        };
        assert_eq!(
            DateWrapper::new(&date, &TimezoneInfo::default()).to_naive_date(),
            NaiveDate::from_ymd_opt(2020, 1, 2).unwrap()
        );
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn chrono_for_time() {
        let time = Time {
            hour: 1,
            minute: 2,
            sec: 3,
            microsec: 4,
            ..Default::default()
        };
        assert_eq!(
            TimeWrapper::new(&time, &TimezoneInfo::default()).to_naive_date_time(),
            NaiveDateTime::new(
                NaiveDate::default(),
                NaiveTime::from_hms_micro_opt(1, 2, 3, 4).unwrap(),
            )
        );
    }

    #[test]
    #[cfg(feature = "chrono")]
    fn chrono_for_datetime() {
        let date_time = DateTime {
            year: 2020,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            sec: 5,
            microsec: 6,
            ..Default::default()
        };
        assert_eq!(
            DateTimeWrapper::new(&date_time, &TimezoneInfo::default()).to_naive_date_time(),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2020, 1, 2).unwrap(),
                NaiveTime::from_hms_micro_opt(3, 4, 5, 6).unwrap(),
            )
        );
    }
}
