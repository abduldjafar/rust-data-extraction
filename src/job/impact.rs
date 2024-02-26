use std::str::FromStr;
use chrono::{ Datelike, Duration, NaiveDate};


pub async fn fetch_sync(execution_date:&str) -> Result<(), Box<dyn std::error::Error>>{
        let load_date = NaiveDate::from_str(execution_date)?;
        let tdy_month = load_date.month();
        let tmr_month = (load_date + Duration::days(1)).month();
        let mut window_start_date: NaiveDate = NaiveDate::default();

        if tdy_month != tmr_month {
            // Calculate the date three months ago with the day set to 1
            let date_60_days_ago = load_date - Duration::days(90);
            
            window_start_date = NaiveDate::from_ymd_opt(
                date_60_days_ago
                    .year(),
                date_60_days_ago
                    .month(),
                1,
            ).unwrap();

            
        }else {
            window_start_date = NaiveDate::from_str(execution_date)? - Duration::days(3);
        }
        
        let end_end_date = execution_date;
        let window_start_date_str = window_start_date.to_string();
        let start_date = window_start_date_str.as_str();

        println!("{:?},{:?}", start_date,end_end_date);
        Ok(())
    }