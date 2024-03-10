use serde_json::{self, Value};
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::fs::{self};
use std::io::Read;

pub fn at_filtered_columns(
    table: &str,
    job_name: &str,
    year: &str,
    config_path: &str,
) -> serde_json::Result<HashMap<String, Value>> {
    let file_path = format!("{}/{}/{}/{}.json", config_path, job_name, year, table);

    let mut file_content = String::new();
    let mut file = File::open(file_path).expect("Failed to open file");
    file.read_to_string(&mut file_content)
        .expect("Failed to read file");

    let map: HashMap<String, Value> = serde_json::from_str(&file_content).unwrap();

    Ok(map)
}

#[tracing::instrument(err)]
pub async fn get_config() -> Result<Value, serde_yaml::Error> {
    let file_path = env::var("PIPELINE_CONFIG").expect("$PIPELINE_CONFIG is not set");
    let mut file = fs::File::open(file_path).expect("file should open read only");

    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("Unable to read file");

    let config: Result<Value, serde_yaml::Error> = serde_yaml::from_str(&contents);

    config
}

pub fn setup_emarsys_sources_tables() -> HashMap<&'static str, String> {
    let execution_date = env::var("CURRENT_DATE").expect("$PIPELINE_CONFIG is not set");
    let emarsys_google_project = "ems-od-lovebonito";
    let src_dataset = "emarsys_lovebonito_794867401";
    let sql_filter = format!(
        "where date(loaded_at, \"Asia/Singapore\") = \"{}\"",
        execution_date
    );

    let result: HashMap<&str, String> = [
        (
            "emarsys_normalized_reg_form_view",
            format!(
                "{}.editable_dataset.normalized_reg_form_view",
                emarsys_google_project
            ),
        ),
        (
            "emarsys_prof_forms",
            format!("{}.editable_dataset.prof_forms", emarsys_google_project),
        ),
        (
            "emarsys_suite_contact_0",
            format!(
                "{}.editable_dataset.suite_contact_0",
                emarsys_google_project
            ),
        ),
        (
            "emarsys_email_bounces",
            format!(
                "{}.{}.email_bounces_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_campaign_categories",
            format!(
                "{}.{}.email_campaign_categories_794867401",
                emarsys_google_project, src_dataset
            ),
        ),
        (
            "emarsys_email_campaigns",
            format!(
                "{}.{}.email_campaigns_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_campaigns_v2",
            format!(
                "{}.{}.email_campaigns_v2_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_cancels",
            format!(
                "{}.{}.email_cancels_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_clicks",
            format!(
                "{}.{}.email_clicks_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_complaints",
            format!(
                "{}.{}.email_complaints_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_opens",
            format!(
                "{}.{}.email_opens_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_sends",
            format!(
                "{}.{}.email_sends_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_email_unsubscribes",
            format!(
                "{}.{}.email_unsubscribes_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_session_categories",
            format!(
                "{}.{}.session_categories_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_session_purchases",
            format!(
                "{}.{}.session_purchases_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_session_views",
            format!(
                "{}.{}.session_views_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
        (
            "emarsys_sessions",
            format!(
                "{}.{}.sessions_794867401 {}",
                emarsys_google_project, src_dataset, sql_filter
            ),
        ),
    ]
    .iter()
    .cloned()
    .collect();

    result
}

pub fn setup_emarsys_columns() -> HashMap<&'static str, &'static str> {
    let datalake_emarsys: HashMap<&str, &str> = [
        ("emarsys_campaign", "
            campaign_id,campaign_name,created_at,deleted,fromemail,fromname,subject,email_category
        "),
        ("emarsys_email_bounces", "
            contact_id,launch_id,domain,email_sent_at,campaign_type,bounce_type,campaign_id,message_id,event_time,customer_id,partitiontime,loaded_at,dsn_reason
        "),
        ("emarsys_email_campaign_categories", "
            id,name,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_email_campaigns", "
            id,name,version_name,language,category_id,parent_campaign_id,type,sub_type,program_id,customer_id,partitiontime,event_time,loaded_at
        "),
        ("emarsys_email_campaigns_v2", "
            campaign_id,origin_campaign_id,is_recurring,name,timezone,version_name,language,program_id,program_version_id,suite_type,suite_event,campaign_type,defined_type,category_name,event_time,customer_id,partitiontime,loaded_at,subject
        "),
        ("emarsys_email_cancels", "
            contact_id,launch_id,reason,campaign_type,suite_type,suite_event,campaign_id,message_id,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_email_clicks", "
            contact_id,launch_id,domain,email_sent_at,campaign_type,geo,platform,md5,is_mobile,is_anonymized,uid,ip,user_agent,section_id,link_id,category_id,is_img,campaign_id,message_id,event_time,customer_id,partitiontime,loaded_at,category_name,link_name,'' as temp_column,relative_link_id
        "),
        ("emarsys_email_complaints", "
            contact_id,launch_id,domain,email_sent_at,campaign_type,campaign_id,message_id,event_time,customer_id,loaded_at,partitiontime
        "),
        ("emarsys_email_opens", "
            contact_id,launch_id,domain,email_sent_at,campaign_type,geo,platform,md5,is_mobile,is_anonymized,uid,ip,user_agent,generated_from,campaign_id,message_id,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_email_sends", "
            contact_id,launch_id,campaign_type,domain,campaign_id,message_id,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_email_unsubscribes", "
            contact_id,launch_id,domain,email_sent_at,campaign_type,source,campaign_id,message_id,event_time,customer_id,loaded_at,partitiontime
        "),
        ("emarsys_normalized_reg_form_view", "
            contact_id,customer_id,subscriber_id,contact_form,contact_source,opt_in,store_opt_in,registration_date,form_name
        "),
        ("emarsys_prof_forms", "
            bit_position,form_name
        "),
        ("emarsys_session_categories", "
            user_id,user_id_type,user_id_field_id,contact_id,category,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_session_purchases", "
            user_id,user_id_type,user_id_field_id,contact_id,items,order_id,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_session_views", "
            user_id,user_id_type,user_id_field_id,contact_id,item_id,event_time,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_sessions", "
            start_time,end_time,purchases,views,tags,categories,last_cart,user_id,user_id_type,user_id_field_id,contact_id,currency,customer_id,partitiontime,loaded_at
        "),
        ("emarsys_suite_contact_0", "
            contact_id,customer_id,subscriber_id,contact_form,contact_source,opt_in,store_opt_in,registration_date
        "),
    ]
    .iter()
    .cloned()
    .collect();

    datalake_emarsys
}
