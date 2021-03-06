#[macro_export]
macro_rules! warc_result {
    ($warc: ident ) => {
        match $warc.next() {
            None => rust_warc::WarcRecord {
                version: String::from("0"),
                header: HashMap::<rust_warc::CaseString, String>::new(),
                content: Vec::<u8>::new(),
            },
            Some(w) => match w {
                Ok(w) => w,
                Err(_e) => rust_warc::WarcRecord {
                    version: String::from("0"),
                    header: HashMap::<rust_warc::CaseString, String>::new(),
                    content: Vec::<u8>::new(),
                },
            },
        }
    };
}

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::io::Read;
use std::str::FromStr;
use warcraider::*;
mod blacklist;

use addr::DomainName;
use avro_rs::{to_value, types::Record, Schema, Writer};
use env_logger;
use failure::Error;
use git_version::git_version;
use libflate::gzip::Decoder;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::*;
use rust_warc::WarcReader;
#[cfg(not(target_os = "windows"))]
use stackdriver_logger;
use subprocess::{Exec, Redirection};

static GA_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\bUA-\d{4,10}-\d{1,4}\b|\bGTM-[A-Z0-9]{1,7}\b").unwrap());

static GA_CONFIG_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"ga\((.*?)\)").unwrap());

static HOSTNAME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"://(.*?(\.au|\.com|\.net|\.org)?)(:|/)").unwrap());

static WHITESPACE_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(\s|\\n){2,}").unwrap());
static HTML_TAG_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?s)</*.*?>").unwrap());
static HTML_BODY_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?s)<(?:body|BODY).*>(.*)").unwrap());
static HTML_TITLE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?sU)<(?:title|TITLE).*>(.*)<").unwrap());
static HTML_SCRIPT_STYLE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?sU)(<(?:script|SCRIPT|style|STYLE).*>.*</(?:script|SCRIPT|style|STYLE).*>)")
        .unwrap()
});
// https://stackoverflow.com/a/15926317
static HTML_LINK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?s)\s+(?:[^>]*?\s+)?href=["'](.*?)["']"#).unwrap());
static HTML_RESOURCE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?s)\s+(?:[^>]*?\s+)?src=["'](.*?)["']"#).unwrap());
static SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::parse_str(
        r#"
    {
    "name": "url_resource",
    "type": "record",
    "fields": [
        {"name": "url", "type": "string"},
        {"name": "hostname", "type": "string"},
        {"name": "domain_name", "type": "string"},
        {"name": "size_bytes", "type": "int"},
        {"name": "load_time", "type": "float"},
        {"name": "title", "type": "string"},
        {"name": "text_content", "type": "string"},
        {"name": "headings_text", "type": "string"},
        {"name": "word_count", "type": "int"},
        {"name": "links", "type": {"type": "array", "items": "string"}},
        {"name": "resource_urls", "type": {"type": "array", "items": "string"}},
        {"name": "keywords", "type": {"type": "map", "values": "float"}},
        {"name": "meta_tags", "type": {"type": "map", "values": "string"}},
        {"name": "headers", "type": {"type": "map", "values": "string"}},
        {"name": "google_analytics", "type": {"type": "array", "items": "string"}},
        {"name": "google_analytics_config", "type": {"type": "array", "items": "string"}},
        {"name": "html_errors", "type": "string"},
        {"name": "source", "type": "string"}
    ]
    }
    "#,
    )
    .unwrap()
});

struct WarcResult {
    url: String,
    hostname: String,
    size: i32,
    bytes: Vec<u8>,
}
fn main() -> Result<(), Error> {
    #[cfg(target_os = "windows")]
    env_logger::init();
    #[cfg(not(target_os = "windows"))]
    stackdriver_logger::init_with_cargo!();

    info!(
        "warcraider version {} working dir {}",
        git_version!(),
        env::current_dir()?.display()
    );

    let tidy = Exec::cmd("tidy")
        .arg("-V")
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Pipe)
        .capture()
        .unwrap();
    info!("tidy version: {}", tidy.stdout_str());

    let mut warc_number: usize = 0;
    let report_number: usize;
    match env::var("REPORT_NUMBER") {
        Ok(val) => report_number = val.parse::<usize>().unwrap(),
        Err(_e) => report_number = 4,
    }
    let mut replica: usize;
    match env::var("REPLICA") {
        Ok(val) => replica = val.parse::<usize>().unwrap(),
        Err(_e) => replica = 1,
    }
    let args: Vec<_> = env::args().collect();
    if args.len() > 1 {
        replica = args[1].parse::<usize>().unwrap();
    }
    warc_number += replica;
    let replicas: usize;
    match env::var("REPLICAS") {
        Ok(val) => replicas = val.parse::<usize>().unwrap(),
        Err(_e) => replicas = 1,
    }
    match env::var("OFFSET") {
        Ok(val) => warc_number += val.parse::<usize>().unwrap() - 1,
        Err(_e) => warc_number += 0,
    }

    while warc_number <= 96 {
        if warc_number == 99 {
            warn!("404 not found");
            warc_number += replicas;
        } else {
            info!("processing warc {}", warc_number);
            process_warc(report_number, warc_number, 0, 50_000)?;
            process_warc(report_number, warc_number, 50_000, 100_000)?;

            warc_number += replicas;
        }
    }
    info!("all warcs done for replica {}!", replica);
    Ok(())
}

fn process_warc(
    report_number: usize,
    warc_number: usize,
    start_at: usize,
    finish_at: usize,
) -> Result<(), Error> {
    let mut i = 0;

    let avro_filename = String::from("")
        + "dta-report0"
        + report_number.to_string().as_str()
        + "-"
        + warc_number.to_string().as_str()
        + "-"
        + start_at.to_string().as_str()
        + ".avro";
    let present = if check_present_avro(&avro_filename) {
        warn!("{} already in google storage bucket", &avro_filename);
        true
    } else {
        let file =
            io::BufWriter::with_capacity(256 * 1024, fs::File::create(&avro_filename).unwrap());
        let mut writer = Writer::new(&SCHEMA, file);
        let warc_filename = String::from("")
            + "dta-report0"
            + report_number.to_string().as_str()
            + "-"
            + warc_number.to_string().as_str()
            + ".warc";
        download_warc(&warc_filename, report_number, warc_number);
        let f = fs::File::open(&warc_filename).expect("Unable to open file");
        let br = io::BufReader::new(f);

        let mut warc = WarcReader::new(br);

        warc.next();
        loop {
            if i >= finish_at {
                info!(
                    "no more warc records in {} between {} and {}",
                    warc_filename, start_at, finish_at
                );
                break;
            } else if i <= start_at {
                //debug!("skipping {}",i);
                i += 4;
                warc.next();
                warc.next();
                warc.next();
                warc.next();
            } else {
                i += 4;
                let items: Vec<WarcResult> = [
                    warc_result!(warc),
                    warc_result!(warc),
                    warc_result!(warc),
                    warc_result!(warc),
                ]
                .par_iter()
                .filter_map(move |item| {
                    let warc_record = item;
                    if warc_record.version != "0"
                        && warc_record.header.get(&"WARC-Type".into()) == Some(&"response".into())
                    {
                        let url = String::from("")
                            + warc_record
                                .header
                                .get(&"WARC-Target-URI".into())
                                .unwrap()
                                .as_str();
                        let size = warc_record
                            .header
                            .get(&"Uncompressed-Content-Length".into())
                            .unwrap_or(&String::from("0"))
                            .parse::<i32>()
                            .unwrap();
                        let hostname = match HOSTNAME_REGEX.captures(&url) {
                            Some(caps) => String::from(caps.get(1).unwrap().as_str()),
                            None => String::from(""),
                        };
                        if blacklist::on_blacklist(&hostname, &url) {
                            debug!(
                                "{}:{} excluded url {} as too complex or too large",
                                warc_number, i, url
                            );
                            None
                        } else {
                            Some(WarcResult {
                                url,
                                size,
                                hostname,
                                bytes: warc_record.content[..].to_vec(),
                            })
                        }
                    } else {
                        None
                    }
                })
                .collect();

                // flush to disk about every 100 records
                if i % 1000 < 5 {
                    writer.flush()?;
                }

                let records: Vec<Record> = items
                    .par_iter()
                    .filter_map(|item| {
                        let mut record = Record::new(writer.schema()).unwrap();
                        let url = String::from("") + item.url.as_str();
                        if i % 500 < 5 {
                            info!("{}:{} {} ({} bytes)", warc_number, i, url, item.size);
                        } else {
                            debug!("{}:{} {} ({} bytes)", warc_number, i, url, item.size);
                        }
                        record.put("size_bytes", item.size);

                        record.put("source", String::from("") + &warc_filename);
                        match Decoder::new(&item.bytes[..]) {
                            Err(_e) => {
                                error!("{}:{} {} not valid gzip", warc_number, i, item.url);
                                None
                            }
                            Ok(mut decoder) => {
                                let mut b = Vec::new();
                                match decoder.read_to_end(&mut b) {
                                    Err(_e) => {
                                        error!(
                                            "{}:{} {} not valid gzip read",
                                            warc_number, i, item.url
                                        );
                                        None
                                    }
                                    Ok(_e) => {
                                        let content = String::from_utf8_lossy(&b).to_string();
                                        let parts: Vec<&str> = content.split("\n\r\n").collect();
                                        let mut headers = HashMap::<String, String>::new();
                                        for line in parts[0].split('\n') {
                                            if line == "" || line.starts_with("HTTP/") {
                                            } else if line.contains(": ") {
                                                let parts: Vec<&str> = line.split(": ").collect();
                                                headers.insert(
                                                    String::from(parts[0]),
                                                    String::from(parts[1]),
                                                );
                                            }
                                        }
                                        record.put(
                                            "load_time",
                                            headers
                                                .get("X-Funnelback-Total-Request-Time-MS")
                                                .unwrap_or(&String::from(""))
                                                .as_str()
                                                .parse::<f32>()
                                                .unwrap_or(0.0)
                                                / 1000.0,
                                        );
                                        record.put(
                                            "hostname",
                                            headers
                                                .get("X-Funnelback-AA-Domain")
                                                .unwrap_or(&item.hostname)
                                                .as_str(),
                                        );
                                        record.put(
                                            "domain_name",
                                            DomainName::from_str(&item.hostname.as_str())
                                                .unwrap()
                                                .root()
                                                .to_str(),
                                        );
                                        record.put("headers", to_value(headers).unwrap());

                                        let raw_html = &parts[1..parts.len()].join(" ");

                                        record.put(
                                            "google_analytics",
                                            to_value(
                                                GA_REGEX
                                                    .captures_iter(&raw_html)
                                                    .map(|cap| {
                                                        String::from(cap.get(0).unwrap().as_str())
                                                    })
                                                    .collect::<Vec<String>>(),
                                            )
                                            .unwrap(),
                                        );
                                        record.put(
                                            "google_analytics_config",
                                            to_value(
                                                GA_CONFIG_REGEX
                                                    .captures_iter(&raw_html)
                                                    .filter_map(|cap| {
                                                        let capstr = cap.get(1).unwrap().as_str();
                                                        // filter out just basic pageview events
                                                        if capstr == "\"send\", \"pageview\""
                                                            || capstr == "'send', 'pageview'"
                                                        {
                                                            None
                                                        } else {
                                                            Some(String::from(capstr))
                                                        }
                                                    })
                                                    .collect::<Vec<String>>(),
                                            )
                                            .unwrap(),
                                        );
                                        let html = if item.size > 2_000_000 || content.len() > 2_000_000 {
                                            warn!("{}:{} content too big, skipping html parsing for {} ({} bytes > 2MB)",warc_number, i, url, item.size);
                                            Default::default()
                                        } else {
                                            find_html_parser(warc_number, i, &url, &raw_html)
                                        };
                                        let text;
                                        if html.ok {
                                            text = WHITESPACE_REGEX
                                                .replace_all(&html.text.join(" "), "")
                                                .to_string();

                                            record.put("title", html.title);
                                            record.put(
                                                "links",
                                                to_value(make_urls_absolute(&url, html.links))
                                                    .unwrap(),
                                            );
                                            record.put(
                                                "resource_urls",
                                                to_value(make_urls_absolute(
                                                    &url,
                                                    html.resource_urls,
                                                ))
                                                .unwrap(),
                                            );
                                        } else {
                                            error!(
                                                "{}:{} {} could not be parsed as html even with fixes, falling back to manually extracting a subset of data ",
                                                warc_number, i, url
                                            );
                                            // if let Err(_e) = fs::write(
                                            //     format!("{}-{}-failed.htm", warc_number, i),
                                            //     &parts[1..parts.len()].join(" "),
                                            // ) {
                                            //     error!(
                                            //         "error writing {}",
                                            //         format!("{}-{}-failed.htm", warc_number, i)
                                            //     )
                                            // }

                                            // if HTML cannot be parsed, fall back to regex extraction
                                            match HTML_BODY_REGEX.captures(&raw_html) {
                                                Some(caps) => {
                                                    let txt = caps.get(0).unwrap().as_str();
                                                    let txt2 = HTML_SCRIPT_STYLE_REGEX
                                                        .replace_all(&txt, "");
                                                    let txt3 =
                                                        HTML_TAG_REGEX.replace_all(&txt2, "");
                                                    text = WHITESPACE_REGEX
                                                        .replace_all(&txt3, " ")
                                                        .to_string();
                                                }
                                                None => text = String::from(""),
                                            }
                                            match HTML_TITLE_REGEX.captures(&raw_html) {
                                                Some(caps) => record
                                                    .put("title", caps.get(1).unwrap().as_str()),
                                                None => record.put("title", ""),
                                            }
                                            record.put(
                                                "links",
                                                to_value(make_urls_absolute(
                                                    &url,
                                                    HTML_LINK_REGEX
                                                        .captures_iter(&raw_html)
                                                        .map(|cap| {
                                                            String::from(
                                                                cap.get(1).unwrap().as_str(),
                                                            )
                                                        })
                                                        .collect::<Vec<String>>(),
                                                ))
                                                .unwrap(),
                                            );
                                            record.put(
                                                "resource_urls",
                                                to_value(make_urls_absolute(
                                                    &url,
                                                    HTML_RESOURCE_REGEX
                                                        .captures_iter(&raw_html)
                                                        .map(|cap| {
                                                            String::from(
                                                                cap.get(1).unwrap().as_str(),
                                                            )
                                                        })
                                                        .collect::<Vec<String>>(),
                                                ))
                                                .unwrap(),
                                            );
                                        }
                                        //debug!("text-c");
                                        record.put(
                                            "word_count",
                                            text.par_split_whitespace().count() as i32,
                                        );
                                        record.put("headings_text", html.headings_text.join(" "));
                                        record.put("meta_tags", to_value(html.meta_tags).unwrap());
                                        record.put("keywords", keywords(String::from("") + &text));
                                        record.put("text_content", text);
                                        record.put("html_errors", html.html_errors);
                                        record.put("url", url);
                                        //dbg!(&record);
                                        Some(record)
                                    }
                                }
                            }
                        }
                    })
                    .collect();
                for record in records {
                    if let Err(e) = writer.append(record) {
                        error!("{}", e);
                        //dbg!(record);
                        error!("bad record");
                    }
                }
            }
        }
        writer.flush()?;

        false
    };
    if !present {
        let upload = Exec::shell("gsutil")
            .arg("cp")
            .arg(&avro_filename)
            .arg(
                String::from("gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/")
                    + &avro_filename,
            )
            .stdout(Redirection::Pipe)
            .capture()
            .unwrap()
            .stdout_str();
        info!("{:?}", upload);
    }

    Ok(())
}
