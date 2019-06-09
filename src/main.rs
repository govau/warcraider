#[macro_use]
extern crate lazy_static;
#[macro_export]
macro_rules! warc_result {
    ($warc: ident ) => {
					match $warc.next() {
						None => rust_warc::WarcRecord {
				version: String::from("0"),
				header: HashMap::<rust_warc::CaseString, String>::new(),
				content: Vec::<u8>::new(),
			},
						Some(w) => {match w {
						Ok(w) => w,
						Err(_e) => rust_warc::WarcRecord {
				version: String::from("0"),
				header: HashMap::<rust_warc::CaseString, String>::new(),
				content: Vec::<u8>::new(),
			}
						}
						}
					}
    }
}

use std::collections::HashMap;
use std::env;
use std::fs;
use std::io;
use std::io::Read;
use std::str::FromStr;
use warcraider::*;

use addr::DomainName;
use avro_rs::{to_value, types::Record, Schema, Writer};
use env_logger;
use failure::Error;
use git_version::git_version;
use libflate::gzip::Decoder;
use log::{debug, error, info, warn};
use rayon::prelude::*;
use regex::*;
use rust_warc::WarcReader;
use soup::*;
//use stackdriver_logger;
use subprocess::{Exec, Redirection};
lazy_static! {
    static ref GA_REGEX: Regex =
        Regex::new(r"\bUA-\d{4,10}-\d{1,4}\b|\bGTM-[A-Z0-9]{1,7}\b").unwrap();
}
lazy_static! {
    static ref A_REGEX: Regex = Regex::new(r"</*a *.*?>").unwrap();
}
lazy_static! {
    static ref P_REGEX: Regex = Regex::new(r"</*p *.*?>").unwrap();
}
lazy_static! {
    static ref BR_REGEX: Regex = Regex::new(r"</*br/*?>").unwrap();
}
lazy_static! {
    static ref TD_REGEX: Regex = Regex::new(r"</*t(r|h|d)/*?>").unwrap();
}
lazy_static! {
    static ref HOSTNAME_REGEX: Regex = Regex::new(r"://(.*?(\.au|\.com|\.net|\.org)?)(:|/)").unwrap();
}

lazy_static! {
    static ref SCHEMA: Schema = Schema::parse_str(
        r#"
		{
	"name": "url_resource",
	"type": "record",
	"fields": [
		{"name": "url", "type": "string"},
		{"name": "domain_name", "type": "string"},
		{"name": "size_bytes", "type": "int"},
		{"name": "load_time", "type": "float"},
		{"name": "title", "type": "string"},
		{"name": "google_analytics", "type": "string"},
		{"name": "text_content", "type": "string"},
		{"name": "headings_text", "type": "string"},
		{"name": "word_count", "type": "int"},
		{"name": "links", "type": {"type": "array", "items": "string"}},
		{"name": "resource_urls", "type": {"type": "array", "items": "string"}},
		{"name": "keywords", "type": {"type": "map", "values": "float"}},
		{"name": "meta_tags", "type": {"type": "map", "values": "string"}},
		{"name": "headers", "type": {"type": "map", "values": "string"}},
		{"name": "source", "type": "string"}
	]
		}
	"#
    )
    .unwrap();
}

struct WarcResult {
    url: String,
    hostname: String,
    size: i32,
    bytes: Vec<u8>,
}
fn main() -> Result<(), Error> {
    env_logger::init();
    //stackdriver_logger::init_with_cargo!();
    info!(
        "warcraider version {} working dir {}",
        git_version!(),
        env::current_dir()?.display()
    );

    let mut warc_number: usize = 0;
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

    while warc_number <= 85 {
        if warc_number == 59 {
            warn!("404 not found");
            warc_number += replicas;
        } else {
            info!("processing warc {}", warc_number);
            process_warc(warc_number, 0, 50_000)?;
            process_warc(warc_number, 50_000, 100_000)?;

            warc_number += replicas;
        }
    }
    info!("all warcs done for replica {}!", replica);
    Ok(())
}

fn process_warc(warc_number: usize, start_at: usize, finish_at: usize) -> Result<(), Error> {
    let mut i = 0;

    let avro_filename = String::from("")
        + "dta-report02-"
        + warc_number.to_string().as_str()
        + "-"
        + start_at.to_string().as_str()
        + ".avro";
    let mut present = false;
    if check_present_avro(&avro_filename) {
        warn!("{} already in google storage bucket", &avro_filename);
        present = true;
    } else {
        let file = io::BufWriter::new(fs::File::create(&avro_filename).unwrap());
        let mut writer = Writer::new(&SCHEMA, file);
        let warc_filename =
            String::from("") + "dta-report02-" + warc_number.to_string().as_str() + ".warc";
        download_warc(&warc_filename, warc_number);
        let f = fs::File::open(&warc_filename).expect("Unable to open file");
        let br = io::BufReader::new(f);

        let mut warc = WarcReader::new(br);

        warc.next();
        loop {
            
            if i >= finish_at {
                info!("no more warc records");
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
                    warc_result!(warc)
				]
				.par_iter()
				.filter_map(move |item| {
					let warc_record = item;
					if warc_record.version != "0" && warc_record.header.get(&"WARC-Type".into()) == Some(&"response".into()) {
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
                        //debug!("regex hs: {}",hostname);
						if size > 2_000_000 || warc_record.content.len() > 2_000_000 {
							warn!("{}:{} too big {} ({} bytes > 2MB)",warc_number, i, url, size);
							None
						}  else if [ "insolvencynotices.asic.gov.au",
								"data.gov.au",
								"trove.nla.gov.au",
								"data.aad.gov.au",
								"www.trove.nla.gov.au",
								"epubs.aims.gov.au",
								"services.aad.gov.au",
								"results.aec.gov.au",
								"periodicdisclosures.aec.gov.au",
								"transcribe.naa.gov.au",
								"bookshop.nla.gov.au",
								"recordsearch.naa.gov.au",
								"library.nma.gov.au",
								"abr.business.gov.au",
								"collections.anmm.gov.au",
								"elibrary.gbrmpa.gov.au",
								"channelfinder.acma.gov.au",
								"vrroom.naa.gov.au",
								"www.tenders.gov.au",
								"dmzapp17p.ris.environment.gov.au",
								"discoveringanzacs.naa.gov.au",
								"elibrary.gbrmpa.gov.au",
								"neats.nopta.gov.au",
								"results.aec.gov.au",
								"recordsearch.naa.gov.au",
								"services.aad.gov.au",
								"soda.naa.gov.au",
								"stat.data.abs.gov.au",
								"store.anmm.gov.au",
								"toiletmap.gov.au",
								"training.gov.au",
								"transcribe.naa.gov.au",
								"wels.agriculture.gov.au",
								"www.padil.gov.au",
								"www.screenaustralia.gov.au"
					].contains(&hostname.as_str()) ||
					url == "http://www.nepc.gov.au/system/files/resources/45fee0f3-1266-a944-91d7-3b98439de8f8/files/dve-prepwk-project2-1-diesel-complex-cuedc.xls" ||
					url == "https://www.ncver.edu.au/__data/assets/word_doc/0013/3046/2221s.doc" ||
					url =="https://www.acma.gov.au/-/media/Broadcast-Carriage-Policy/Information/Word-document/reg_qld-planning_data-docx.docx?la=en" ||
					url == "https://www.acma.gov.au/-/media/Broadcasting-Spectrum-Planning/Information/Word-Document-Digital-TV/Planning-data-Regional-Queensland-TV1.docx?la=en" ||
					url.matches("ca91-4-xd").count() > 0 {
						warn!("{}:{} bad url {}", warc_number, i, url);
						None
						} else {
							Some(WarcResult {
								url: url,
								size: size,
								hostname: hostname,
								bytes: warc_record.content[..].to_vec(),
							})
						}
					} else {
						None
					}
				})
				.collect();

                if i % 1000 < 10 {
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
                                return None;
                            }
                            Ok(mut decoder) => {
                                let mut b = Vec::new();
                                let mut content: String;
                                match decoder.read_to_end(&mut b) {
                                    Ok(_e) => content = String::from_utf8_lossy(&b).to_string(),
                                    Err(_e) => {
                                        error!("{}:{} {} not valid gzip read", warc_number, i, item.url);
                                        return None;
                                    }
                                }

                                let parts: Vec<&str> = content.split("\n\r\n").collect();
                                let mut raw_html = BR_REGEX
                                    .replace_all(&parts[1..parts.len()].join(" "), "")
                                    .to_string();
                                if raw_html.matches("<").count() > 30000 {
                                    warn!(
                                        "{}:{} {} contains too many html tags ({})",
                                        warc_number,
                                        i,
                                        url,
                                        raw_html.matches("<").count()
                                    );
                                    // fs::write(
                                    //     format!("{}-{}.htm", warc_number, i),
                                    //     &content,
                                    // )?;
                                }
                                if raw_html.matches("<a ").count() > 9500 {
                                    error!(
                                        "{}:{} {} contains too many <a> tags ({}), fixing",
                                        warc_number,
                                        i,
                                        url,
                                        raw_html.matches("<a ").count()
                                    );
                                    raw_html = A_REGEX.replace_all(&raw_html, "").to_string();
                                }
                                if raw_html.contains("<p/>")
                                    && raw_html.matches("<p>").count() > 10000
                                {
                                    error!(
                                        "{}:{} {} contains too many <p> tags ({}), fixing",
                                        warc_number,
                                        i,
                                        url,
                                        raw_html.matches("<p>").count()
                                    );
                                    raw_html = P_REGEX.replace_all(&raw_html, "").to_string();
                                }
                                let td_tags = TD_REGEX.find_iter(&raw_html).count();
                                if td_tags > 10000 {
                                    error!(
											"{}:{} {} contains too many <td>/<tr>/<th> tags ({}), fixing",
											warc_number,i,
											url,
											td_tags
										);
                                    raw_html = TD_REGEX.replace_all(&raw_html, "").to_string();
                                }
                                let mut headers = HashMap::<String, String>::new();
                                for line in parts[0].split("\n") {
                                    if line == "" || line.starts_with("HTTP/") {
                                    } else if line.contains(": ") {
                                        let parts: Vec<&str> = line.split(": ").collect();
                                        headers
                                            .insert(String::from(parts[0]), String::from(parts[1]));
                                    }
                                }

                                //debug!("size-b");
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
                                //debug!("load");
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

                                //debug!("headers");
                                //debug!("{}",raw_html);
                                let soup = Soup::new(&raw_html);
                                let text = parse_html_to_text(&soup);
                                if text.len() < 10 && url.matches("pdf").count() == 0 {
                                //     match fs::write(
                                //         format!("{}-{}.htm", warc_number, i),
                                //         &content,
                                //     ) {
                                //         Ok(_e) => error!(
                                //         "{}:{} {} has too short text ({} characters)",
                                //         warc_number,i,
                                //         url, text.len()
                                //      ),
                                //         Err(_e) =>
                                    error!(
                                        "{}:{} {} has too short text ({} characters)",
                                        warc_number,
                                        i,
                                        url,
                                        text.len()
                                     )
                                //      }
                                }
                                let text_words = String::from("") + text.as_str();
                                match soup.tag("title").find() {
                                    Some(title) => record.put("title", title.text().trim()),
                                    None => record.put("title", ""),
                                }
                                //debug!("title");
                                record.put("text_content", text);
                                //debug!("text-c");
                                record.put(
                                    "word_count",
                                    text_words.par_split_whitespace().count() as i32,
                                );
                                //debug!("Wordc");
                                match GA_REGEX.captures(&raw_html) {
                                    Some(caps) => record
                                        .put("google_analytics", caps.get(0).unwrap().as_str()),
                                    None => record.put("google_analytics", ""),
                                }
                                //debug!("ga");
                                record.put("headings_text", headings_text(&soup));
                                //debug!("headingt");
                                record.put(
                                    "links",
                                    to_value(
                                        soup.tag("a")
                                            .find_all()
                                            .filter_map(|link| link.get("href"))
                                            .collect::<Vec<_>>(),
                                    )
                                    .unwrap(),
                                );
                                //debug!("links");
                                record
                                    .put("resource_urls", to_value(resource_urls(&soup)).unwrap());
                                //debug!("resource");
                                record.put("meta_tags", to_value(meta_tags(&soup)).unwrap());
                                //debug!("meta");
                                record.put("keywords", keywords(text_words));
                                //debug!("keywords");
                                //dbg!(record);
                                record.put("url", url);
                                Some(record)
                            }
                        }
                    })
                    .collect();
                for record in records {
                    writer.append(record)?;
                }
            }
        }
        writer.flush()?;
    }
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
