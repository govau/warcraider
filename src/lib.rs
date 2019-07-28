#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate failure;

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path;

use log::{debug, error, info, warn};
use quick_xml::events::Event;
use quick_xml::Reader;
use rake::*;
use rayon::prelude::*;
use regex::*;
use soup::*;

use ammonia::Builder;
use subprocess::{Exec, ExitStatus, Redirection};
use url::Url;

#[derive(Debug, Fail)]
pub enum HTMLError {
    #[fail(display = "invalid html")]
    InvalidHTML {},
}
#[derive(Debug)]
pub struct HTMLResult {
    pub ok: bool,
    pub html_errors: String,
    pub title: String,
    pub text: Vec<String>,
    pub headings_text: Vec<String>,
    pub links: Vec<String>,
    pub resource_urls: Vec<String>,
    pub meta_tags: HashMap<String, String>,
}

impl Default for HTMLResult {
    fn default() -> Self {
        HTMLResult {
            ok: false,
            html_errors: String::from(" "),
            title: String::from(" "),
            text: Vec::new(),
            headings_text: Vec::new(),
            links: Vec::new(),
            resource_urls: Vec::new(),
            meta_tags: HashMap::<String, String>::new(),
        }
    }
}

lazy_static! {
    static ref WHITESPACE_REGEX: Regex = Regex::new(r"\s+").unwrap();
}

fn get_cleaner(mut cleaner: Builder) -> Builder {
    let add_tags = vec!["script", "html", "head", "body", "title", "meta", "link"];
    let rm_tags = vec![
        "abbr",
        "acronym",
        "area",
        "article",
        "aside",
        "b",
        "bdi",
        "bdo",
        "blockquote",
        "br",
        "caption",
        "center",
        "cite",
        "code",
        "col",
        "colgroup",
        "data",
        "dd",
        "del",
        "details",
        "dfn",
        "div",
        "dl",
        "dt",
        "em",
        "figcaption",
        "figure",
        "footer",
        "header",
        "hgroup",
        "hr",
        "i",
        "img",
        "ins",
        "kbd",
        "kbd",
        "li",
        "map",
        "mark",
        "nav",
        "ol",
        "p",
        "pre",
        "q",
        "rp",
        "rt",
        "rtc",
        "ruby",
        "s",
        "samp",
        "small",
        "span",
        "strike",
        "strong",
        "sub",
        "summary",
        "sup",
        "table",
        "tbody",
        "td",
        "th",
        "thead",
        "time",
        "tr",
        "tt",
        "u",
        "ul",
        "var",
        "wbr",
    ];
    let mut cct = std::collections::HashSet::new();
    cct.insert("style");
    cct.insert("noscript");
    cct.insert("noframes");
    let mut attr = std::collections::HashSet::new();
    attr.insert("src");
    attr.insert("href");
    attr.insert("name");
    attr.insert("content");
    attr.insert("http-equiv");
    attr.insert("itemprop");
    attr.insert("property");

    cleaner
        .add_tags(add_tags)
        .rm_tags(&rm_tags)
        .clean_content_tags(cct)
        .generic_attributes(attr);
    cleaner
}

pub fn check_present_avro(avro_filename: &str) -> bool {
    let avro_gcs_status = Exec::shell("gsutil")
        .arg("stat")
        .arg(String::from("gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/") + avro_filename)
        .join()
        .unwrap();
    println!();
    if avro_gcs_status == ExitStatus::Exited(0) {
        warn!("avro does exist on gcs {}", avro_filename);
        true
    } else {
        info!("avro does not exist on gcs {}", avro_filename);
        false
    }
}

pub fn download_warc(warc_filename: &str, report_number: usize, warc_number: usize) {
    if !path::Path::new(&warc_filename).exists() {
        let url = format!("https://datagovau.s3.ap-southeast-2.amazonaws.com/cd574697-6734-4443-b350-9cf9eae427a2/99f43557-1d3d-40e7-bc0c-665a4275d625/dta-report0{}-{}.warc",report_number, warc_number);
        info!("starting download: {}", url);
        let mut response = chttp::get(url).unwrap();
        let mut dest = fs::File::create(&warc_filename).unwrap();
        io::copy(&mut response.body_mut(), &mut dest).unwrap();
        debug!("downloaded");
    }
}
pub fn find_html_parser(warc_number: usize, i: usize, url: &str, raw_html: &str) -> HTMLResult {
    let cleaner = get_cleaner(Builder::new());
    let mut html;
    let mut tidy_err = String::from("");
    let clean_html = cleaner.clean(raw_html).to_string();
    match parse_html(&url, &clean_html, true) {
        Ok(h) => {
            html = h;
        }
        Err(_e) => {
            debug!("{}:{} {} tidying up html", warc_number, i, url);
            // download tidy from https://github.com/htacg/tidy-html5/releases
            let tidy = Exec::cmd("tidy")
                .arg("-q")
                .arg("--show-errors=0")
                .arg("--show-info=no")
                .arg("--wrap=0")
                .arg("--vertical-space=auto")
                .stdin(raw_html)
                .stdout(Redirection::Pipe)
                .stderr(Redirection::Pipe)
                .capture()
                .unwrap();
            let tidy_html = tidy.stdout_str();
            let tidy_clean_html = cleaner.clean(&tidy_html).to_string();

            tidy_err = tidy.stderr_str();
            debug!("html errors: {}", tidy_err);

            match parse_html(&url, &tidy_clean_html, false) {
                Ok(h) => html = h,
                Err(_e) => {
                    let tag_count = raw_html.matches('<').count();
                    if tag_count > 3000 {
                        warn!(
                            "{}:{} {} contains too many html tags ({})",
                            warc_number,
                            i,
                            url,
                            raw_html.matches('<').count()
                        );
                    }
                    warn!("{}:{} {} falling back to html soup", warc_number, i, url);
                    match parse_html_soup(&url, &clean_html) {
                        Ok(h) => {
                            debug!(
                                "{}:{} {} fall back to html soup worked",
                                warc_number, i, url
                            );
                            html = h;
                        }
                        Err(_e) => html = Default::default(),
                    }
                }
            }
        }
    }
    html.html_errors = tidy_err;
    html
}

pub fn parse_html(
    url: &str,
    raw_html: &str,
    check_end_names: bool,
) -> Result<HTMLResult, HTMLError> {
    if raw_html.is_empty() {
        error!("{} can't parse empty html", url);
        return Err(HTMLError::InvalidHTML {});
    }
    let mut result: HTMLResult = Default::default();

    let mut reader = Reader::from_str(raw_html);
    reader.trim_text(true);
    reader.expand_empty_elements(true);
    reader.check_end_names(check_end_names);

    let mut buf = Vec::new();
    let mut in_body = true;
    let mut in_heading = false;
    let mut in_title = false;

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => match e.name() {
                b"meta" => {
                    let mut name = String::from("");
                    let mut value = String::from("");
                    for attr in e.attributes() {
                        if let Ok(a) = attr {
                            if a.key == b"name"
                                || a.key == b"http-equiv"
                                || a.key == b"itemprop"
                                || a.key == b"property"
                            {
                                name = String::from_utf8_lossy(&a.value).into_owned()
                            }
                            if a.key == b"content" {
                                value = String::from_utf8_lossy(&a.value).into_owned()
                            }
                        }
                    }

                    if !name.is_empty() && !value.is_empty() {
                        result.meta_tags.insert(name, value);
                    }
                }
                b"a" => {
                    for a in e.attributes() {
                        if let Ok(a) = a {
                            if a.key == b"href" {
                                let link = String::from_utf8_lossy(&a.value).into_owned();
                                if !link.starts_with('_') && !link.starts_with('#') {
                                    result.links.push(link)
                                }
                            }
                        }
                    }
                }
                b"head" | b"noscript" => in_body = false,
                b"script" | b"style" | b"link" => {
                    in_body = false;
                    for a in e.attributes() {
                        if let Ok(a) = a {
                            if a.key == b"src" || a.key == b"href" {
                                result
                                    .resource_urls
                                    .push(String::from_utf8_lossy(&a.value).into_owned());
                            }
                        }
                    }
                }
                b"body" => in_body = true,
                b"title" => in_title = true,
                b"h1" | b"h2" | b"h3" | b"h4" | b"h5" | b"h6" => in_heading = true,
                _ => (),
            },
            Ok(Event::End(ref e)) => match e.name() {
                b"h1" | b"h2" | b"h3" | b"h4" | b"h5" | b"h6" => in_heading = false,
                b"head" | b"noscript" | b"script" | b"style" => in_body = true,
                b"title" => in_title = false,
                _ => (),
            },
            Ok(Event::Text(e)) => {
                if let Ok(txt) = e.unescape_and_decode(&reader) {
                    if in_title {
                        result.title = String::from("") + &txt;
                    }
                    if in_body {
                        result.text.push(String::from("") + &txt);
                    }
                    if in_heading {
                        result.headings_text.push(String::from("") + &txt);
                    }
                }
            }
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => {
                warn!(
                    "Error {} at position {}: {:?}",
                    url,
                    reader.buffer_position(),
                    e
                );
                if !check_end_names {
                    if let Err(_e) = fs::write(
                        format!(
                            "{}-{}-broken.htm",
                            &url.replace(":", "").replace("/", ""),
                            reader.buffer_position()
                        ),
                        &raw_html,
                    ) {
                        error!(
                            "error writing {}",
                            format!(
                                "{}-{}-broken.htm",
                                &url.replace(":", "").replace("/", ""),
                                reader.buffer_position()
                            )
                        )
                    }
                }
                return Err(HTMLError::InvalidHTML {});
            }
            _ => (), // There are several other `Event`s we do not consider here
        }

        // if we don't keep a borrow elsewhere, we can clear the buffer to keep memory usage low
        buf.clear();
    }

    result.ok = true;
    Ok(result)
}

lazy_static! {
    static ref R: Rake = Rake::new(StopWords::from_file("SmartStoplist.txt").unwrap());
}
pub fn keywords(text_words: String) -> HashMap<String, f32> {
    let mut keywords = HashMap::<String, f32>::new();
    // debug!("{} words to be raked", text_words.split_whitespace().count());
    R.run(text_words.as_str()).iter().for_each(
        |&KeywordScore {
             ref keyword,
             ref score,
         }| {
            keywords.insert(String::from("") + keyword.as_str(), *score as f32);
            // debug!("{} {}", keywords.len(), keyword.as_str());
        },
    );
    keywords
}
pub fn make_urls_absolute(url: &str, mut links: Vec<String>) -> Vec<String> {
    links.sort();
    links.dedup();
    match Url::parse(url) {
        Ok(this_document) => links
            .par_iter()
            .map(move |link| match this_document.join(link) {
                Ok(l) => l.into_string(),
                Err(_e) => String::from("") + &link,
            })
            .collect(),
        Err(_e) => links,
    }
}

pub fn parse_html_soup(url: &str, raw_html: &str) -> Result<HTMLResult, HTMLError> {
    if raw_html.is_empty() {
        error!("{} can't parse empty html", url);
        return Err(HTMLError::InvalidHTML {});
    }
    let mut result: HTMLResult = Default::default();
    let soup = Soup::new(&raw_html);
    result.text = vec![parse_soup_to_text(&soup)];
    result.headings_text = vec![soup_headings_text(&soup)];
    result.resource_urls = soup_resource_urls(&soup);
    result.meta_tags = soup_meta_tags(&soup);
    match soup.tag("title").find() {
        Some(title) => result.title = String::from(title.text().trim()),
        None => result.title = String::from(""),
    }

    result.links = soup
        .tag("a")
        .find_all()
        .filter_map(|link| link.get("href"))
        .collect::<Vec<_>>();

    //dbg!(&result);
    result.ok = true;
    Ok(result)
}

pub fn parse_soup_to_text(soup: &Soup) -> String {
    match soup.tag("body").find() {
        Some(body) => WHITESPACE_REGEX
            .replace_all(
                body.children()
                    .map(|tag| {
                        if tag.name() == "script"
                            || tag.name() == "noscript"
                            || tag.name() == "style"
                        {
                            String::from("")
                        } else {
                            tag.text().trim().to_string()
                        }
                    })
                    .collect::<Vec<String>>()
                    .join(" ")
                    .as_str(),
                " ",
            )
            .to_string(),
        None => String::from(""),
    }
}

pub fn soup_headings_text(soup: &Soup) -> String {
    let mut headings_text = String::new();
    //let mut i = 0;
    for heading in vec!["h1", "h2", "h3", "h4", "h5", "h6"].iter() {
        //debug!("heading {}", *heading);
        for header in soup.tag(*heading).find_all() {
            //i += 1;
            //debug!("heading {} {} {}", *heading, i, header.text());
            let head_text = header.text();
            if !head_text.is_empty() {
                headings_text.push_str("\n ");
                headings_text.push_str(&head_text);
            }
        }
    }
    String::from(headings_text.trim())
}

pub fn soup_resource_urls(soup: &Soup) -> Vec<String> {
    let mut resource_urls: Vec<String> = [
        soup.tag("script")
            .find_all()
            .filter_map(|link| link.get("src"))
            .collect::<Vec<String>>(),
        soup.tag("link")
            .find_all()
            .filter_map(|link| link.get("href"))
            .collect::<Vec<String>>(),
        soup.tag("img")
            .find_all()
            .filter_map(|link| link.get("src"))
            .collect::<Vec<String>>(),
    ]
    .concat();
    resource_urls.sort();
    resource_urls.dedup();
    resource_urls
}

pub fn soup_meta_tags(soup: &Soup) -> HashMap<String, String> {
    let mut meta_tags = HashMap::<String, String>::new();
    soup.tag("meta").find_all().for_each(|meta| {
        let attrs = meta.attrs();
        if attrs.contains_key("name") {
            match attrs.get("content") {
                Some(i) => meta_tags.insert(attrs.get("name").unwrap().to_string(), i.to_string()),
                None => Some(String::from("?")),
            };
        } else if attrs.contains_key("http-equiv") {
            //If http-equiv is set, it is a pragma directive — information normally given by the web server about how the web page is served.
            match attrs.get("content") {
                Some(i) => {
                    meta_tags.insert(attrs.get("http-equiv").unwrap().to_string(), i.to_string())
                }
                None => Some(String::from("?")),
            };
        } else if attrs.contains_key("charset") {
            //If charset is set, it is a charset declaration — the character encoding used by the webpage.
            meta_tags.insert(
                String::from("charset"),
                attrs.get("charset").unwrap().to_string(),
            );
        } else if attrs.contains_key("itemprop") {
            //If itemprop is set, it is user-defined metadata — transparent for the user-agent as the semantics of the metadata is user-specific.
            match attrs.get("content") {
                Some(i) => {
                    meta_tags.insert(attrs.get("itemprop").unwrap().to_string(), i.to_string())
                }
                None => Some(String::from("?")),
            };
        } else if attrs.contains_key("property") {
            //facebook open graph

            match attrs.get("content") {
                Some(i) => {
                    meta_tags.insert(attrs.get("property").unwrap().to_string(), i.to_string())
                }
                None => Some(String::from("?")),
            };
        }
    });
    meta_tags
}
