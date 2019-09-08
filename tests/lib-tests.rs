#[cfg(test)]
mod tests {
    use insta::{assert_debug_snapshot, assert_json_snapshot};

    #[test]
    fn test_html_parser_snapshots() {
        let raw_html = std::fs::read_to_string("tests/moneysmart.htm").unwrap();
        let html: warcraider::HTMLResult = warcraider::find_html_parser(1, 1,
"https://www.moneysmart.gov.au/life-events-and-you/life-events/divorce-and-separation/divorce-and-separation-financial-checklist", &raw_html);
        assert_json_snapshot!( html,{
        ".meta_tags" => "[meta_tags]"
        });
        let mut meta_tags = html
            .meta_tags
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<String>>();
        meta_tags.sort();
        assert_debug_snapshot!(meta_tags);
    }

    #[test]
    fn test_make_urls_absolute() {
        let result = warcraider::make_urls_absolute(
            "http://example.com",
            vec![
                String::from("/index.htm"),
                String::from("http://google.com"),
            ],
        );
        //dbg!(&result);
        assert_eq!(
            result,
            ["http://example.com/index.htm", "http://google.com/"]
        );
    }
    #[test]
    fn test_make_urls_absolute_trailing_quote() {
        let result = warcraider::make_urls_absolute(
            "http://example.com",
            vec![
                String::from("/index.htm"),
                String::from("/index.htm'"),
                String::from("/index.htm\""),
                String::from("/index.htm&quot;"),
                String::from("/index.htm%20"),
                String::from("/notindex.htm"),
            ],
        );
        //dbg!(&result);
        assert!(
            result
                == vec![
                    "http://example.com/index.htm",
                    "http://example.com/notindex.htm"
                ]
        );
    }
}
