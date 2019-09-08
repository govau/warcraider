#[cfg(test)]
mod tests {
    //find_html_parser(warc_number, i, &url, &raw_html)
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
