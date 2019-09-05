use once_cell::sync::Lazy;
use regex::Regex;

static NUM_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"-?\p{N}+[./٫,']?\p{N}*").unwrap());

pub(crate) trait NumberChecker {
    fn is_number(&self) -> bool;
}

impl<'a> NumberChecker for &'a str {
    fn is_number(&self) -> bool {
        NUM_RE.is_match(self)
    }
}
