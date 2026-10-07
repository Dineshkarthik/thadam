//! The small text grammars: units, topic filters, templates, JSON Pointers,
//! and timestamps.
#![no_main]

use libfuzzer_sys::fuzz_target;
use thadam_transform::paths::{FieldPath, Template, TopicFilter};
use thadam_transform::timeparse::{UtcOffset, parse_rfc3339};
use thadam_transform::units::{Unit, UnitConversion};

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = Unit::parse(text);
    let _ = UnitConversion::from_codes(text, "m/s");
    let _ = UnitConversion::from_codes("Cel", text);
    if let Ok(filter) = TopicFilter::parse(text) {
        let _ = filter.matches(text);
        let _ = filter.matches("a/b/c");
    }
    if let Ok(filter) = TopicFilter::parse("a/+/#") {
        let _ = filter.matches(text);
    }
    let _ = Template::parse(text);
    let _ = FieldPath::parse(text);
    let _ = UtcOffset::parse(text);
    let _ = parse_rfc3339(text, None);
    let _ = parse_rfc3339(text, Some(UtcOffset::UTC));
});
