use std::collections::HashMap;
use std::str;

use eyre::{eyre, Result};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take_while, take_while1},
    character::streaming::char,
    combinator::{cut, map, opt, value},
    error::{Error, ErrorKind, ParseError},
    multi::separated_list0,
    number::streaming::double,
    sequence::{preceded, separated_pair, terminated},
    Err, IResult, Needed,
};

#[derive(Debug, PartialEq, Clone)]
pub enum JsonValue {
    Str(String),
    Null,
    Num(f64),
    Bool(bool),
    Object(HashMap<String, JsonValue>),
    Array(Vec<JsonValue>),
}

#[derive(Debug, PartialEq, Clone)]
pub struct JsonValueWithSternSource {
    pub source: Option<PodAndContainer>,
    pub value: JsonValue,
}

#[derive(Debug, PartialEq, Clone)]
pub struct PodAndContainer {
    pub pod: String,
    pub container: String,
}

impl JsonValue {
    pub fn map_value(&self, key: &str) -> Result<&JsonValue> {
        let map = match self {
            JsonValue::Object(x) => x,
            _ => return Err(eyre!("map_value with key '{key}' on non-object: {self:?}")),
        };

        match map.get(key) {
            Some(value) => Ok(value),
            None => Err(eyre!("Key `{}` not found", key)),
        }
    }

    pub fn num_value(&self) -> Result<f64> {
        match self {
            JsonValue::Num(x) => Ok(*x as f64),
            _ => Err(eyre!("int_value on non-numeric")),
        }
    }

    pub fn str_value(&self) -> Result<String> {
        match self {
            JsonValue::Str(x) => Ok(x.clone()),
            _ => Err(eyre!("str_value on non-string")),
        }
    }
}

fn space<'a>(i: &'a str) -> IResult<&'a str, &'a str> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

fn null<'a>(i: &'a str) -> IResult<&'a str, JsonValue> {
    tag("null")(i).and_then(|(i, _o)| Ok((i, JsonValue::Null)))
}

fn bool<'a>(input: &'a str) -> IResult<&'a str, JsonValue> {
    let parse_true = value(JsonValue::Bool(true), tag("true"));
    let parse_false = value(JsonValue::Bool(false), tag("false"));

    alt((parse_true, parse_false))(input)
}

fn key_value<'a>(i: &'a str) -> IResult<&'a str, (String, JsonValue)> {
    separated_pair(
        preceded(space, string),
        cut(preceded(space, char(':'))),
        json_value,
    )(i)
}

fn hash<'a>(i: &'a str) -> IResult<&'a str, HashMap<String, JsonValue>> {
    preceded(
        char('{'),
        cut(terminated(
            map(
                separated_list0(preceded(space, char(',')), key_value),
                |tuple_vec| tuple_vec.into_iter().collect(),
            ),
            preceded(space, char('}')),
        )),
    )(i)
}

/// some combinators, like `separated_list0` or `many0`, will call a parser repeatedly,
/// accumulating results in a `Vec`, until it encounters an error.
/// If you want more control on the parser application, check out the `iterator`
/// combinator (cf `examples/iterator.rs`)
fn array<'a>(i: &'a str) -> IResult<&'a str, Vec<JsonValue>> {
    preceded(
        char('['),
        cut(terminated(
            separated_list0(preceded(space, char(',')), json_value),
            preceded(space, char(']')),
        )),
    )(i)
}

fn json_value<'a>(i: &'a str) -> IResult<&'a str, JsonValue> {
    preceded(
        space,
        alt((
            null,
            bool,
            map(double, JsonValue::Num),
            map(string, JsonValue::Str),
            map(hash, JsonValue::Object),
            map(array, JsonValue::Array),
        )),
    )(i)
}

pub fn root<'a>(i: &'a str) -> IResult<&'a str, JsonValueWithSternSource> {
    let (rest, _) = opt(space)(i)?;
    let (rest, pod_and_container) = opt(pod_and_container)(rest)?;
    let (rest, hashed) = preceded(space, hash)(rest)?;
    let json_value = JsonValue::Object(hashed);
    let result = JsonValueWithSternSource {
        source: pod_and_container,
        value: json_value,
    };
    Ok((rest, result))
}

fn pod_and_container<'a>(i: &'a str) -> IResult<&'a str, PodAndContainer> {
    let (rest, pod) = k8s_name(i)?;
    let (rest, container) = preceded(space, k8s_name)(rest)?;
    let result = PodAndContainer {
        pod: pod.to_string(),
        container: container.to_string(),
    };
    Ok((rest, result))
}

fn k8s_name<'a>(i: &'a str) -> IResult<&'a str, &'a str> {
    let is_word_char = |c: char| c.is_alphanumeric() || c == '_' || c == '-';
    take_while1(is_word_char)(i)
}

fn string<'a>(i: &'a str) -> IResult<&'a str, String> {
    preceded(char('\"'), cut(terminated(string_inner, char('\"'))))(i)
}

fn string_inner<'a>(i: &'a str) -> IResult<&'a str, String> {
    // Although this could have been solved with parser combinators, it was
    // twice as fast with hand coding.
    let mut buffer = String::with_capacity(256 as usize);
    let mut iterator = i.chars();

    loop {
        let i = iterator.as_str();
        let c = match iterator.next() {
            Some(c) => c,
            None => break,
        };
        if c == '"' {
            return Ok((i, buffer));
        } else if c == '\\' {
            let escaped_c = match iterator.next() {
                Some(c) => c,
                None => {
                    return Err(Err::Failure(Error::from_error_kind(i, ErrorKind::Char)));
                }
            };
            buffer.push(match escaped_c {
                'n' => '\n',
                't' => '\t',
                '\\' => '\\',
                '"' => '"',
                'u' => {
                    let mut digits = String::new();
                    for _ in 0..4 {
                        digits.push(match iterator.next() {
                            Some(c) => c,
                            None => {
                                return Err(Err::Failure(Error::from_error_kind(
                                    i,
                                    ErrorKind::Char,
                                )));
                            }
                        })
                    }
                    let num = u32::from_str_radix(&digits, 16).expect("Couldn't parse str radix");
                    let c = std::char::from_u32(num)
                        .expect("Couldn't create char from parsed str radix");
                    c
                }
                _ => return Err(Err::Failure(Error::from_error_kind(i, ErrorKind::Char))),
            });
        } else {
            buffer.push(c);
        }
    }

    return Err(Err::Incomplete(Needed::new(1)));
}

#[cfg(test)]
mod test {
    use proptest::{collection::hash_map, num, prelude::*};
    use proptest_recurse::{StrategyExt, StrategySet};
    use serde_json::{Number, Value};

    use super::*;

    #[test]
    fn test_nom() {
        use nom::bytes::streaming::escaped;
        use nom::character::streaming::digit1;
        use nom::character::streaming::one_of;

        fn esc(s: &str) -> IResult<&str, &str> {
            escaped(digit1, '\\', one_of("\"n\\"))(s)
        }

        assert_eq!(esc("123;123"), Ok((";123", "123")));
        assert_eq!(esc("12\\\"34;"), Ok((";", "12\\\"34")));
    }

    #[test]
    fn test_line_with_preceeding_pod_and_container() {
        let input = r#"timeseries-dms-sync-6c96fdcd76-ss88k timeseries-dms-sync {"timestamp":{"seconds":1760623406,"nanos":540000000},"severity":"INFO","message":"[POST] 500 /_handle_batch [9 ms]","sourceLocation":{"file":"CallLogging.kt","line":"48","function":"io.ktor.server.plugins.calllogging.CallLoggingKt.CallLogging$lambda$0$log"},"context":{"requestId":"ad560ee4-e861-4ff1-a29b-9669acff66fa"},"loggerName":"io.ktor.server.Application","logging.googleapis.com/trace":"projects/unknown/traces/ad560ee4-e861-4ff1-a29b-9669acff66fa"}"#;
        let (rest, value) = root(input).unwrap();
        assert_eq!("", rest);
        let source = value.source.unwrap();
        assert_eq!("timeseries-dms-sync-6c96fdcd76-ss88k", source.pod);
        assert_eq!("timeseries-dms-sync", source.container);
    }

    #[test]
    fn test_unicode_letter_string() {
        let parsed = string("\"\\u003d\"");
        assert_eq!(parsed, Ok(("", "=".to_string())));
    }

    #[test]
    fn test_string_with_newline() {
        let input = "\"a\\nb\"";
        let parsed = string(input);
        assert_eq!(parsed, Ok(("", "a\nb".to_string())));
    }

    #[test]
    fn string_with_quote() {
        let input = r#""This is a string with '\"' quotes.""#;
        let expected = r#"This is a string with '"' quotes."#.to_string();
        let parsed = string(input);
        assert_eq!(parsed, Ok(("", expected)));
    }

    // Property based tests
    fn arb_json(set: &mut StrategySet) -> SBoxedStrategy<Value> {
        // Serde can create valid JSON in any shape, so rather than using regexs
        // to recreate JSON from first principles, we use serde::json::Value.
        //
        // However, we don't support nested objects, or boolean values.
        prop_oneof![
            Just(Value::Null),
            any::<i64>().prop_map(|i| Value::Number(Number::from(i))),
            num::f64::NORMAL.prop_map(|f| Value::Number(Number::from_f64(f).unwrap())),
            "\\PC*".prop_map(Value::String)
        ]
        .prop_mutually_recursive(0, 4, 4, set, arb_json_object)
    }

    fn arb_json_object(set: &mut StrategySet) -> SBoxedStrategy<Value> {
        hash_map("\\PC*", set.get::<Value, _>(arb_json), 0..2)
            .prop_map(|h| Value::Object(h.into_iter().collect()))
            .sboxed()
    }

    fn arb_json_str(set: &mut StrategySet) -> impl Strategy<Value = String> {
        arb_json_object(set).prop_map(|j| j.to_string())
    }

    proptest! {
        // This is a positive test: it explores what can be parsed, not what
        // cannot be parsed. If it fails, something should be parsed and isn't,
        #[test]
        fn can_parse(input in arb_json_str(&mut Default::default())){
            let (remainder, _obj) = root(&input)?;
            prop_assert_eq!("", remainder);
        }
    }
}
