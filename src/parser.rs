use std::collections::HashMap;
use std::str;

use eyre::{eyre, Result};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take_while, take_while_m_n},
    character::streaming::{char, none_of, one_of},
    combinator::{cut, map, value},
    error::ParseError,
    multi::{many0, separated_list0},
    number::streaming::double,
    sequence::{preceded, separated_pair, terminated},
    IResult,
};

#[derive(Debug, PartialEq, Clone)]
pub enum JsonValue {
    Str(String),
    Null,
    Num(f64),
    Bool(bool),
    Object(HashMap<String, JsonValue>),
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

    pub fn int_value(&self) -> Result<f64> {
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

fn space<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

fn null<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, JsonValue, E> {
    tag("null")(i).and_then(|(i, _o)| Ok((i, JsonValue::Null)))
}

fn bool<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, JsonValue, E> {
    let parse_true = value(JsonValue::Bool(true), tag("true"));
    let parse_false = value(JsonValue::Bool(false), tag("false"));

    alt((parse_true, parse_false))(input)
}

fn key_value<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, (String, JsonValue), E> {
    separated_pair(
        preceded(space, string),
        cut(preceded(space, char(':'))),
        json_value,
    )(i)
}

fn hash<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, HashMap<String, JsonValue>, E> {
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

fn json_value<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, JsonValue, E> {
    preceded(
        space,
        alt((
            map(hash, JsonValue::Object),
            map(string, JsonValue::Str),
            map(double, JsonValue::Num),
            bool,
            null,
        )),
    )(i)
}

pub fn root<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, JsonValue, E> {
    preceded(space, map(hash, JsonValue::Object))(i)
}

fn string<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, String, E> {
    preceded(char('\"'), cut(terminated(parse_str, char('\"'))))(i)
}

fn parse_str<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, String, E> {
    let any_string_char = none_of("\"");
    let escaped = preceded(tag("\\"), |i| match one_of("nt\"\\")(i) {
        Ok((rest, c)) => {
            let c = match c {
                'n' => '\n',
                't' => '\t',
                x => x,
            };
            Ok((rest, c))
        }
        Err(x) => Err(x),
    });
    let string_path = alt((unicode_letter, escaped, any_string_char));
    match many0(string_path)(i) {
        Ok((rest, parts)) => Ok((rest, parts.iter().collect())),
        Err(x) => Err(x),
    }
}

fn unicode_letter<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, char, E> {
    let four_digits = |x: &'a str| take_while_m_n(4, 4, |c: char| c.is_digit(16))(x);
    let (rest, digits) = preceded(tag("\\u"), four_digits)(i)?;
    let num = u32::from_str_radix(digits, 16).expect("Couldn't parse str radix");
    let c = std::char::from_u32(num).expect("Couldn't create char from parsed str radix");
    Ok((rest, c))
}

#[cfg(test)]
mod test {
    use nom::error::ErrorKind;
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
    fn test_unicode_letter() {
        let parsed = unicode_letter::<(&str, ErrorKind)>("\\u003d");
        assert_eq!(parsed, Ok(("", '=')));
    }

    #[test]
    fn test_string_with_newline() {
        let input = "\"a\\nb\"";
        let parsed = string::<(&str, ErrorKind)>(input);
        assert_eq!(parsed, Ok(("", "a\nb".to_string())));
    }

    #[test]
    fn string_with_quote() {
        let input = r#""This is a string with '\"' quotes.""#;
        let expected = r#"This is a string with '"' quotes."#.to_string();
        let parsed = string::<(&str, ErrorKind)>(input);
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
            let (remainder, _obj) = root::<(&str, ErrorKind)>(&input)?;
            prop_assert_eq!("", remainder);
        }
    }
}
