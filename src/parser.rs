use nom::{
    branch::alt,
    bytes::streaming::{escaped, take_while},
    character::streaming::{char, none_of, one_of},
    combinator::{cut, map},
    error::{context, ContextError, ParseError},
    multi::separated_list0,
    number::streaming::double,
    sequence::{preceded, separated_pair, terminated},
    IResult,
};
use std::collections::HashMap;
use std::error::Error;
use std::str;

#[derive(Debug, PartialEq)]
pub enum JsonValue {
    Str(String),
    Num(f64),
    Object(HashMap<String, JsonValue>),
}

#[derive(Debug)]
pub struct StringError {
    message: String,
}

impl std::fmt::Display for StringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "String error: {}", self.message)
    }
}

impl Error for StringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

impl StringError {
    pub fn new(message: String) -> Self {
        StringError { message: message }
    }

    pub fn str(message: &str) -> Self {
        Self::new(message.to_string())
    }

    pub fn boxed(message: &str) -> Box<Self> {
        Box::new(Self::str(message))
    }
}

impl JsonValue {
    pub fn map_value(&self, key: &str) -> Result<&JsonValue, Box<dyn Error>> {
        let map = match self {
            JsonValue::Object(x) => x,
            _ => return Err(StringError::boxed("Something is wrong")),
        };

        match map.get(key) {
            Some(value) => Ok(value),
            None => Err(StringError::boxed("Key not found")),
        }
    }

    pub fn int_value(&self) -> Result<f64, Box<dyn Error>> {
        match self {
            JsonValue::Num(x) => Ok(*x as f64),
            _ => Err(StringError::boxed("Value is not a number")),
        }
    }

    pub fn str_value(&self) -> Result<String, Box<dyn Error>> {
        match self {
            JsonValue::Str(x) => Ok(x.clone()),
            _ => Err(StringError::boxed("Value is not a string")),
        }
    }
}

fn space<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

fn parse_str<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let end_of_str = none_of("\"");
    escaped(end_of_str, '\\', one_of(r#"n\""#))(i)
}

fn string<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, &'a str, E> {
    context(
        "string",
        preceded(char('\"'), cut(terminated(parse_str, char('\"')))),
    )(i)
}

fn key_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, (&'a str, JsonValue), E> {
    separated_pair(
        preceded(space, string),
        cut(preceded(space, char(':'))),
        json_value,
    )(i)
}

fn hash<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, HashMap<String, JsonValue>, E> {
    context(
        "map",
        preceded(
            char('{'),
            cut(terminated(
                map(
                    separated_list0(preceded(space, char(',')), key_value),
                    |tuple_vec| {
                        tuple_vec
                            .into_iter()
                            .map(|(k, v)| (String::from(k), v))
                            .collect()
                    },
                ),
                preceded(space, char('}')),
            )),
        ),
    )(i)
}

fn json_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, JsonValue, E> {
    preceded(
        space,
        alt((
            map(hash, JsonValue::Object),
            map(string, |s| JsonValue::Str(String::from(s))),
            map(double, JsonValue::Num),
        )),
    )(i)
}

pub fn root<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, JsonValue, E> {
    preceded(space, map(hash, JsonValue::Object))(i)
}

#[cfg(test)]
mod test {
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
}
