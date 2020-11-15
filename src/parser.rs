use nom::{
    branch::alt,
    bytes::streaming::take_while,
    character::streaming::{char, none_of, one_of},
    combinator::{cut, map},
    error::ParseError,
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
                |tuple_vec| {
                    tuple_vec
                        .into_iter()
                        .map(|(k, v)| (String::from(k), v))
                        .collect()
                },
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
            map(string, |s| JsonValue::Str(s)),
            map(double, JsonValue::Num),
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
    use nom::bytes::streaming::tag;
    use nom::multi::many0;

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
        Err(x) => return Err(x),
    }
}

fn unicode_letter<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, char, E> {
    use nom::bytes::streaming::{tag, take_while_m_n};

    let four_digits = |x: &'a str| take_while_m_n(4, 4, |c: char| c.is_digit(16))(x);
    let my_tag = tag("\\u");
    let (rest, digits) = preceded(my_tag, four_digits)(i)?;
    let num = u32::from_str_radix(digits, 16).expect("Couldn't parse str radix");
    let c = std::char::from_u32(num).expect("Couldn't create char from parsed str radix");
    return Ok((rest, c));
}

#[cfg(test)]
mod test {
    use super::*;
    use nom::error::ErrorKind;
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
}
