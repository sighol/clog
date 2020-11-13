use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, take_while},
    character::complete::{alphanumeric1, anychar, char, none_of, one_of},
    combinator::{cut, map, opt, value},
    error::{context, convert_error, ContextError, ErrorKind, ParseError, VerboseError},
    multi::separated_list0,
    number::complete::double,
    sequence::{delimited, preceded, separated_pair, terminated},
    Err, IResult,
};
use std::collections::HashMap;
use std::str;

#[derive(Debug, PartialEq)]
pub enum JsonValue {
    Str(String),
    Boolean(bool),
    Num(f64),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

fn space<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let chars = " \t\r\n";
    take_while(move |c| chars.contains(c))(i)
}

fn parse_str<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, &'a str, E> {
    let end_of_str = none_of("\"");
    escaped(end_of_str, '\\', one_of("\"n\\"))(i)
}

fn boolean<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, bool, E> {
    let parse_true = value(true, tag("true"));
    let parse_false = value(false, tag("false"));
    alt((parse_true, parse_false))(input)
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
            map(boolean, JsonValue::Boolean),
        )),
    )(i)
}

fn root<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<&'a str, JsonValue, E> {
    delimited(space, map(hash, JsonValue::Object), opt(space))(i)
}

fn main() {
    use std::io::{self, prelude::*};
    let mut parser = Parser::new();
    for line in io::stdin().lock().lines() {
        let unwrapped = line.unwrap();
        let answer = parser.add(&unwrapped);
        match answer {
            ParserOutput::None => (),
            ParserOutput::Text(s) => println!("Success: {}", s),
            ParserOutput::Log(l) => l.println(),
        }
    }
}

#[derive(Debug, Clone)]
struct LogLine {
    pub time: DateTime<Utc>,
    pub severity: String,
    pub message: String,
}

impl LogLine {
    fn println(&self) {
        println!("{} [{}] {}", self.time, self.severity, self.message)
    }
}

#[derive(Debug, Clone)]
enum ParserOutput {
    None,
    Text(String),
    Log(LogLine),
}

struct Parser {
    buffer: String,
}

fn get_log_line(parsed: JsonValue) -> Result<LogLine, ()> {
    if let JsonValue::Object(map) = parsed {
        let time_json = map.get("timestamp").unwrap();
        if let JsonValue::Object(time_map) = time_json {
            let seconds = time_map.get("seconds").unwrap();
            let nanos = time_map.get("nanos").unwrap();
            if let JsonValue::Num(seconds_int) = seconds {
                if let JsonValue::Num(nanos_int) = nanos {
                    let start = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
                    let duration = Duration::seconds(*seconds_int as i64)
                        + Duration::nanoseconds(*nanos_int as i64);
                    let time = start + duration;

                    if let JsonValue::Str(severity) = map.get("severity").unwrap() {
                        if let JsonValue::Str(message) = map.get("message").unwrap() {
                            return Ok(LogLine {
                                time: time,
                                message: message.to_string(),
                                severity: severity.to_string(),
                            });
                        }
                    }
                }
            }
        }
    }

    Err(())
}

impl Parser {
    fn new() -> Self {
        Parser {
            buffer: String::new(),
        }
    }

    fn add(&mut self, line: &str) -> ParserOutput {
        if self.buffer.len() == 0 && !line.trim().starts_with("{") {
            return ParserOutput::Text(line.to_string());
        }

        self.buffer.push_str(line);
        self.buffer.push_str("\n");

        let result = root::<(&str, ErrorKind)>(&self.buffer.trim());
        match result {
            Ok(x) => {
                let output = format!("{:#?}", x);
                let log_line = get_log_line(x.1).unwrap();
                self.buffer.clear();
                return ParserOutput::Log(log_line);
            }
            Err(x) => {
                let output = format!("{:#?}", x);
                ParserOutput::None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn respond_to_json_input() {
        let input = r#"{
            "timestamp": {
              "seconds": 1605277107,
              "nanos": 234000000
            },
            "severity": "INFO",
            "message": "Responding at http://0.0.0.0:8080",
            "context": {}
          }"#;
        let lines: Vec<String> = input
            .split('\n')
            .into_iter()
            .map(|it| it.to_owned())
            .collect();
        let mut parser = Parser::new();
        for i in 0..lines.len() - 1 {
            let response = parser.add(&lines[i]);
            assert_eq!(response, ParserOutput::None);
        }

        let expected = "2020-11-13 15:18:27.234 INFO Responding at http://0.0.0.0:8000";
        let last = parser.add(&lines[lines.len() - 1]);
        // assert_eq!(ParserOutput::Line(expected.to_string()), last);
    }

    #[test]
    fn respond_to_text_input() {
        let mut parser = Parser::new();
        assert_eq!(
            ParserOutput::Line("Hello world".to_string()),
            parser.add("Hello world")
        );
    }

    #[test]
    fn test_nom() {
        use nom::{bytes::complete::tag, combinator::opt, IResult};
        fn abcd_opt(i: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
            opt(tag("abcd"))(i)
        }

        assert_eq!(
            abcd_opt(&b"abcdxxx"[..]),
            Ok((&b"xxx"[..], Some(&b"abcd"[..])))
        );
        assert_eq!(abcd_opt(&b"efghxxx"[..]), Ok((&b"efghxxx"[..], None)));
    }
}
