use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use nom::{
    branch::alt,
    bytes::complete::{escaped, tag, take_while},
    character::complete::{char, none_of, one_of},
    combinator::{cut, map, opt, value},
    error::{context, ContextError, ErrorKind, ParseError},
    multi::separated_list0,
    number::complete::double,
    sequence::{delimited, preceded, separated_pair, terminated},
    IResult,
};
use std::collections::HashMap;
use std::error::Error;
use std::str;

#[derive(Debug)]
struct StringError {
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
    fn new(message: String) -> Self {
        StringError { message: message }
    }

    fn str(message: &str) -> Self {
        Self::new(message.to_string())
    }

    fn boxed(message: &str) -> Box<Self> {
        Box::new(Self::str(message))
    }
}

#[derive(Debug, PartialEq)]
pub enum JsonValue {
    Str(String),
    Boolean(bool),
    Num(f64),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

impl JsonValue {
    fn map_value(&self, key: &str) -> Result<&JsonValue, Box<dyn Error>> {
        let map = match self {
            JsonValue::Object(x) => x,
            _ => return Err(StringError::boxed("Something is wrong")),
        };

        match map.get(key) {
            Some(value) => Ok(value),
            None => Err(StringError::boxed("Key not found")),
        }
    }

    fn int_value(&self) -> Result<f64, Box<dyn Error>> {
        match self {
            JsonValue::Num(x) => Ok(*x as f64),
            _ => Err(StringError::boxed("Value is not a number")),
        }
    }

    fn str_value(&self) -> Result<String, Box<dyn Error>> {
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
    let mut t = term::stdout().expect("Could not unwrap stdout()");

    let mut parser = Parser::new();
    for line in io::stdin().lock().lines() {
        let unwrapped = line.unwrap();
        let answer = parser.add(&unwrapped);
        match answer {
            ParserOutput::None => (),
            ParserOutput::Text(s) => println!("{}", s),
            ParserOutput::Log(l) => {
                let sev = l.severity.to_lowercase();
                if sev.contains("warn") { t.fg(term::color::YELLOW).unwrap() }
                if sev.contains("error") { t.fg(term::color::RED).unwrap() }
                if sev.contains("info") { t.fg(term::color::GREEN).unwrap() }
                if sev.contains("debug") { t.fg(term::color::BRIGHT_BLACK).unwrap() }
                if sev.contains("fatal") { t.fg(term::color::MAGENTA).unwrap() }
                println!("{}", l);
                t.reset().unwrap();
            },
        }
    }
}

#[derive(Debug, Clone)]
struct LogLine {
    pub time: DateTime<Utc>,
    pub severity: String,
    pub message: String,
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} [{}] {}",
            self.time,
            self.severity,
            self.message.replace("\\n", "\n").replace("\\t", "\t")
        )
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

fn get_log_line(parsed: JsonValue) -> Result<LogLine, Box<dyn Error>> {
    let time_json = parsed.map_value("timestamp")?;
    let seconds_value = time_json.map_value("seconds")?.int_value()?;
    let nanos_value = time_json.map_value("nanos")?.int_value()?;
    let start = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
    let duration =
        Duration::seconds(seconds_value as i64) + Duration::nanoseconds(nanos_value as i64);
    let time = start + duration;
    let severity = parsed.map_value("severity")?.str_value()?;
    let message = parsed.map_value("message")?.str_value()?;
    return Ok(LogLine {
        time: time,
        message: message.to_string(),
        severity: severity.to_string(),
    });
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
                let output = match get_log_line(x.1) {
                    Ok(x) => ParserOutput::Log(x),
                    Err(_) => ParserOutput::Text(self.buffer.clone()),
                };
                self.buffer.clear();
                output
            }
            Err(_) => {
                if line == "}" {
                    // Most probably, we have failed to parse something prior. Release everything and try again.
                    let output = ParserOutput::Text(self.buffer.clone());
                    self.buffer.clear();
                    return output;
                }
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
            ParserOutput::Text("Hello world".to_string()),
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
