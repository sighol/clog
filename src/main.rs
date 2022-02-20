#![deny(rust_2021_compatibility)]
mod parser;

use std::collections::HashMap;
use std::fmt::Display;
use std::mem::take;
use std::str;

use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Local;
use chrono::Utc;
use color_eyre::Result;
use nom::error::ErrorKind;

use parser::{root, JsonValue};

#[derive(Debug, Clone)]
struct LogLine {
    pub time: DateTime<Utc>,
    pub severity: String,
    pub message: String,
    pub context: HashMap<String, String>,
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tz = Local::now().timezone();
        let local_time = self.time.with_timezone(&tz);

        write!(f, "{} [{}] {}", local_time, self.severity, self.message,)
    }
}

fn bunyan_to_level(level: i32) -> &'static str {
    match level {
        50 => "error",
        40 => "warn",
        30 => "info",
        20 => "debug",
        10 => "tracing",
        _ => "unknown",
    }
}

fn get_log_line(parsed: JsonValue) -> Result<LogLine> {
    let time_json = parsed
        .map_value("timestamp")
        .or_else(|_| parsed.map_value("time"))?;
    println!("{:?}", time_json);

    let time: DateTime<Utc> = if let Ok(time_str) = time_json.str_value() {
        Utc.datetime_from_str(&time_str, "%+")?
    } else {
        let seconds_value = time_json.map_value("seconds")?.int_value()?;
        let nanos_value = time_json.map_value("nanos")?.int_value()?;
        let start = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
        let duration =
            Duration::seconds(seconds_value as i64) + Duration::nanoseconds(nanos_value as i64);
        start + duration
    };

    let severity = parsed
        .map_value("severity")
        .and_then(|x| x.str_value())
        .or_else(|_| {
            parsed
                .map_value("level")
                .and_then(|level| level.int_value())
                .and_then(|level| Ok(bunyan_to_level(level as i32).to_string()))
        })
        .unwrap_or_else(|_| "unknown".to_string());

    let message = parsed
        .map_value("message")
        .and_then(|x| x.str_value())
        .or_else(|_| parsed.map_value("msg").and_then(|x| x.str_value()))?;
    let message = if let Ok(exception_message) = parsed.map_value("exc_info") {
        format!("{}\n{}", message, exception_message.str_value()?)
    } else {
        message
    };
    let context_value = parsed.map_value("context");
    let mut context = HashMap::<String, String>::new();
    // XXX (robertc) this throws away an Err() when the key is not an object,
    // but the severity and message cases above do not similarly fail
    // gracefully.
    if let Ok(JsonValue::Object(context_json_map)) = context_value {
        for (key, json_value) in context_json_map {
            if let JsonValue::Str(value) = json_value {
                context.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(LogLine {
        time,
        message,
        severity,
        context,
    })
}

#[derive(Debug, Clone)]
enum ParserOutput {
    None,
    Text(String),
    Log(LogLine),
}

impl Display for ParserOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            ParserOutput::Log(l) => write!(f, "{}", l),
            ParserOutput::None => Ok(()),
            ParserOutput::Text(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Default, Debug)]
struct Parser {
    buffer: String,
}

impl Parser {
    fn new() -> Self {
        Default::default()
    }

    fn flush(&mut self) -> ParserOutput {
        if self.buffer.is_empty() {
            ParserOutput::None
        } else {
            ParserOutput::Text(take(&mut self.buffer))
        }
    }

    fn add(&mut self, line: &str) -> Vec<ParserOutput> {
        use nom::Err::{Error, Failure, Incomplete};

        self.buffer.push_str(line);

        let result = root::<(&str, ErrorKind)>(&self.buffer);
        match result {
            Ok((rest, value)) => {
                let output = match get_log_line(value) {
                    Ok(x) => ParserOutput::Log(x),
                    Err(_) => ParserOutput::Text(self.buffer.clone()),
                };
                let rest = rest.trim_start_matches('\n').to_string();
                self.buffer.clear();
                let mut output = vec![output];
                for next_output in self.add(&rest) {
                    match next_output {
                        ParserOutput::None => (),
                        _ => output.push(next_output),
                    }
                }
                output
            }
            Err(Incomplete(_)) => vec![],
            Err(Failure(_)) | Err(Error(_)) => {
                let output = ParserOutput::Text(self.buffer.clone());
                self.buffer.clear();
                vec![output]
            }
        }
    }
}

fn main() {
    use std::io::{self, prelude::*};
    let mut t = term::stdout().expect("Could not unwrap stdout()");

    let mut parser = Parser::new();
    for line in io::stdin().lock().lines() {
        let mut unwrapped = line.unwrap().to_string();
        unwrapped.push('\n');
        let answers = parser.add(&unwrapped);
        for answer in answers {
            print(&answer, &mut t);
        }
    }
    print(&parser.flush(), &mut t);
}

type Terminal = Box<dyn term::Terminal<Output = std::io::Stdout> + std::marker::Send>;

fn print(answer: &ParserOutput, t: &mut Terminal) {
    match answer {
        ParserOutput::None => (),
        ParserOutput::Text(s) => print!("{}", s),
        ParserOutput::Log(l) => {
            let tz = Local::now().timezone();
            let local_time = l.time.with_timezone(&tz);
            t.fg(term::color::GREEN).unwrap();
            print!("{}", local_time);
            if let Some(request_id) = l.context.get("requestId") {
                t.reset().unwrap();
                // t.fg(term::color::BRIGHT_BLACK).unwrap();
                let request_id = request_id[..8].to_string();
                t.attr(term::Attr::Bold).unwrap();
                print!(" [{}]", request_id);
                t.reset().unwrap();
            }
            t.reset().unwrap();
            t.fg(term::color::BRIGHT_BLACK).unwrap();
            t.attr(term::Attr::Bold).unwrap();
            print!(" {:7}", l.severity.to_uppercase());
            t.reset().unwrap();
            let severity = l.severity.to_lowercase();
            if severity.contains("warn") {
                t.fg(term::color::YELLOW).unwrap();
            }
            if severity.contains("error") {
                t.fg(term::color::RED).unwrap();
            }
            if severity.contains("info") {
                // t.fg(term::color::GREEN).unwrap();
            }
            if severity.contains("debug") {
                t.fg(term::color::BRIGHT_BLACK).unwrap();
            }
            if severity.contains("fatal") {
                t.fg(term::color::MAGENTA).unwrap();
            }

            print!(" {}", l.message);
            t.reset().unwrap();
            println!();
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
            assert_eq!(response[0].to_string(), "".to_string());
        }

        let expected = "2020-11-13 14:18:27.234 UTC [INFO] Responding at http://0.0.0.0:8080";
        let last = parser.add(&lines[lines.len() - 1]);
        assert_eq!(expected.to_string(), last[0].to_string());
        // assert_eq!(ParserOutput::Line(expected.to_string()), last);
    }

    #[test]
    fn respond_to_text_input() {
        let mut parser = Parser::new();
        assert_eq!(
            ParserOutput::Text("Hello world\n".to_string()).to_string(),
            parser.add("Hello world")[0].to_string()
        );
    }
}
