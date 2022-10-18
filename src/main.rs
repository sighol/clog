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
use colored::*;
use nom::error::ErrorKind;

use lazy_static::lazy_static;

use std::sync::atomic::{AtomicBool, Ordering};

use parser::{root, JsonValue};

use clap::Parser as ClapParser;
use clap::ValueEnum as ClapValueEnum;

lazy_static! {
    static ref USE_LOCAL_TIMEZONE: AtomicBool = AtomicBool::new(true);
}

#[derive(Debug, Clone)]
struct LogLine {
    pub time: DateTime<Utc>,
    pub severity: String,
    pub message: String,
    pub context: HashMap<String, String>,
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let is_local = USE_LOCAL_TIMEZONE.load(Ordering::Relaxed);
        let tz = if is_local {
            Local::now().offset().fix()
        } else {
            Utc.fix()
        };

        let time_in_timezone = self.time.with_timezone(&tz);
        let time_in_timezone = time_in_timezone.format("%Y-%m-%d %H:%M:%S%.3f");
        write!(f, "{}", time_in_timezone.to_string().green())?;
        if !is_local {
            write!(f, "{}", "Z".green())?;
        }
        if let Some(process_id) = self.context.get("processId") {
            let max_len = std::cmp::min(process_id.len(), 6);
            let process_id = process_id[..max_len].to_string();
            write!(f, " [p={:6}]", process_id.bold())?;
        } else if let Some(request_id) = self.context.get("requestId") {
            let max_len = std::cmp::min(request_id.len(), 8);
            let request_id = request_id[..max_len].to_string();
            write!(f, " [{:<8}]", request_id)?;
        }
        let severity = self.severity.to_lowercase();
        let (severity_color, message_color) = if severity.contains("warn") {
            ("yellow", "yellow")
        } else if severity.contains("error") {
            ("red", "red")
        } else if severity.contains("debug") {
            ("bright black", "bright black")
        } else if severity.contains("fatal") {
            ("magenta", "magenta")
        } else {
            ("bright black", "lbaft")
        };
        write!(
            f,
            " {:7}",
            self.severity.to_uppercase().bold().color(severity_color)
        )?;
        writeln!(f, " {}", self.message.color(message_color))
    }
}

fn bunyan_to_level(level: i32) -> &'static str {
    match level {
        50 => "ERROR",
        40 => "WARN",
        30 => "INFO",
        20 => "DEBUG",
        10 => "TRACING",
        _ => "UNKNOWN",
    }
}

fn get_log_line(parsed: JsonValue) -> Result<LogLine> {
    let time_json = parsed
        .map_value("timestamp")
        .or_else(|_| parsed.map_value("time"))?;

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

#[derive(ClapParser)]
struct Cli {
    #[arg(value_enum, long="color", default_value_t=ColorChoice::Auto)]
    color: ColorChoice,
}

#[derive(ClapValueEnum, Clone, Debug)]
enum ColorChoice {
    Auto,
    Never,
    Always,
}

fn main() {
    use std::io::{self, prelude::*};

    let args: Cli = Cli::parse();
    match args.color {
        ColorChoice::Always => {
            colored::control::set_override(true);
        }
        ColorChoice::Never => {
            colored::control::set_override(false);
        }
        _ => {}
    };

    let mut parser = Parser::new();
    for line in io::stdin().lock().lines() {
        let mut unwrapped = line.unwrap().to_string();
        unwrapped.push('\n');
        let answers = parser.add(&unwrapped);
        for answer in answers {
            print!("{}", &answer);
        }
    }
    print!("{}", &parser.flush());
}

#[cfg(test)]
mod test {
    use super::*;

    fn before() {
        colored::control::set_override(false);
        USE_LOCAL_TIMEZONE.store(false, Ordering::Relaxed);
    }

    #[test]
    fn respond_to_json_input() {
        before();
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
            .map(|it| it.to_owned() + "\n")
            .collect();
        let mut parser = Parser::new();

        // Add all but the last line. It is only after the list line that the
        // log statement is complete.
        for i in 0..lines.len() - 1 {
            let response = parser.add(&lines[i]);
            assert_eq!(0, response.len());
        }

        // Add list line, which will complete the log message.
        let output = parser.add(&lines[lines.len() - 1]);
        let expected = "2020-11-13 14:18:27.234Z INFO    Responding at http://0.0.0.0:8080\n";
        assert_eq!(expected.to_string(), output[0].to_string());
    }

    #[test]
    fn json_input_with_context() {
        before();
        let input = r#"{
            "message": "[1167/9733 11% ETA=2022-04-01 20:50:06.868555] Ingesting 389 wells. 693.10 items/sec.",
            "timestamp": "2022-04-01T18:49:54.068831Z",
            "severity": "INFO",
            "context": {
              "requestId": "test",
              "processId": "776f2d01-8bba-4c36-b6a8-5f7074c096d7"
            }
          }"#;
        let lines: Vec<String> = input
            .split('\n')
            .into_iter()
            .map(|it| it.to_owned() + "\n")
            .collect();
        let mut parser = Parser::new();

        // Add all but the last line. It is only after the list line that the
        // log statement is complete.
        for i in 0..lines.len() - 1 {
            let response = parser.add(&lines[i]);
            assert_eq!(0, response.len());
        }
        let output = parser.add(&lines[lines.len() - 1])[0].to_string();
        assert_eq!(
            output,
            "2022-04-01 18:49:54.068Z [p=776f2d] INFO    [1167/9733 11% ETA=2022-04-01 20:50:06.868555] Ingesting 389 wells. 693.10 items/sec.\n"
        );
    }

    #[test]
    fn buyan_input() {
        before();
        let input = r#"{
            "v": 0,
            "name": "tracing_demo",
            "msg": "Orphan event without a parent span",
            "level": 30,
            "hostname": "sighol-desktop",
            "pid": 293764,
            "time": "2022-02-20T18:05:16.272997204Z",
            "target": "docktail",
            "line": 97,
            "file": "src/main.rs"
          }"#;
        let mut parser = Parser::new();
        let output = parser.add(input);
        let output = output[0].to_string();

        assert_eq!(
            output,
            "2022-02-20 18:05:16.272Z INFO    Orphan event without a parent span\n"
        );
    }

    #[test]
    fn respond_to_text_input() {
        before();
        let mut parser = Parser::new();
        assert_eq!(
            ParserOutput::Text("Hello world".to_string()).to_string(),
            parser.add("Hello world")[0].to_string()
        );
    }
}
