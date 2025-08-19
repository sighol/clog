#![deny(rust_2021_compatibility)]
mod parser;

use std::collections::HashMap;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Write;
use std::mem::take;
use std::str::FromStr;

use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Local;
use chrono::Utc;
use color_eyre::Result;
use colored::{Color, Colorize};
use eyre::bail;
use eyre::eyre;
use eyre::Context;

use parser::{root, JsonValue};

use clap::Parser as ClapParser;
use clap::ValueEnum as ClapValueEnum;

#[derive(Debug)]
struct LogLine {
    pub time: DateTime<Utc>,
    pub severity: String,
    pub parsed_map: HashMap<String, JsonValue>,
}

struct PrintConfig {
    pub extra: Vec<String>,
    pub verbose: bool,
    pub is_local_timezone: bool,
    pub oneline_maxlength: Option<usize>,
}

impl PrintConfig {
    fn tz(&self) -> FixedOffset {
        if self.is_local_timezone {
            Local::now().offset().fix()
        } else {
            Utc.fix()
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
enum Severity {
    Tracing,
    Debug,
    Info,
    Warning,
    Error,
    Fatal,
}

impl FromStr for Severity {
    type Err = eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        return match s.to_lowercase().as_ref() {
            "tracing" | "trace" => Ok(Self::Tracing),
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" | "warning" => Ok(Self::Warning),
            "error" => Ok(Self::Error),
            "fatal" => Ok(Self::Fatal),
            _ => Err(eyre!("Unknown severity: {}", s)),
        };
    }
}

fn is_too_dark(r: u8, g: u8, b: u8) -> bool {
    let luma = 0.2126 * (r as f64) + 0.7152 * (g as f64) + 0.0722 * (b as f64); // per ITU-R BT.709
    luma < 80.0
}

fn random_color(input: &str) -> String {
    let mut hasher = DefaultHasher::new();
    input.hash(&mut hasher);
    let hash_value = hasher.finish();
    let red = (hash_value & 0xFF) as u8;
    let green = ((hash_value >> 8) & 0xFF) as u8;
    let blue = ((hash_value >> 16) & 0xFF) as u8;
    let bold = if ((hash_value >> 24) & 0x1) == 0 {
        "1"
    } else {
        ""
    };
    let foreground = if is_too_dark(red, green, blue) {
        "48;2;255;255;255"
    } else {
        "40"
    };

    format!("\x1b[{bold};{foreground};38;2;{red};{green};{blue}m")
}

impl LogLine {
    fn print<W>(&self, f: &mut W, config: &PrintConfig) -> std::io::Result<()>
    where
        W: Write,
    {
        let time_in_timezone = self.time.with_timezone(&config.tz());
        let time_in_timezone = time_in_timezone.format("%Y-%m-%d %H:%M:%S%.3f");
        write!(f, "{}", time_in_timezone.to_string().green())?;
        if !config.is_local_timezone {
            write!(f, "{}", "Z".green())?;
        }
        // process id or request_id
        if let Some(process_id) = self.value(&self.parsed_map, "context.processId") {
            let max_len = std::cmp::min(process_id.len(), 6);
            let process_id = process_id[..max_len].to_string();
            write!(f, " [p={:6}]", process_id.bold())?;
        } else if let Some(request_id) = self.value(&self.parsed_map, "context.requestId") {
            let max_len = std::cmp::min(request_id.len(), 8);
            let request_id = request_id[..max_len].to_string();
            let color = random_color(&request_id);

            write!(f, " [{color}{:<8}\x1b[0m]", request_id)?;
        }

        let extra_colors = vec![Color::BrightBlack, Color::BrightCyan, Color::BrightMagenta];
        for (i, e) in config.extra.iter().enumerate() {
            let color = extra_colors[i % extra_colors.len()];
            if let Some(app) = self.value(&self.parsed_map, e) {
                write!(f, " [{}]", app.color(color))?;
            } else {
                write!(f, " []")?;
            }
        }

        let (severity_style, message_style) = match self.severity() {
            Severity::Tracing => (Color::BrightBlack, Color::BrightBlack),
            Severity::Debug => (Color::BrightBlack, Color::BrightBlack),
            Severity::Info => (Color::BrightBlack, Color::White),
            Severity::Warning => (Color::Yellow, Color::Yellow),
            Severity::Error => (Color::Red, Color::Red),
            Severity::Fatal => (Color::Magenta, Color::Magenta),
        };

        write!(
            f,
            " {:7}",
            self.severity.to_uppercase().color(severity_style).bold()
        )?;
        let (message_path, message) = self
            .get_message(config)
            .map(|x| (Some(x.0), x.1))
            .unwrap_or((None, String::new()));

        for line in message.lines() {
            writeln!(f, " {}", line.color(message_style))?;
        }
        if config.verbose {
            write_logline_map(f, &self.parsed_map, &String::from("  "), message_path)?;
        }
        Ok(())
    }

    fn get_message(&self, config: &PrintConfig) -> Option<(&'static str, String)> {
        let search_places = ["message", "msg", "event", "MessageTemplate"];
        for sp in search_places {
            if let Some(value) = self.parsed_map.get(sp) {
                if let Ok(str_value) = value.str_value() {
                    // Add info from exc_info
                    let exception_message =
                        if let Some(exception_message) = self.parsed_map.get("exc_info") {
                            if let Ok(exception_message) = exception_message.str_value() {
                                Some(exception_message)
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                    let message = match exception_message {
                        Some(exc) => format!("{}\n{}", str_value, exc),
                        None => str_value,
                    };

                    let message = message.trim();
                    let message = if let Some(max_length) = config.oneline_maxlength {
                        let message = message.replace("\n", " \u{2936} ");
                        if message.len() > max_length {
                            let mut end: usize = 0;
                            message
                                .chars()
                                .into_iter()
                                .take(max_length - 3)
                                .for_each(|x| end += x.len_utf8());
                            format!("{}...", &message[..end])
                        } else {
                            message
                        }
                    } else {
                        message.replace("\n", "\n    ")
                    };

                    return Some((sp, message));
                }
            }
        }
        return None;
    }

    fn value(&self, map: &HashMap<String, JsonValue>, key: &str) -> Option<String> {
        let parts: Vec<_> = key.split(".").collect();
        let parts_len = parts.len();
        let mut map = map;
        for (i, part) in parts.into_iter().enumerate() {
            let is_last = i == parts_len - 1;
            let part_value = map.get(part);
            if is_last {
                return match part_value {
                    Some(JsonValue::Object(m)) => Some(format!("{:?}", m)),
                    Some(JsonValue::Num(n)) => Some(format!("{}", n)),
                    Some(JsonValue::Str(s)) => Some(format!("{}", s)),
                    Some(JsonValue::Bool(b)) => Some(format!("{}", b)),
                    Some(JsonValue::Array(value)) => Some(format!("{:?}", value)),
                    Some(JsonValue::Null) => None,
                    None => None,
                };
            } else if let Some(JsonValue::Object(m)) = part_value {
                map = m
            } else {
                return None;
            }
        }
        panic!("Unreachable")
    }

    fn severity(&self) -> Severity {
        let severity = self.severity.to_lowercase();
        return if severity.contains("warn") {
            Severity::Warning
        } else if severity.contains("error") || severity.contains("critical") {
            Severity::Error
        } else if severity.contains("debug") {
            Severity::Debug
        } else if severity.contains("trace") || severity.contains("tracing") {
            Severity::Tracing
        } else if severity.contains("fatal") {
            Severity::Fatal
        } else {
            Severity::Info
        };
    }
}

fn write_logline_map<W>(
    f: &mut W,
    map: &HashMap<String, JsonValue>,
    indent: &str,
    message_path: Option<&str>,
) -> std::io::Result<()>
where
    W: Write,
{
    let mut sorted_keys: Vec<_> = map.keys().clone().into_iter().collect();
    sorted_keys.sort();
    for key in sorted_keys.into_iter() {
        if key == "timestamp" || key == "@timestamp" || key == "severity" || key == "level" {
            continue;
        }
        let value = match &map[key] {
            JsonValue::Null => None,
            JsonValue::Num(n) => Some(format!("{}", n)),
            JsonValue::Str(s) => {
                if s.contains("\n") {
                    let line_prefix = format!("\n{indent}  ");
                    Some(format!("{line_prefix}{}", s.replace("\n", &line_prefix)))
                } else {
                    Some(format!("{s}"))
                }
            }
            JsonValue::Bool(b) => Some(format!("{}", b)),
            JsonValue::Array(value) => Some(format!("{:?}", value)),
            JsonValue::Object(map) => {
                if map.len() != 0 {
                    writeln!(f, "{}{}:", indent, key.bright_black())?;
                    write_logline_map(f, map, &format!("  {}", indent), message_path)?;
                }
                None
            }
        };
        if let Some(value) = value {
            if Some(key.as_str()) != message_path {
                writeln!(f, "{}{} = {}", indent, key.bright_black(), value)?;
            }
        }
    }
    Ok(())
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
    let parsed = match parsed {
        JsonValue::Object(_) => parsed,
        _ => bail!("parsed is not a JsonValue::Object"),
    };
    let time_json = parsed
        .map_value("timestamp")
        .or_else(|_| parsed.map_value("time"))
        .or_else(|_| parsed.map_value("eventTime"))
        .or_else(|_| parsed.map_value("@timestamp"))
        .or_else(|_| parsed.map_value("Timestamp"))
        .or_else(|_| parsed.map_value("ts"))?;

    let time: DateTime<Utc> = if let Ok(time_str) = time_json.str_value() {
        DateTime::parse_from_str(&time_str, "%+")
            .map(|dt| dt.with_timezone(&Utc))
            .context(format!("Failed to parse datetime: `{}`", &time_str))?
    } else {
        let seconds_value = time_json.map_value("seconds")?.int_value()?;
        let nanos_value = time_json.map_value("nanos")?.int_value()?;
        let start = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        let duration = Duration::try_seconds(seconds_value as i64).unwrap()
            + Duration::nanoseconds(nanos_value as i64);
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
        .or_else(|_| parsed.map_value("level").and_then(|x| x.str_value()))
        .or_else(|_| parsed.map_value("log.level").and_then(|x| x.str_value()))
        .unwrap_or_else(|_| "unknown".to_string());

    let map = match parsed {
        JsonValue::Object(map) => map,
        _ => bail!("Parsed is not a map"),
    };

    Ok(LogLine {
        time,
        severity,
        parsed_map: map,
    })
}

#[derive(Debug)]
enum ParserOutput {
    None,
    Text(String),
    Log(LogLine),
}

impl ParserOutput {
    fn print<W>(&self, f: &mut W, config: &PrintConfig) -> std::io::Result<()>
    where
        W: Write,
    {
        match &self {
            ParserOutput::Log(l) => l.print(f, config),
            ParserOutput::Text(s) => write!(f, "{}", s),
            ParserOutput::None => Ok(()),
        }
    }
}

#[derive(Default, Debug)]
struct Parser {
    buffer: String,
    pub debug: bool,
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

    fn push(&mut self, line: &str) -> Vec<ParserOutput> {
        use nom::Err::{Error, Failure, Incomplete};

        self.buffer.push_str(line);

        let result = root(&self.buffer);
        match result {
            Ok((rest, value)) => {
                let output = match get_log_line(value) {
                    Ok(x) => ParserOutput::Log(x),
                    Err(e) => {
                        if self.debug {
                            eprintln!("Failed get_log_line: {:?}", e.to_string().red())
                        }
                        ParserOutput::Text(self.buffer.clone())
                    }
                };
                let rest = rest.trim_start_matches('\n').to_string();
                self.buffer.clear();
                let mut output = vec![output];
                for next_output in self.push(&rest) {
                    match next_output {
                        ParserOutput::None => (),
                        _ => output.push(next_output),
                    }
                }
                output
            }
            Err(Incomplete(_)) => vec![],
            Err(Failure(_)) | Err(Error(_)) => {
                if self.debug {
                    eprintln!("Parsing failure: {:?}", format!("{:?}", result).red());
                }
                let output = ParserOutput::Text(self.buffer.clone());
                self.buffer.clear();
                vec![output]
            }
        }
    }
}

#[derive(ClapParser)]
#[command(version, author)]
struct Cli {
    #[arg(value_enum, long="color", default_value_t=ColorChoice::Always)]
    color: ColorChoice,

    #[arg(short, long, help = "Extra values to print. Eg. X-CDP-SDK")]
    extra: Vec<String>,

    #[arg(
        long,
        help = "Turn on debug mode. All lines that can't be parsed will be output to stderr"
    )]
    debug: bool,

    #[arg(short, long, help = "Show all additional info in a map")]
    verbose: bool,

    #[arg(
        short = 's',
        long,
        help = "Ignore messages with less important severity"
    )]
    min_severity: Option<Severity>,

    #[arg(long, help = "Output timestamps in UTC")]
    utc: bool,

    #[arg(long = "oneline")]
    oneline: bool,
}

#[derive(ClapValueEnum, Clone, Debug)]
enum ColorChoice {
    Auto,
    Never,
    Always,
}

fn main() -> eyre::Result<()> {
    use std::io::{self, prelude::*};

    let args: Cli = Cli::parse();
    match args.color {
        ColorChoice::Always => colored::control::set_override(true),
        ColorChoice::Never => colored::control::set_override(false),
        _ => {}
    };

    let print_config = PrintConfig {
        extra: args.extra,
        is_local_timezone: !args.utc,
        verbose: args.verbose,
        oneline_maxlength: if args.oneline { Some(100) } else { None },
    };

    let mut parser = Parser::new();
    parser.debug = args.debug;

    let mut stdout = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let mut unwrapped = line.unwrap().to_string();
        unwrapped.push('\n');
        let outputs = parser.push(&unwrapped);
        for output in outputs {
            match &args.min_severity {
                Some(minimum) => {
                    let is_included = match &output {
                        ParserOutput::None => false,
                        ParserOutput::Text(_) => true,
                        ParserOutput::Log(m) => m.severity() >= *minimum,
                    };
                    if !is_included {
                        continue;
                    }
                }
                None => {}
            }
            output.print(&mut stdout, &print_config)?;
            stdout.flush()?;
        }
    }
    parser.flush().print(&mut stdout, &print_config)?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    impl ParserOutput {
        fn to_string(&self) -> String {
            let config = PrintConfig {
                extra: vec![],
                is_local_timezone: false,
                verbose: false,
                oneline_maxlength: None,
            };
            let mut s = Vec::<u8>::new();
            self.print(&mut s, &config).expect("Fail to write");
            String::from_utf8(s).expect("Couldn't convert to string")
        }
    }

    fn before() {
        colored::control::set_override(false);
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
            let response = parser.push(&lines[i]);
            assert_eq!(0, response.len());
        }

        // Add list line, which will complete the log message.
        let output = parser.push(&lines[lines.len() - 1]);
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
            let response = parser.push(&lines[i]);
            assert_eq!(0, response.len());
        }
        let output = parser.push(&lines[lines.len() - 1])[0].to_string();
        assert_eq!(
            output,
            "2022-04-01 18:49:54.068Z [p=776f2d] INFO    [1167/9733 11% ETA=2022-04-01 20:50:06.868555] Ingesting 389 wells. 693.10 items/sec.\n"
        );
    }

    #[test]
    fn event_time_input_and_extra() {
        let input = r#"{
            "eventTime": "2023-03-15T14:36:07.937134597+00:00",
            "message": "something something",
            "reportLocation": {
              "filePath": "libraries/observability/src/logging/mod.rs",
              "lineNumber": 70,
              "modulePath": "observability::logging"
            },
            "serviceContext": {
              "service": "robotics_services",
              "version": "0.2.1"
            },
            "severity": "ERROR"
          }"#;

        let mut parser = Parser::new();
        let output = parser.push(input);
        let mut buffer = Vec::new();
        output[0]
            .print(
                &mut buffer,
                &PrintConfig {
                    extra: vec!["reportLocation.modulePath".to_string()],
                    verbose: false,
                    is_local_timezone: false,
                    oneline_maxlength: None,
                },
            )
            .unwrap();
        let buffer_str = String::from_utf8(buffer).unwrap();
        assert_eq!(
            &buffer_str,
            "2023-03-15 14:36:07.937Z [observability::logging] ERROR   something something\n"
        );
    }

    #[test]
    fn event_time_and_at_timestamp() {
        let input = r#"{
            "event": "Refreshing project usage data",
            "@timestamp": "2023-09-14T12:39:35.604694Z",
            "level": "debug",
            "db": {
              "connection_wait_time_ms": 0,
              "sql_statement_hash": "abc123"
            },
            "project": null,
            "callsite": {
              "pathname": "/app/okkeeper.py",
              "filename": "bookkeeper.py",
              "module": "bookkeeper",
              "func_name": "_keep_refreshing_usage_data",
              "lineno": 116,
              "my_bool": true,
              "my_list": [1, 2, 3],
              "thread": 140450880908160,
              "thread_name": "MainThread"
            }
          }
          "#;

        let mut parser = Parser::new();
        let output = parser.push(input);
        let mut buffer = Vec::new();
        output[0]
            .print(
                &mut buffer,
                &PrintConfig {
                    extra: vec!["callsite.module".to_string()],
                    verbose: false,
                    is_local_timezone: false,
                    oneline_maxlength: None,
                },
            )
            .unwrap();
        let buffer_str = String::from_utf8(buffer).unwrap();
        assert_eq!(
            &buffer_str,
            "2023-09-14 12:39:35.604Z [bookkeeper] DEBUG   Refreshing project usage data\n"
        );
    }

    #[test]
    fn with_verbose_printing() {
        let input = r#"{
            "event": "Refreshing project usage data",
            "@timestamp": "2023-09-14T12:39:35.604694Z",
            "level": "debug",
            "db": {
              "connection_wait_time_ms": 0,
              "sql_statement_hash": "abc123"
            },
            "project": null,
            "context": {},
            "callsite": {
              "pathname": "/app/okkeeper.py",
              "filename": "bookkeeper.py",
              "module": "bookkeeper",
              "func_name": "_keep_refreshing_usage_data",
              "lineno": 116,
              "my_bool": true,
              "my_list": [1, 2, 3],
              "thread": 140450880908160,
              "thread_name": "MainThread"
            }
          }"#;

        let mut parser = Parser::new();
        let output = parser.push(input);
        let mut buffer = Vec::new();
        for o in output {
            o.print(
                &mut buffer,
                &PrintConfig {
                    extra: vec!["callsite.module".to_string()],
                    verbose: true,
                    is_local_timezone: false,
                    oneline_maxlength: None,
                },
            )
            .unwrap();
        }
        let expected = r#"2023-09-14 12:39:35.604Z [bookkeeper] DEBUG   Refreshing project usage data
  callsite:
    filename = bookkeeper.py
    func_name = _keep_refreshing_usage_data
    lineno = 116
    module = bookkeeper
    my_bool = true
    my_list = [Num(1.0), Num(2.0), Num(3.0)]
    pathname = /app/okkeeper.py
    thread = 140450880908160
    thread_name = MainThread
  db:
    connection_wait_time_ms = 0
    sql_statement_hash = abc123
"#;
        let buffer_str = String::from_utf8(buffer).unwrap();
        assert_eq!(&buffer_str, expected,);
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
        let output = parser.push(input);
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
            parser.push("Hello world")[0].to_string()
        );
    }
}
