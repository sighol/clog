use chrono::prelude::*;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use nom::error::ErrorKind;
use std::error::Error;
use std::str;

mod parser;
use parser::{root, JsonValue};

#[derive(Debug, Clone)]
struct LogLine {
    pub time: DateTime<Utc>,
    pub severity: String,
    pub message: String,
}

impl std::fmt::Display for LogLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} [{}] {}", self.time, self.severity, self.message,)
    }
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

#[derive(Debug, Clone)]
enum ParserOutput {
    None,
    Text(String),
    Log(LogLine),
}

impl ParserOutput {
    #[cfg(test)]
    fn to_string(&self) -> String {
        match self {
            ParserOutput::None => "".to_string(),
            ParserOutput::Text(s) => s.clone(),
            ParserOutput::Log(l) => format!("{}", l),
        }
    }
}

struct Parser {
    buffer: String,
}

impl Parser {
    fn new() -> Self {
        Parser {
            buffer: String::new(),
        }
    }

    fn flush(&mut self) -> ParserOutput {
        if self.buffer.len() == 0 {
            ParserOutput::None
        } else {
            let output = ParserOutput::Text(self.buffer.to_string());
            self.buffer.clear();
            output
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
        unwrapped.push_str("\n");
        let answers = parser.add(&unwrapped);
        for answer in answers {
            print(&answer, &mut t);
        }
    }
    print(&parser.flush(), &mut t);
}

fn print(
    answer: &ParserOutput,
    t: &mut Box<dyn term::Terminal<Output = std::io::Stdout> + std::marker::Send>,
) {
    match answer {
        ParserOutput::None => (),
        ParserOutput::Text(s) => print!("{}", s),
        ParserOutput::Log(l) => {
            let sev = l.severity.to_lowercase();
            if sev.contains("warn") {
                t.fg(term::color::YELLOW).unwrap()
            }
            if sev.contains("error") {
                t.fg(term::color::RED).unwrap()
            }
            if sev.contains("info") {
                t.fg(term::color::GREEN).unwrap()
            }
            if sev.contains("debug") {
                t.fg(term::color::BRIGHT_BLACK).unwrap()
            }
            if sev.contains("fatal") {
                t.fg(term::color::MAGENTA).unwrap()
            }
            print!("{}", l);
            t.reset().unwrap();
            println!("");
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
