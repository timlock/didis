use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

pub trait Value {
    fn parse_from_string(&mut self, s: &str) -> Result<(), String>;
    fn try_activate(&mut self) -> Result<(), String>;
}

impl<T> Value for T
where
    T: FromStr + Display,
    <T as FromStr>::Err: Error,
{
    fn parse_from_string(&mut self, arg: &str) -> Result<(), String> {
        match T::from_str(arg) {
            Ok(s) => {
                *self = s;
                Ok(())
            }
            Err(err) => Err(format!("{:?}", err)),
        }
    }

    fn try_activate(&mut self) -> Result<(), String> {
        let t = self.to_string();
        match t.as_str() {
            "true" | "false" => self.parse_from_string("true"),
            _ => Err(String::from("bound value should be of type bool")),
        }
    }
}

struct Flag<'a> {
    name: &'static str,
    usage: &'static str,
    value: &'a mut dyn Value,
}

impl<'a> Flag<'a> {
    fn new(value: &'a mut dyn Value, name: &'static str, usage: &'static str) -> Self {
        Self { value, name, usage }
    }
}

fn parse_name(value: &str) -> Option<&str> {
    match value.starts_with("--") {
        true => Some(value.strip_prefix("--").unwrap()),
        false => match value.starts_with('-') {
            true => Some(value.strip_prefix('-').unwrap()),
            false => None,
        },
    }
}

#[derive(Debug)]
pub enum FlagError {
    UnknownFlag(String),
    ParseError((String, String)),
    NoValue(String),
}

impl Display for FlagError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FlagError::UnknownFlag(name) => {
                write!(f, "unknown flag: {name}")
            }
            FlagError::ParseError((name, err)) => {
                write!(f, "could not parse flag {name} err: {err}")
            }
            FlagError::NoValue(name) => {
                write!(f, "expected value for flag {name}")
            }
        }
    }
}

#[derive(Default)]
pub struct FlagSet<'a> {
    inner: HashMap<&'static str, Flag<'a>>,
    flags: HashMap<String, Flag<'a>>,
    args: Vec<String>,
}

impl<'a> FlagSet<'a> {
    pub fn bind(&mut self, value: &'a mut dyn Value, name: &'static str, usage: &'static str) {
        let flag = Flag::new(value, name, usage);
        if self.flags.insert(name.to_ascii_lowercase(), flag).is_some() {
            panic!("should not register flag name {name} twice")
        }
    }

    pub fn parse(&mut self, args: impl IntoIterator<Item = String>) -> Result<(), FlagError> {
        let mut iter = args.into_iter();

        loop {
            let name = match iter.next() {
                Some(f) => f,
                None => break,
            };

            if !name.starts_with('-') {
                self.args.push(name);
                self.args.extend(iter);
                break;
            }

            if name == "--" {
                self.args.extend(iter);
                break;
            }

            let stripped = match name.strip_prefix("-") {
                Some(single) => single.strip_prefix("-").unwrap_or_else(|| single),
                None => panic!("At this point the name should start with a hyphen"),
            };

            if let Some(equals_pos) = stripped.find("=") {
                let (flag_name, flag_value) = stripped.split_at(equals_pos);

                let flag = self
                    .flags
                    .get_mut(flag_name)
                    .ok_or(FlagError::UnknownFlag(stripped.to_owned()))?;

                flag.value
                    .parse_from_string(flag_value)
                    .map_err(|err| FlagError::ParseError((stripped.to_owned(), err)))?;
            } else {
                let flag = self
                    .flags
                    .get_mut(stripped)
                    .ok_or(FlagError::UnknownFlag(stripped.to_owned()))?;

                if let Err(_) = flag.value.try_activate() {
                    let value = match iter.next() {
                        Some(v) => v,
                        None => return Err(FlagError::NoValue(stripped.to_owned())),
                    };

                    flag.value
                        .parse_from_string(value.as_str())
                        .map_err(|err| FlagError::ParseError((stripped.to_owned(), err)))?;
                }
            }
        }
        Ok(())
    }

    pub fn print_usage(&self) {
        for (name, flag) in &self.inner {
            println!("{}\n\t{}", name, flag.usage)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string() {
        let args = vec!["-b", "string"];
        let expected_flag = ("b", String::from("string"));
        let expects_err = false;

        let mut flag_set = FlagSet::default();

        let mut value = String::new();
        flag_set.bind(&mut value, "b", "");

        let result = flag_set.parse(args.iter().map(|a| a.to_string()));
        assert_eq!(expects_err, result.is_err());

        assert_eq!(expected_flag.1, value);
    }

    #[test]
    fn test_parse_i32() {
        let args = vec!["-i", "1"];
        let expected_flag = ("i", 1);
        let expects_err = false;

        let mut flag_set = FlagSet::default();

        let mut value = 0;
        flag_set.bind(&mut value, "i", "");
        let result = flag_set.parse(args.iter().map(|a| a.to_string()));
        flag_set.flags.remove("i");
        assert_eq!(expects_err, result.is_err());

        assert_eq!(expected_flag.1, value);
    }

    #[test]
    fn test_parse_bool() {
        let args = vec!["-b"];
        let expected_flag = ("b", true);
        let expects_err = false;

        let mut flag_set = FlagSet::default();

        let mut value = false;
        flag_set.bind(&mut value, "b", "");

        let result = flag_set.parse(args.iter().map(|a| a.to_string()));
        assert_eq!(expects_err, result.is_err());

        assert_eq!(expected_flag.1, value);
    }

    #[test]
    #[ignore]
    fn test_parse_multiple_bools() {
        let args = vec!["-ba"];
        let expected_flag1 = ("a", true);
        let expected_flag2 = ("b", true);

        let mut flag_set = FlagSet::default();

        let mut value1 = false;
        flag_set.bind(&mut value1, "a", "");
        let mut value2 = false;
        flag_set.bind(&mut value2, "b", "");

        let result = flag_set.parse(args.iter().map(|a| a.to_string()));

        assert!(result.is_ok());

        assert_eq!(expected_flag1.1, value1);
        assert_eq!(expected_flag2.1, value2);
    }

    #[test]
    fn test_parse_remaining() {
        let args = vec!["--test", "text", "first", "second", "third"];
        let expected_flags = vec![("test", String::from("text"))];
        let remaining = vec!["first", "second", "third"];

        let mut flag_set = FlagSet::default();

        let mut value = String::new();
        flag_set.bind(&mut value, "test", "");

        let result = flag_set.parse(args.iter().map(|a| a.to_string()));
        let args = flag_set.args;

        assert_eq!("text", value.as_str());

        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(remaining.len(), args.len());
        for i in 0..args.len() {
            assert_eq!(remaining[i], args[i]);
        }
    }

    #[test]
    fn test_parse_remaining_with_bool_flag() {
        let args = vec!["--test", "first", "second", "third"];
        let expected_flags = vec![("test", true)];
        let remaining = vec!["first", "second", "third"];

        let mut flag_set = FlagSet::default();

        let mut value = false;
        flag_set.bind(&mut value, "test", "");

        let result = flag_set.parse(args.iter().map(|a| a.to_string()));
        let args = flag_set.args;

        assert!(value);

        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(remaining.len(), args.len());
        for i in 0..args.len() {
            assert_eq!(remaining[i], args[i]);
        }
    }
}
