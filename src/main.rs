use bytes::Bytes;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, io, sync::Arc};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    Array(Vec<Value>),
}

pub struct Parser {
    buf: Bytes,
    pos: usize,
}

fn extract_string(value: &Value) -> String {
    match value {
        Value::SimpleString(x) | Value::BulkString(x) => x.to_string(),
        _ => panic!("String expected"),
    }
}

impl Parser {
    pub fn new(buf: &[u8]) -> Parser {
        Parser {
            buf: Bytes::copy_from_slice(buf),
            pos: 0,
        }
    }

    pub fn parse_value(&mut self) -> Value {
        match self.buf[self.pos] {
            b'+' => {
                self.pos += 1;
                let mut data: Vec<u8> = Vec::<u8>::new();
                while self.buf[self.pos] != b'\r' {
                    data.push(self.buf[self.pos]);
                    self.pos += 1;
                }
                self.pos += 2;
                let s = String::from_utf8(data);
                return Value::SimpleString(s.expect("ffddf"));
            }
            b'$' => {
                self.pos += 4;
                let mut data: Vec<u8> = Vec::<u8>::new();
                while self.buf[self.pos] != b'\r' {
                    data.push(self.buf[self.pos]);
                    self.pos += 1;
                }
                self.pos += 2;
                let s = String::from_utf8(data);
                return Value::BulkString(s.expect("ffddf"));
            }
            b'*' => {
                self.pos += 1;
                let mut positive = true;
                match self.buf[self.pos] {
                    b'+' => {
                        self.pos += 1;
                    }
                    b'-' => {
                        self.pos += 1;
                        positive = false;
                    }
                    _ => {}
                }
                let mut number_data = Vec::<u8>::new();
                while self.buf[self.pos] != b'\r' {
                    number_data.push(self.buf[self.pos]);
                    self.pos += 1;
                }
                self.pos += 2;
                let items: i64 = String::from_utf8(number_data)
                    .expect("error")
                    .parse::<i64>()
                    .expect("error");
                let mut array = Vec::<Value>::new();
                for _ in 0..items {
                    array.push(self.parse_value());
                }
                return Value::Array(array);
            }
            _ => {
                panic!("Not supported {}", self.buf[self.pos]);
            }
        }
    }
}

fn get_command(val: Value) -> (String, Vec<Value>) {
    match val {
        Value::Array(v) => {
            let first: Value = v[0].clone();
            let rest: Vec<Value> = v.split_first().expect("error").1.to_vec();
            match first {
                Value::SimpleString(x) | Value::BulkString(x) => (x, rest),
                _ => panic!("Not a string"),
            }
        }
        _ => panic!("Not a command"),
    }
}

fn get_time() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
}

async fn read(stream: &mut TcpStream, buffer: &mut [u8]) -> usize {
    let mut total = 0;
    loop {
        let read = stream.read(&mut buffer[total..]);
        match read.await {
            Ok(0) => {
                println!("Read 0 chars. Breaking");
                break;
            }
            Ok(n) => {
                total += n;
                println!("Read {} chars", n);
                break;
            }
            Err(e) => {
                panic!("Unable to read stream: {}", e);
            }
        }
    }
    return total;
}

pub struct StoredValue {
    value: String,
    expiry: u128,
}

async fn handle_command(
    command: (String, Vec<Value>),
    store: Arc<RwLock<HashMap<String, StoredValue>>>,
) -> String {
    match command.0.to_ascii_uppercase().as_str() {
        "PING" => "+PONG\r\n".to_string(),
        "ECHO" => format!(
            "+{}\r\n",
            command
                .1
                .iter()
                .map(extract_string)
                .collect::<Vec<String>>()
                .join("")
        ),
        "SET" => {
            let cmd = &command.1;
            let key = extract_string(cmd.get(0).expect("ab"));
            let value = extract_string(cmd.get(1).expect("ab"));
            let mut to_add = Duration::from_secs(3600 * 24 * 365);
            if cmd.len() == 4 {
                let key = extract_string(cmd.get(2).expect("ab"));
                assert!(key.to_ascii_uppercase() == "PX");
                let expiry = extract_string(cmd.get(3).expect("ab"));
                to_add = Duration::from_millis(expiry.parse::<u64>().expect("fdff"));
            }
            let expiration = get_time() + to_add.as_millis();

            let mut writable = store.write().await;
            writable.insert(
                key,
                StoredValue {
                    value: value,
                    expiry: expiration,
                },
            );

            "+OK\r\n".to_string()
        }
        "GET" => {
            let key = command.1.get(0).expect("ab");
            let str: String = extract_string(key);
            let readable: tokio::sync::RwLockReadGuard<'_, HashMap<String, StoredValue>> =
                store.read().await;
            let val = readable.get(&str);
            match val {
                Some(x) => {
                    if x.expiry < get_time() {
                        "$-1\r\n".to_string()
                    } else {
                        format!("${}\r\n{}\r\n", x.value.len(), x.value)
                    }
                }
                None => "$-1\r\n".to_string(),
            }
        }
        _ => panic!("Command not recognized {}", command.0),
    }
}

async fn handle_client(
    mut store: Arc<RwLock<HashMap<String, StoredValue>>>,
    mut stream: TcpStream,
) {
    loop {
        let mut buffer: [u8; 1024] = [0; 1024];
        let n: usize = read(&mut stream, &mut buffer).await;
        println!(
            "Read string: {}\nEnd",
            String::from_utf8((&buffer[..n]).to_vec()).expect("fdfd")
        );
        let command: (String, Vec<Value>) = get_command(Parser::new(&buffer[..n]).parse_value());
        let res = handle_command(command, Arc::clone(&store)).await;
        stream.write_all(res.as_bytes()).await;
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Logs from your program will appear here!");
    let store = Arc::new(RwLock::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((_stream, _)) => {
                tokio::spawn(handle_client(Arc::clone(&store), _stream));
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_string() {
        let data = b"+ABC\r\n";
        let mut p = Parser::new(data);
        assert_eq!(p.parse_value(), Value::SimpleString(String::from("ABC")));
    }
    #[test]
    fn test_parse_empty_string() {
        let data = b"+\r\n";
        let mut p = Parser::new(data);
        assert_eq!(p.parse_value(), Value::SimpleString(String::from("")));
    }
    #[test]
    fn test_parse_bulk_string() {
        let data = b"$5\r\nabcdef\r\n";
        let mut p = Parser::new(data);
        assert_eq!(p.parse_value(), Value::BulkString(String::from("abcdef")));
    }
    #[test]
    fn test_parse_array() {
        let data = b"*2\r\n+AB\r\n+CD\r\n";
        let mut p = Parser::new(data);
        assert_eq!(
            p.parse_value(),
            Value::Array(vec![
                Value::SimpleString(String::from("AB")),
                Value::SimpleString(String::from("CD"))
            ])
        );
    }
    #[test]
    fn test_parse_echo_command() {
        let data = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        let mut p = Parser::new(data);
        assert_eq!(
            p.parse_value(),
            Value::Array(vec![
                Value::BulkString(String::from("ECHO")),
                Value::BulkString(String::from("hey"))
            ])
        );
    }
    #[test]
    fn test_parse_echo_command_no_args() {
        let data = b"*1\r\n$4\r\nECHO\r\n";
        let mut p = Parser::new(data);
        assert_eq!(
            p.parse_value(),
            Value::Array(vec![Value::BulkString(String::from("ECHO"))])
        );
    }
    #[test]
    fn test_parse_get_command() {
        let data = b"*2\r\n$3\r\nGET\r\n$3\r\nKEY\r\n";
        let mut p = Parser::new(data);
        assert_eq!(
            p.parse_value(),
            Value::Array(vec![
                Value::BulkString(String::from("GET")),
                Value::BulkString(String::from("KEY"))
            ])
        );
    }
    #[test]
    fn test_get_command_get() {
        let data = Value::Array(vec![
            Value::BulkString(String::from("GET")),
            Value::BulkString(String::from("hey")),
        ]);
        assert_eq!(
            get_command(data),
            (
                "GET".to_string(),
                vec![Value::BulkString(String::from("hey"))]
            )
        );
    }
    #[test]
    fn test_get_command_set() {
        let data = Value::Array(vec![
            Value::BulkString(String::from("SET")),
            Value::BulkString(String::from("hey")),
            Value::BulkString(String::from("value")),
        ]);
        assert_eq!(
            get_command(data),
            (
                "SET".to_string(),
                vec![
                    Value::BulkString(String::from("hey")),
                    Value::BulkString(String::from("value"))
                ]
            )
        );
    }
    #[test]
    fn test_parse_command() {
        let data = Value::Array(vec![
            Value::BulkString(String::from("ECHO")),
            Value::BulkString(String::from("hey")),
        ]);
        assert_eq!(
            get_command(data),
            (
                "ECHO".to_string(),
                vec![Value::BulkString(String::from("hey"))]
            )
        );
    }
}
