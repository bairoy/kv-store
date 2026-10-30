#[derive(Debug, PartialEq)]
pub enum Command {
    Set(String, String),
    Get(String),
    Delete(String),
}

pub enum Response {
    Ok,
    Value(String),
    NotFound,
    Error(String),
}

pub fn parse_request(request: &str) -> Option<Command> {
    let parts: Vec<&str> = request.splitn(3, ' ').collect();

    match parts.as_slice() {
        ["SET", key, value] => Some(Command::Set(key.to_string(), value.to_string())),

        ["GET", key] => Some(Command::Get(key.to_string())),

        ["DELETE", key] => Some(Command::Delete(key.to_string())),

        _ => None,
    }
}

pub fn encode_response(response: Response) -> String {
    match response {
        Response::Ok => "+OK".to_string(),
        Response::Value(value) => {
            format!("${}\r\n{}\r\n", value.len(), value)
        }
        Response::NotFound => "$-1\r\n".to_string(),
        Response::Error(message) => {
            format!("-ERR {message}")
        }
    }
}
