//! Minimal bootstrap HTTP process.
//!
//! This standard-library server proves the workspace and health-check contract.
//! It is intentionally not the production API server.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::ExitCode;
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 8 * 1024;

fn main() -> ExitCode {
    let address = std::env::var("AETHERFLOW_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    match run(&address) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aetherflow-server: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(address: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(address)?;
    println!(
        "AetherFlow {} listening on {}",
        aetherflow_core::VERSION,
        listener.local_addr()?
    );

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                if let Err(error) = handle_connection(stream) {
                    eprintln!("request error: {error}");
                }
            }
            Err(error) => eprintln!("connection error: {error}"),
        }
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut request = vec![0_u8; MAX_REQUEST_BYTES];
    let bytes_read = stream.read(&mut request)?;
    let request_line = request[..bytes_read]
        .split(|byte| *byte == b'\n')
        .next()
        .unwrap_or_default();
    let mut parts = request_line.split(|byte| *byte == b' ');
    let method = parts.next().unwrap_or_default();
    let path = parts.nth(1).unwrap_or_default();

    let (status, body) = if method == b"GET" && path == b"/health" {
        ("200 OK", "{\"status\":\"ok\"}")
    } else {
        ("404 Not Found", "{\"error\":\"not_found\"}")
    };

    write!(
        stream,
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::handle_connection;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    fn request(path: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let address = listener.local_addr().expect("read local address");

        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept connection");
            handle_connection(stream).expect("handle request");
        });

        let mut client = TcpStream::connect(address).expect("connect");
        write!(client, "GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n")
            .expect("send request");

        let mut response = String::new();
        client.read_to_string(&mut response).expect("read response");
        server.join().expect("server thread");
        response
    }

    #[test]
    fn health_endpoint_returns_ok() {
        let response = request("/health");
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("{\"status\":\"ok\"}"));
    }

    #[test]
    fn unknown_path_returns_not_found() {
        let response = request("/not-a-route");
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
        assert!(response.ends_with("{\"error\":\"not_found\"}"));
    }
}
