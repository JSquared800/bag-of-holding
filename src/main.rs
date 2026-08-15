use std::io::{Error, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use uuid;
use httparse;
use serde::Serialize;
use blake3;

// const CHUNK_SIZE: usize = 16 * 1024 * 1024;
const CHUNK_SIZE: usize = 4;
const NUM_WORKERS: i16 = 8;

#[derive(Serialize)]
struct ChunkRecord {
    index: usize,
    byte_offset: usize,
    size: usize,
    hash: String,
}

#[derive(Serialize)]
struct Manifest {
    file_id: String,
    file_name: String,
    total_size: usize,
    chunk_size: usize,
    chunk_count: usize,
    chunks: Vec<ChunkRecord>
}

fn respond(stream: &mut TcpStream, status_code: u16, reason: &str) -> Result<(), Error> {
    let body = reason;
    let response = format!(
        "HTTP/1.1 {status_code} {reason}\r\n\
        Content-Type: text/plain\r\n\
        Content-Length: {}\r\n\
        Connection: close\r\n\
        \r\n\
        {body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    Ok(())
}
fn write_chunk(output_file: &str, buf: &Vec<u8>) -> std::io::Result<()> {
    std::fs::write(output_file, buf)
}
fn read_chunk(stream: &mut TcpStream, buf: &mut Vec<u8>, remaining: usize) -> std::io::Result<Option<Vec<u8>>> {
    let target = std::cmp::min(remaining, CHUNK_SIZE);
    let mut total = buf.len();
    if total >= target {
        let chunk: Vec<u8> = buf.drain(..target).collect();
        return Ok(Some(chunk));
    }
    buf.resize(target, 0);
    while total < target {
        match stream.read(&mut buf[total..target])? {
            0 => {
                buf.truncate(total);
                return Ok(None);
            }
            n => total += n,
        }
    }
    Ok(Some(buf.drain(..target).collect()))
}

fn handle_client(mut stream: TcpStream) -> Result<(), Error> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 512];
    let file_id = uuid::Uuid::new_v4().to_string();
    let header_len: usize;
    let method: String;
    let path: String;
    let content_length: usize;
    let file_name: String;
    println!("Handling client!");
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(n)) => {
                header_len = n;
                method = req.method.unwrap_or("Unknown").to_string();
                path = req.path.unwrap_or("Unknown").to_string();
                content_length = headers.iter()
                    .find(|h| h.name.eq_ignore_ascii_case("Content-Length"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                file_name = headers.iter()
                    .find(|h| h.name.eq_ignore_ascii_case("X-File-Name"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .unwrap_or("")
                    .to_string();

                break;
            }
            Ok(httparse::Status::Partial) => {
                let n = stream.read(&mut tmp)?;
                if n == 0 {
                    println!("Connection closed before headers finished");
                    return Ok(());
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(e) => {
                println!("Parse error: {e}");
                return Ok(());
            }
        }
    }
    println!("header_len={header_len} method={method} path={path} content_length={content_length}");
    if method != "POST" || path != "/upload" {
        respond(&mut stream, 404, "Incorrect method type or path")?;
        return Ok(());
    }
    if content_length == 0 {
        respond(&mut stream, 411, "Length is required")?;
        return Ok(());
    }
    if file_name.is_empty() {
        respond(&mut stream, 400, "X-File-Name header is required")?;
        return Ok(());
    }
    let mut manifest = Manifest {
        file_id: file_id.clone(),
        file_name,
        total_size: content_length,
        chunk_size: CHUNK_SIZE,
        chunk_count: 0,
        chunks: Vec::new(),
    };
    std::fs::create_dir(format!("{file_id}"))?;
    buf.drain(..header_len);
    let mut bytes_written: usize = 0;
    let mut chunk_index: usize = 0;
    while bytes_written < content_length {
        let remaining = content_length - bytes_written;
        match read_chunk(&mut stream, &mut buf, remaining) {
            Ok(Some(chunk_data)) => {
                let filename = format!("{file_id}/{file_id}_{chunk_index:05}");
                let hash = blake3::hash(&chunk_data);
                let hex_hash = hash.to_hex().to_string();
                let record = ChunkRecord {
                    index: chunk_index,
                    byte_offset: bytes_written,
                    size: chunk_data.len(),
                    hash: hex_hash,
                };
                println!("Getting ready to write to {filename}");
                write_chunk(&filename, &chunk_data)?;
                manifest.chunk_count+=1;
                manifest.chunks.push(record);
                bytes_written += chunk_data.len();
                chunk_index+=1;
            }
            Ok(None) => {
                println!("Client disconnected early, got {bytes_written}/{content_length} bytes");
                return Ok(());
            }
            Err(e) => {
                println!("Read error {e}");
                return Ok(())
            }
        }
    }
    let manifest_file = serde_json::to_string_pretty(&manifest)?;
    std::fs::write(format!("{file_id}/{file_id}_manifest.json"), manifest_file)?;
    respond(&mut stream, 200, "OK")?;
    Ok(())
}

fn main() -> std::io::Result<()>{
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    for stream in listener.incoming() {
        thread::spawn(||{
            match stream {
                Ok(stream) => handle_client(stream),
                Err(e) => {
                    println!("An error occurred with the stream: {e}");
                    Err(e)
                }
            }
        });
    }
    Ok(())
}