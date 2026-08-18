use std::io::{Error, ErrorKind, Read, Write};
use std::io;
use std::net::{TcpListener, TcpStream};
use std::{fs, thread};
use std::fs::OpenOptions;
use std::sync::mpsc;
use uuid;
use httparse;
use serde::{Deserialize, Serialize};
use blake3;

mod threadpool;

const CHUNK_SIZE: usize = 16 * 1024 * 1024;
const NUM_WORKERS: usize = 8;

#[derive(Serialize, Deserialize, Debug)]
struct ChunkRecord {
    index: usize,
    byte_offset: usize,
    size: usize,
    hash: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Manifest {
    file_id: String,
    file_name: String,
    total_size: usize,
    chunk_size: usize,
    chunk_count: usize,
    chunks: Vec<ChunkRecord>
}

struct Request {
    header_len: usize,
    method: String,
    path: String,
    content_length: usize,
    file_name: String,
    file_id: String,
}

fn respond(stream: &mut TcpStream,
           status_code: u16,
           reason: &str,
           content_type: &str,
           body: &str) -> Result<(), Error> {
    let response = format!(
        "HTTP/1.1 {status_code} {reason}\r\n\
        Content-Type: {content_type}\r\n\
        Content-Length: {}\r\n\
        Connection: close\r\n\
        \r\n\
        {body}",
        body.len()
    );

    stream.write_all(response.as_bytes())?;
    Ok(())
}

fn parse_request(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Result<Option<Request>, Error> {

    let mut tmp = [0u8; 512];
    loop {
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&buf) {
            Ok(httparse::Status::Complete(n)) => {
                let header_len = n;
                let method = req.method.unwrap_or("Unknown").to_string();
                let path = req.path.unwrap_or("Unknown").to_string();
                let content_length = headers.iter()
                    .find(|h| h.name.eq_ignore_ascii_case("Content-Length"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let file_name = headers.iter()
                    .find(|h| h.name.eq_ignore_ascii_case("X-File-Name"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .unwrap_or("")
                    .to_string();
                let file_id = headers.iter()
                    .find(|h| h.name.eq_ignore_ascii_case("X-File-Id"))
                    .and_then(|h| std::str::from_utf8(h.value).ok())
                    .unwrap_or("")
                    .to_string();
                let req = Request {
                    header_len,
                    method,
                    path,
                    content_length,
                    file_name,
                    file_id,
                };
                return Ok(Some(req))
            }
            Ok(httparse::Status::Partial) => {
                let n = stream.read(&mut tmp)?;
                if n == 0 {
                    return Ok(None);
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            Err(e) => {
                return Err(Error::new(ErrorKind::InvalidData, e.to_string()));
            }
        }
    }
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

fn upload_file(mut stream: &mut TcpStream, mut buf: &mut Vec<u8>, req: Request) -> Result<(), Error> {
    let file_id = uuid::Uuid::new_v4().to_string();
    let thread_pool = threadpool::ThreadPool::new(NUM_WORKERS);
    let (result_tx, result_rx) = mpsc::channel::<Result<ChunkRecord, String>>();
    buf.drain(..req.header_len);

    let mut bytes_written: usize = 0;
    let mut chunk_index: usize = 0;
    while bytes_written < req.content_length {
        let remaining = req.content_length - bytes_written;
        match read_chunk(&mut stream, &mut buf, remaining) {
            Ok(Some(chunk_data)) => {
                let index = chunk_index;
                let byte_offset = bytes_written;
                let size = chunk_data.len();
                let result_tx = result_tx.clone();
                thread_pool.execute(move || {
                    let hash = blake3::hash(&chunk_data);
                    let hex_hash = hash.to_hex().to_string();
                    let filename = format!("chunks/{hex_hash}");
                    let result = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&filename);
                    let write_attempt = match result {
                        Ok(mut file) => {
                            match file.write_all(&chunk_data) {
                                Ok(()) => {
                                    Ok(ChunkRecord { index, byte_offset, size, hash: hex_hash})
                                }
                                Err(e) => Err(format!("Failed to write {filename}: {e}"))
                            }
                        }
                        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(ChunkRecord { index, byte_offset, size, hash: hex_hash }),
                        Err(e) => Err(format!("Error: {e}"))
                    };
                    match write_attempt {
                        Ok(record) => {
                            let _ = result_tx.send(Ok(record));
                        }
                        Err(e) => {
                            let _ = result_tx.send(Err(e));
                        }
                    }
                });
                bytes_written += size;
                chunk_index+=1;
            }
            Ok(None) => {
                println!("Client disconnected early, got {bytes_written}/{0} bytes", req.content_length);
                drop(result_tx);
                thread_pool.join();
                return Ok(());
            }
            Err(e) => {
                println!("Read error {e}");
                return Ok(())
            }
        }
    }
    drop(result_tx);
    thread_pool.join();
    let mut chunks = Vec::new();

    for result in result_rx {
        match result {
            Ok(record) => chunks.push(record),
            Err(e) => {
                eprintln!("Upload failed: {e}");
                respond(&mut stream, 500, "Chunk Write Failed", "text/plain", "")?;
                return Ok(());
            }
        }
    }
    chunks.sort_by_key(|chunk| chunk.index);
    let manifest = Manifest {
        file_id: file_id.clone(),
        file_name: req.file_name,
        total_size: req.content_length,
        chunk_size: CHUNK_SIZE,
        chunk_count: chunks.len(),
        chunks,
    };
    let manifest_file = serde_json::to_string_pretty(&manifest)?;
    fs::write(format!("manifests/{file_id}.json"), manifest_file)?;
    let body: &str = &format!("{{\"file_id\" : \"{}\"}}", file_id);
    respond(&mut stream, 201, "Created", "application/json", body)?;

    Ok(())
}
fn download_file(file_id: String, stream: &mut TcpStream) -> Result<(), Error> {
    if uuid::Uuid::parse_str(&file_id.as_str()).is_err() {
        return respond(stream, 400, "Invalid file_id", "text/plain", "");
    }
    let json_string = fs::read_to_string(format!("manifests/{file_id}.json"))?;
    let manifest: Manifest = serde_json::from_str(&json_string)?;
    let chunks = manifest.chunks;
    let header = format!(
        "HTTP/1.1 200 OK\r\n\
        Content-Type: application/octet-stream\r\n\
        Content-Disposition: attachment; filename=\"{}\"\r\n\
        Content-Length: {}\r\n\
        Connection: close\r\n\
        \r\n",
        manifest.file_name, manifest.total_size
    );
    stream.write_all(header.as_bytes())?;

    for chunk in &chunks {
        let chunk_file_path = format!("chunks/{}", chunk.hash);
        let data = fs::read(chunk_file_path)?;
        let hash = blake3::hash(&data);
        let hex_hash = hash.to_hex().to_string();
        if hex_hash != chunk.hash {
            return Err(Error::new(
                io::ErrorKind::InvalidData,
                format!("Chunk {} hash mismatch: expected {}, got {}", chunk.index, chunk.hash, hex_hash),
            ));
        }
        stream.write_all(&data)?;
    }
    Ok(())
}
fn handle_client(mut stream: TcpStream) -> Result<(), Error> {
    let mut buf: Vec<u8> = Vec::new();
    let req = match parse_request(&mut stream, &mut buf) {
        Ok(Some(req)) => {
            req
        },
        Ok(None) => {
            println!("Connection closed before headers finished");
            return Ok(())
        }
        Err(e) => {
            println!("Parse error: {e}");
            return Err(e)
        }
    };
    match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/upload") => {
            if req.content_length == 0 {
                respond(&mut stream, 411, "Length is required", "text/plain", "")?;
                return Ok(());
            }
            if req.file_name.is_empty() {
                respond(&mut stream, 400, "X-File-Name header is required", "text/plain", "")?;
                return Ok(());
            }
            upload_file(&mut stream, &mut buf, req)?;
        }

        ("GET", "/download") => {

            if req.file_id.is_empty() {
                respond(&mut stream, 400, "X-File-Id header is required", "text/plain", "")?;
                return Ok(());
            }
            download_file(req.file_id, &mut stream)?;

        }
        _ => {
            respond(&mut stream, 404, "Incorrect method type or path", "text/plain", "")?;

        }
    }
    Ok(())
}

pub fn start_server() -> std::io::Result<()>{
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    fs::create_dir_all("chunks")?;
    fs::create_dir_all("manifests")?;
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