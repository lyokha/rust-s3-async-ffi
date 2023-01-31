use tokio::runtime::{Builder, Runtime};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use futures::future::FutureExt;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::error::S3Error;
use ffi_convert::{CReprOf, CDrop, AsRust};
use serde::Deserialize;
use std::os::fd::AsRawFd;
use std::os::raw::{c_char, c_int, c_void};
use std::ffi::CStr;


#[repr(C)]
#[derive(CReprOf, CDrop, AsRust)]
#[target_type(BucketDescr)]
pub struct CBucketDescr {
    name: *const c_char,
    region: *const c_char,
    #[nullable] access_key: *const c_char,
    #[nullable] secret_key: *const c_char,
    #[nullable] security_token: *const c_char,
    #[nullable] session_token: *const c_char,
    #[nullable] expiration: *const c_char
}

#[derive(Deserialize)]
struct BucketDescr {
    name: String,
    region: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    security_token: Option<String>,
    session_token: Option<String>,
    expiration: Option<String>
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_init_bucket(bucket: *const CBucketDescr) -> *mut Bucket {
    if bucket.is_null() {
        return std::ptr::null_mut();
    }

    let bucket = unsafe { (*bucket).as_rust() };

    match bucket {
        Ok(bucket) => {
            let bucket_name = bucket.name;

            let region = bucket.region.parse();
            if region.is_err() {
                return std::ptr::null_mut();
            }
            let region = region.unwrap();

            let credentials = Credentials::new(bucket.access_key.as_deref(),
                                               bucket.secret_key.as_deref(),
                                               bucket.security_token.as_deref(),
                                               bucket.session_token.as_deref(),
                                               bucket.expiration.as_deref());
            if credentials.is_err() {
                return std::ptr::null_mut();
            }
            let credentials = credentials.unwrap();

            let handle = Bucket::new(&bucket_name, region, credentials);
            if handle.is_err() {
                return std::ptr::null_mut();
            }
            let handle = handle.unwrap();

            Box::into_raw(Box::new(handle))
        },
        _ => std::ptr::null_mut()
    }

}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_close_bucket(rt: *mut Bucket) {
    unsafe { *Box::from_raw(rt) };
}


type StdUnixStream = std::os::unix::net::UnixStream;

fn init_object_stream() -> std::io::Result<(StdUnixStream, StdUnixStream)> {
    let (client, server) = StdUnixStream::pair()?;

    client.set_nonblocking(true).unwrap();
    server.set_nonblocking(true).unwrap();

    Ok((client, server))
}


#[repr(C)]
pub struct StreamHandle {
    rt_handle: *const Runtime,
    join_handle: Box<JoinHandle<Result<u16, S3Error>>>,
    client: Box<StdUnixStream>,
    fd: c_int
}


#[no_mangle]
pub extern "C" fn c_init_tokio_runtime() -> *const Runtime {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    Box::into_raw(Box::new(rt))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_close_tokio_runtime(rt: *mut Runtime) {
    unsafe { *Box::from_raw(rt) };
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_init_object_stream(rt: *const Runtime, bucket: *const Bucket,
    write: c_int, path: *const c_char) -> *mut StreamHandle
{
    let rt = rt.as_ref().unwrap();

    let pair = init_object_stream();
    if pair.is_err() {
        return std::ptr::null_mut();
    }

    let (client, server) = pair.unwrap();
    let fd = client.as_raw_fd();
    let bucket = bucket.as_ref().unwrap();
    let path = unsafe { CStr::from_ptr(path).to_str().unwrap().to_owned() };

    let join_handle = rt.spawn(async move {
        let mut server = UnixStream::from_std(server).unwrap();
        let res = if write == 0 {
            get_object(bucket, &mut server, &path).await
        } else {
            put_object(bucket, &mut server, &path).await
        };
        res
    });

    Box::into_raw(Box::new(StreamHandle{
        rt_handle: rt, join_handle: Box::new(join_handle), client: Box::new(client), fd }))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_put_object_chunk(
    handle: *mut StreamHandle, chunk: *const c_void, size: usize, count: *mut isize,
    errno : *mut i32) -> *mut StreamHandle
{
    let handle = unsafe { *Box::from_raw(handle) };

    *count = unsafe { nix::libc::write(handle.fd, chunk, size) };
    *errno = errno::errno().0;

    Box::into_raw(Box::new(handle))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_get_object_chunk(
    handle: *mut StreamHandle, chunk: *mut c_void, size: usize, count: *mut isize,
    errno : *mut i32) -> *mut StreamHandle
{
    let StreamHandle{ rt_handle, join_handle, client, fd } = unsafe { *Box::from_raw(handle) };

    *count = unsafe { nix::libc::read(fd, chunk, size) };
    *errno = errno::errno().0;

    Box::into_raw(Box::new(StreamHandle{ rt_handle, join_handle: Box::new(*join_handle),
        client: Box::new(*client), fd }))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_close_stream(handle: *mut StreamHandle) ->
    *mut JoinHandle<Result<u16, S3Error>>
{
    let StreamHandle{ rt_handle: _rt_handle, join_handle, client: _client, fd: _fd} =
        unsafe { *Box::from_raw(handle) };

    Box::into_raw(Box::new(*join_handle))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn c_get_task_status(handle: *mut JoinHandle<Result<u16, S3Error>>,
    status_code: *mut c_int) -> *mut JoinHandle<Result<u16, S3Error>>
{
    let task = unsafe { *Box::from_raw(handle) };

    if task.is_finished() {
        match task.now_or_never().unwrap().unwrap() {
            Ok(status) => *status_code = status as c_int,
            Err(S3Error::Http(status, _body)) => *status_code = status as c_int,
            Err(_) => *status_code = -1
        }
        std::ptr::null_mut()
    } else {
        unsafe { *status_code = 0 }
        Box::into_raw(Box::new(task))
    }
}


async fn put_object(bucket: &Bucket, server: &mut UnixStream, path: &str) -> Result<u16, S3Error>
{
    let status = bucket.put_object_stream(server, path).await;
    let status_code = match status {
        Ok(status) => status,
        Err(S3Error::Http(status, _body)) => status,
        Err(_) => 500
    };
    Ok(status_code)
}


async fn get_object(bucket: &Bucket, server: &mut UnixStream, path: &str) -> Result<u16, S3Error>
{
    let status = bucket.get_object_stream(path, server).await;
    let status_code = match status {
        Ok(status) => status,
        Err(S3Error::Http(status, _body)) => status,
        Err(_) => 500
    };
    Ok(status_code)
}


#[cfg(test)]
mod tests {
    use crate::{BucketDescr, CBucketDescr,
                c_init_bucket, c_close_bucket, c_init_tokio_runtime, c_init_object_stream,
                c_put_object_chunk, c_get_object_chunk, c_get_task_status, c_close_stream};
    use tokio::time::{sleep, Duration};
    use ffi_convert::CReprOf;
    use std::os::raw::c_void;
    use config::{Config, File};
    use serde::Deserialize;
    use chrono::Utc;

    #[derive(Deserialize)]
    struct Path {
        value: String
    }

    #[tokio::test]
    // test this as 'cargo test -- --nocapture' to see what happens under the hood
    async fn write_and_read_chunked() -> std::io::Result<()> {

        // Read bucket configuration
        let source = File::with_name("test/data/bucket.toml");
        let config = Config::builder().add_source(source).build().expect("Bad bucket config");
        let bucket_descr = config.try_deserialize::<BucketDescr>().expect("Bad bucket config");

        // Read path
        let source = File::with_name("test/data/path.toml");
        let config = Config::builder().add_source(source).build().expect("Bad path");
        let path = config.try_deserialize::<Path>().expect("Bad path");

        // Pretend that we are C and call synchronous functions

        // Initialize bucket
        let bucket = unsafe { c_init_bucket(&CBucketDescr::c_repr_of(bucket_descr).unwrap()) };

        if bucket.is_null() {
            panic!("Failed to initialize s3 bucket");
        }

        // Initialize tokio runtime
        let rt = c_init_tokio_runtime();

        let path = std::ffi::CString::new(path.value).unwrap();

        // Initialize writing an object
        let mut handle = unsafe { c_init_object_stream(rt, bucket, 1, path.as_ptr()) };

        if handle.is_null() {
            panic!("Failed to initialize write object stream");
        }

        let now = Utc::now().to_string();
        let now_len = now.len();

        let chunks = vec![(b"Chunk 1\n".as_ptr() as *const c_void, 8),
                          (b"Chunk 2\n".as_ptr() as *const c_void, 8),
                          (now.as_bytes().as_ptr() as *const c_void, now_len),
                          (now.as_bytes().as_ptr() as *const c_void, now_len)];

        let mut w_contents: Vec<u8> = Vec::new();

        // Write chunks one-by-one
        for chunk in chunks {
            loop {
                let mut count = 0;
                let mut errno = 0;

                handle = unsafe{
                    c_put_object_chunk(handle, chunk.0, chunk.1, &mut count, &mut errno)
                };

                if count < 0 && (errno == nix::libc::EAGAIN || errno == nix::libc::EWOULDBLOCK) {
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }

                let contents = unsafe {
                    std::slice::from_raw_parts(chunk.0 as *const u8, count as usize)
                };

                w_contents.extend(contents);

                let contents = String::from_utf8_lossy(&contents);
                println!(">>> {:2} bytes written | {contents}", count);

                break;
            }
        }

        // Close writing stream
        let mut handle = unsafe { c_close_stream(handle) };

        let mut status = 0;

        // Get write status code
        loop {
            handle = unsafe { c_get_task_status(handle, &mut status) };
            if handle.is_null() {
                break
            }

            sleep(Duration::from_millis(10)).await;
        }

        println!("---\nObject write complete, status: {status}\n");

        // Wait a moment, otherwise S3 is not so fast and may return nothing or an older object
        sleep(Duration::from_millis(1000)).await;

        // Initialize reading the object just written
        let mut handle = unsafe { c_init_object_stream(rt, bucket, 0, path.as_ptr()) };

        if handle.is_null() {
            panic!("Failed to initialize read object stream");
        }

        // Read buffer
        let mut buf = [0; 16];

        let mut r_contents: Vec<u8> = Vec::new();

        // Read chunks one-by-one into the buffer
        loop {
            let mut count = 0;
            let mut errno = 0;

            handle = unsafe{
                c_get_object_chunk(handle, buf.as_mut_ptr() as *mut c_void, buf.len(),
                                   &mut count, &mut errno)
            };

            if count <= 0 {
                if count < 0 && (errno == nix::libc::EAGAIN || errno == nix::libc::EWOULDBLOCK) {
                    sleep(Duration::from_millis(10)).await;
                    continue;
                }

                break
            };

            let contents = &buf[..count as usize];

            r_contents.extend(contents);

            let contents = String::from_utf8_lossy(contents);
            println!(">>> {:2} bytes read | {contents}", count);
        }

        // Close reading stream
        let mut handle = unsafe { c_close_stream(handle) };

        let mut status = 0;

        // Get read status code
        loop {
            handle = unsafe { c_get_task_status(handle, &mut status) };
            if handle.is_null() {
                break
            }

            sleep(Duration::from_millis(10)).await;
        }

        println!("---\nObject read complete, status: {status}\n");

        // Close tokio runtime
        // (skip this because dropping runtimes is not allowed in asynchronous contexts)
        // unsafe { c_close_tokio_runtime(rt) };

        // Close bucket
        unsafe { c_close_bucket(bucket) };

        // Test that the written and the read data are equal
        assert!(w_contents == r_contents);

        Ok(())
    }
}

