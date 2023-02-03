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
use nix::libc::{size_t, ssize_t};
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
pub unsafe extern "C" fn rust_s3_init_bucket(bucket: *const CBucketDescr) -> *mut Bucket {
    if bucket.is_null() {
        return std::ptr::null_mut();
    }

    let bucket = (*bucket).as_rust();

    match bucket {
        Ok(bucket) => {
            let bucket_name = bucket.name;

            let region = bucket.region.parse();
            if region.is_err() {
                return std::ptr::null_mut();
            }

            let credentials = Credentials::new(bucket.access_key.as_deref(),
                                               bucket.secret_key.as_deref(),
                                               bucket.security_token.as_deref(),
                                               bucket.session_token.as_deref(),
                                               bucket.expiration.as_deref());
            if credentials.is_err() {
                return std::ptr::null_mut();
            }

            let handle = Bucket::new(&bucket_name, region.unwrap(), credentials.unwrap());
            if handle.is_err() {
                return std::ptr::null_mut();
            }

            Box::into_raw(Box::new(handle.unwrap()))
        },
        _ => std::ptr::null_mut()
    }

}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_close_bucket(bucket: *mut Bucket) {
    drop(*Box::from_raw(bucket))
}


#[no_mangle]
pub extern "C" fn rust_s3_init_tokio_runtime() -> *const Runtime {
    let rt = Builder::new_multi_thread().enable_all().build().unwrap();

    Box::into_raw(Box::new(rt))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_close_tokio_runtime(rt: *mut Runtime) {
    drop(*Box::from_raw(rt))
}


type StdUnixStream = std::os::unix::net::UnixStream;

type S3Result = Result<u16, S3Error>;

#[repr(C)]
pub struct StreamHandle {
    rt_handle: *const Runtime,
    join_handle: Box<JoinHandle<S3Result>>,
    client: Box<StdUnixStream>,
    fd: c_int
}


unsafe fn init_object_stream(write: bool, rt: *const Runtime, bucket: *const Bucket,
    path: *const c_char) -> *mut StreamHandle
{
    let pair = StdUnixStream::pair();
    if pair.is_err() {
        return std::ptr::null_mut();
    }

    let (client, server) = pair.unwrap();

    client.set_nonblocking(true).unwrap();
    server.set_nonblocking(true).unwrap();

    let fd = client.as_raw_fd();

    let rt = rt.as_ref().unwrap();
    let bucket = bucket.as_ref().unwrap();
    let path = CStr::from_ptr(path).to_str().unwrap().to_owned();

    let join_handle = rt.spawn(async move {
        let mut server = UnixStream::from_std(server).unwrap();
        stream_object(write, bucket, &mut server, &path).await
    });

    Box::into_raw(Box::new(StreamHandle {
        rt_handle: rt, join_handle: Box::new(join_handle), client: Box::new(client), fd
    }))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_write_object_stream(rt: *const Runtime, bucket: *const Bucket,
    path: *const c_char) -> *mut StreamHandle
{
    init_object_stream(true, rt, bucket, path)
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_read_object_stream(rt: *const Runtime, bucket: *const Bucket,
    path: *const c_char) -> *mut StreamHandle
{
    init_object_stream(false, rt, bucket, path)
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_close_object_stream(handle: *mut StreamHandle) ->
    *mut JoinHandle<S3Result>
{
    let StreamHandle { rt_handle: _rt_handle, join_handle, client: _client, fd: _fd } =
        *Box::from_raw(handle);

    Box::into_raw(Box::new(*join_handle))
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_write_object_chunk(handle: *mut StreamHandle,
    chunk: *const c_void, size: size_t, errno : *mut c_int) -> ssize_t
{
    let count = nix::libc::write(handle.as_mut().unwrap().fd, chunk, size) as ssize_t;
    *errno = errno::errno().0;

    count
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_read_object_chunk(handle: *mut StreamHandle,
    chunk: *mut c_void, size: size_t, errno : *mut c_int) -> ssize_t
{
    let count = nix::libc::read(handle.as_mut().unwrap().fd, chunk, size) as ssize_t;
    *errno = errno::errno().0;

    count
}


const ASYNC_TASK_NOT_READY: c_int = -2;

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_get_task_status(handle: *mut JoinHandle<S3Result>) -> c_int {
    let task = handle.as_mut().unwrap();

    if task.is_finished() {
        match task.now_or_never().unwrap().unwrap() {
            Ok(status) => status as c_int,
            Err(S3Error::Http(status, _body)) => status as c_int,
            Err(_) => -1
        }
    } else {
        ASYNC_TASK_NOT_READY
    }
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_close_task(handle: *mut JoinHandle<S3Result>) {
    drop(*Box::from_raw(handle))
}


async fn stream_object(write: bool, bucket: &Bucket, server: &mut UnixStream, path: &str) ->
    S3Result
{
    let status = if write {
        bucket.put_object_stream(server, path).await
    } else {
        bucket.get_object_stream(path, server).await
    };

    let status_code = match status {
        Ok(status) => status,
        Err(S3Error::Http(status, _body)) => status,
        Err(_) => 500
    };

    Ok(status_code)
}


#[cfg(test)]
mod tests {
    use crate::{BucketDescr, CBucketDescr, ASYNC_TASK_NOT_READY,
                rust_s3_init_bucket, rust_s3_close_bucket,
                rust_s3_init_tokio_runtime, /* rust_s3_close_tokio_runtime, */
                rust_s3_write_object_stream, rust_s3_read_object_stream,
                rust_s3_close_object_stream,
                rust_s3_write_object_chunk, rust_s3_read_object_chunk,
                rust_s3_get_task_status, rust_s3_close_task};
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
        let bucket = unsafe {
            rust_s3_init_bucket(&CBucketDescr::c_repr_of(bucket_descr).unwrap())
        };

        if bucket.is_null() {
            panic!("Failed to initialize s3 bucket");
        }

        // Initialize tokio runtime
        let rt = rust_s3_init_tokio_runtime();

        let path = std::ffi::CString::new(path.value).unwrap();

        // Initialize writing an object
        let handle = unsafe { rust_s3_write_object_stream(rt, bucket, path.as_ptr()) };

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
                let mut errno = 0;

                let count = unsafe {
                    rust_s3_write_object_chunk(handle, chunk.0, chunk.1, &mut errno)
                };

                if count < 0 {
                    if errno == nix::libc::EAGAIN || errno == nix::libc::EWOULDBLOCK {
                        sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    break;
                }

                let contents = unsafe {
                    std::slice::from_raw_parts(chunk.0 as *const u8, count as usize)
                };

                w_contents.extend(contents);

                let contents = String::from_utf8_lossy(contents);
                println!(">>> {:2} bytes written | {contents}", count);

                break;
            }
        }

        // Close writing stream
        let handle = unsafe { rust_s3_close_object_stream(handle) };

        let mut status;

        // Get write status code
        while { status = unsafe { rust_s3_get_task_status(handle) };
                status == ASYNC_TASK_NOT_READY
        } { sleep(Duration::from_millis(10)).await }

        // Close task
        unsafe { rust_s3_close_task(handle) };

        println!("---\nObject write complete, status: {status}\n");

        // Wait a moment, otherwise S3 is not so fast and may return nothing or an older object
        sleep(Duration::from_millis(1000)).await;

        // Initialize reading the object just written
        let handle = unsafe { rust_s3_read_object_stream(rt, bucket, path.as_ptr()) };

        if handle.is_null() {
            panic!("Failed to initialize read object stream");
        }

        // Read buffer
        let mut buf = [0; 16];

        let mut r_contents: Vec<u8> = Vec::new();

        // Read chunks one-by-one into the buffer
        loop {
            let mut errno = 0;

            let count = unsafe {
                rust_s3_read_object_chunk(handle, buf.as_mut_ptr() as *mut c_void, buf.len(),
                    &mut errno)
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
        let handle = unsafe { rust_s3_close_object_stream(handle) };

        let mut status;

        // Get read status code
        while { status = unsafe { rust_s3_get_task_status(handle) };
                status == ASYNC_TASK_NOT_READY
        } { sleep(Duration::from_millis(10)).await }

        // Close task
        unsafe { rust_s3_close_task(handle) };

        println!("---\nObject read complete, status: {status}\n");

        // Close tokio runtime
        // (skip this because dropping runtimes is not allowed in asynchronous contexts)
        // unsafe { rust_s3_close_tokio_runtime(rt) };

        // Close bucket
        unsafe { rust_s3_close_bucket(bucket) };

        // Test that the written and the read data are equal
        assert!(w_contents == r_contents);

        Ok(())
    }
}

