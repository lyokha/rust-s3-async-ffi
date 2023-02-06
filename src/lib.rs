use tokio::runtime::{Builder, Runtime};
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio::io::AsyncWriteExt;
use futures::future::FutureExt;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::error::S3Error;
use ffi_convert::{CReprOf, CDrop, AsRust};
use serde::Deserialize;
use std::os::fd::AsRawFd;
use std::os::raw::{c_char, c_int, c_void};
use nix::libc::{self, size_t, ssize_t};
use nix::sys::socket;
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


unsafe fn spawn_object_stream(write: bool, rt: *const Runtime, bucket: *const Bucket,
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
        if write {
            let res = bucket.put_object_stream(&mut server, path).await;
            server.write_u8(1).await?;
            res
        } else {
            bucket.get_object_stream(path, &mut server).await
        }
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
    spawn_object_stream(true, rt, bucket, path)
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_read_object_stream(rt: *const Runtime, bucket: *const Bucket,
    path: *const c_char) -> *mut StreamHandle
{
    spawn_object_stream(false, rt, bucket, path)
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_write_object_stream_done(handle: *mut StreamHandle,
    errno: *mut c_int) -> c_int
{
    let res = match socket::shutdown(handle.as_mut().unwrap().fd, socket::Shutdown::Write) {
        Ok(()) => 0,
        Err(_) => -1
    };
    *errno = errno::errno().0;

    res
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
    let count = libc::write(handle.as_mut().unwrap().fd, chunk, size) as ssize_t;
    *errno = errno::errno().0;

    count
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_read_object_chunk(handle: *mut StreamHandle,
    chunk: *mut c_void, size: size_t, errno : *mut c_int) -> ssize_t
{
    let count = libc::read(handle.as_mut().unwrap().fd, chunk, size) as ssize_t;
    *errno = errno::errno().0;

    count
}


const ASYNC_TASK_ERROR: c_int = -1;
const ASYNC_TASK_NOT_READY: c_int = -2;

#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_get_task_status(handle: *mut JoinHandle<S3Result>,
    msg: *mut *mut c_char) -> c_int
{
    if !msg.is_null() {
        *msg = std::ptr::null_mut()
    }

    let task = handle.as_mut().unwrap();

    if task.is_finished() {
        match task.now_or_never().unwrap() {
            Ok(Ok(status)) => status as c_int,
            Ok(Err(S3Error::Http(status, body))) => {
                if !msg.is_null() && !body.is_empty() {
                    *msg = alloc_msg(&body)
                };
                status as c_int
            },
            Ok(Err(s3_error)) => {
                if !msg.is_null() {
                    *msg = alloc_msg(&s3_error.to_string())
                };
                ASYNC_TASK_ERROR
            },
            Err(join_error) => {
                if !msg.is_null() {
                    *msg = alloc_msg(&join_error.to_string())
                };
                ASYNC_TASK_ERROR
            }
        }
    } else {
        ASYNC_TASK_NOT_READY
    }
}


unsafe fn alloc_msg(msg: &str) -> *mut c_char {
    let len = msg.len();
    let buf = libc::malloc(len + 1) as *mut c_char;
    libc::memcpy(buf as *mut c_void, msg.as_bytes().as_ptr() as *const c_void, len);
    let last = buf.add(len);
    *last = '\0' as c_char;
    buf
}


#[no_mangle]
#[allow(clippy::missing_safety_doc)]
pub unsafe extern "C" fn rust_s3_close_task(handle: *mut JoinHandle<S3Result>) {
    drop(*Box::from_raw(handle))
}


#[cfg(test)]
mod tests {
    use crate::{BucketDescr, CBucketDescr, ASYNC_TASK_NOT_READY,
                rust_s3_init_bucket, rust_s3_close_bucket,
                rust_s3_init_tokio_runtime, /* rust_s3_close_tokio_runtime, */
                rust_s3_write_object_stream, rust_s3_read_object_stream,
                rust_s3_write_object_stream_done, rust_s3_close_object_stream,
                rust_s3_write_object_chunk, rust_s3_read_object_chunk,
                rust_s3_get_task_status, rust_s3_close_task};
    use tokio::time::{sleep, Duration};
    use ffi_convert::CReprOf;
    use std::os::raw::c_void;
    use config::{Config, File};
    use serde::Deserialize;
    use nix::libc;
    use std::ffi::{CStr, CString};

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

        let path = CString::new(path.value).unwrap();

        // Initialize writing an object
        let handle = unsafe { rust_s3_write_object_stream(rt, bucket, path.as_ptr()) };

        if handle.is_null() {
            panic!("Failed to initialize write object stream");
        }

        let now = chrono::Utc::now().to_string();
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
                    if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                        sleep(Duration::from_millis(10)).await;
                        continue
                    } else {
                        panic!("Failed to write chunks: {:?}",
                               unsafe { CStr::from_ptr(libc::strerror(errno)) })
                    }
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

        let mut errno = 0;

        // Finalize writing the object
        let res = unsafe { rust_s3_write_object_stream_done(handle, &mut errno) };

        if res == -1 {
            panic!("Failed to finalize writing the object: {:?}",
                   unsafe { CStr::from_ptr(libc::strerror(errno)) });
        }

        // Notify buffer
        let mut buf = [0; 1];

        // Read the one-byte notification into the buffer
        loop {
            let mut errno = 0;

            let count = unsafe {
                rust_s3_read_object_chunk(handle, buf.as_mut_ptr() as *mut c_void, buf.len(),
                    &mut errno)
            };

            if count <= 0 {
                if count < 0 {
                    if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                        sleep(Duration::from_millis(10)).await;
                        continue
                    } else {
                        panic!("Failed to read the notify buffer: {:?}",
                               unsafe { CStr::from_ptr(libc::strerror(errno)) })
                    }
                }
                break
            };

            assert_eq!(buf[0], 1);
        }

        // Close write stream
        let handle = unsafe { rust_s3_close_object_stream(handle) };

        let mut status;
        let mut msg = std::ptr::null_mut();

        // Get write status code
        while { status = unsafe { rust_s3_get_task_status(handle, &mut msg) };
                status == ASYNC_TASK_NOT_READY
        } { sleep(Duration::from_millis(10)).await }

        println!("---\nObject write complete, status: {status}");

        // Print write status message and then free it
        if !msg.is_null() {
            println!("Error while writing object: {:?}", unsafe { CStr::from_ptr(msg) });
            unsafe { libc::free(msg as *mut c_void) };
        }

        println!();

        // Close task
        unsafe { rust_s3_close_task(handle) };

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
                if count < 0 {
                    if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                        sleep(Duration::from_millis(10)).await;
                        continue
                    } else {
                        panic!("Failed to read chunks: {:?}",
                               unsafe { CStr::from_ptr(libc::strerror(errno)) })
                    }
                }
                break
            };

            let contents = &buf[..count as usize];

            r_contents.extend(contents);

            let contents = String::from_utf8_lossy(contents);
            println!(">>> {:2} bytes read | {contents}", count);
        }

        // Close read stream
        let handle = unsafe { rust_s3_close_object_stream(handle) };

        let mut status;
        let mut msg = std::ptr::null_mut();

        // Get read status code
        while { status = unsafe { rust_s3_get_task_status(handle, &mut msg) };
                status == ASYNC_TASK_NOT_READY
        } { sleep(Duration::from_millis(10)).await }

        println!("---\nObject read complete, status: {status}");

        // Print read status message and then free it
        if !msg.is_null() {
            println!("Error while reading object: {:?}", unsafe { CStr::from_ptr(msg) });
            unsafe { libc::free(msg as *mut c_void) };
        }

        println!();

        // Close task
        unsafe { rust_s3_close_task(handle) };

        // Close tokio runtime
        // (skip this because dropping runtimes is not allowed in asynchronous contexts)
        // unsafe { rust_s3_close_tokio_runtime(rt) };

        // Close bucket
        unsafe { rust_s3_close_bucket(bucket) };

        // Test that the written and the read data are equal
        assert_eq!(w_contents, r_contents);

        Ok(())
    }
}

