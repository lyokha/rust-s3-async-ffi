rust-s3-async-ffi
=================

Asynchronous streaming of AWS S3 objects in C and C++ by means of crate
[rust-s3](https://crates.io/crates/rust-s3) and Unix domain sockets.

Building the project with

```ShellSession
$ cargo build
```

produces a C dynamic library *librusts3asyncffi.so* which can be linked against
code written in C or C++. Internally, *librusts3asyncffi.so* spawns asynchronous
tasks running functions *put_object_stream()* and *get_object_stream()* from
crate *rust-s3*. The tasks are driven by associated pairs of connected Unix
sockets. The client side of a pair is supposed for passing to the client side
of an application as a raw file descriptor.

Tests
-----

There are 2 approaches to test this.

### 1. Rust unit test

This approach makes use of an embedded Rust unit test emulating a C program
flow. To test it in that way, the contents of files *test/data/bucket.toml* and
*test/data/path.toml* must be properly configured. In the original configuration
they do not expose any secrets.

###### File *bucket.toml*

```toml
name = "my-bucket"
region = "eu-central-1"

# access_key = "secret"
# secret_key = "secret"

# request_timeout = 30.0
```

Fields *access_key*, *secret_key*, *security_token*, *session_token*, and
*expiration*, despite the fact that they are optional, must build proper
credentials.

###### File *path.toml*

```toml
value = "/path/to/my/object.data"
```

After configuring the files, run the test.

```ShellSession
$ cargo test -- --nocapture
    Finished test [unoptimized + debuginfo] target(s) in 0.14s
     Running unittests src/lib.rs (target/debug/deps/rusts3asyncffi-39e82c84cfd7cfcf)

running 1 test
>>>  8 bytes written | Chunk 1

>>>  8 bytes written | Chunk 2

>>> 33 bytes written | 2023-01-31 16:27:41.981058052 UTC
>>> 33 bytes written | 2023-01-31 16:27:41.981058052 UTC
---
Object write complete, status: 200

>>> 16 bytes read | Chunk 1
Chunk 2

>>> 16 bytes read | 2023-01-31 16:27
>>> 16 bytes read | :41.981058052 UT
>>> 16 bytes read | C2023-01-31 16:2
>>> 16 bytes read | 7:41.981058052 U
>>>  2 bytes read | TC
---
Object read complete, status: 200

test tests::write_and_read_chunked ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.84s
```

### 2. Sample C++ program

The other approach is a decent use case which involves building a C++ program
using [boost::asio](https://www.boost.org/doc/libs/release/libs/asio/) with
writing and reading S3 objects driven by *librusts3asyncffi.so*.

```ShellSession
$ cd test/asio
$ make
g++ -g -Wall -o s3_async_test s3_async_test.cpp -lboost_program_options -L../../target/debug -lrusts3asyncffi
```

Now that the program is built, its configuration file *bucket.ini* must be
properly configured. Originally, it looks similar to *bucket.toml*.

###### File *bucket.ini*

```ini
name=my-bucket
region=eu-central-1

# access_key=secret
# secret_key=secret

# request_timeout=30.0
```

Like in *bucket.toml*, fields *access_key*, *secret_key*, *security_token*,
*session_token*, and *expiration* must build proper credentials.

The object's path gets passed into the program in a command-line argument.

```ShellSession
$ LD_LIBRARY_PATH=../../target/debug ./s3_async_test -p /path/to/my/object.data
>>>  8 bytes written | Chunk 1

>>>  8 bytes written | Chunk 2

>>> 25 bytes written | Tue Jan 31 20:03:07 2023

>>> 25 bytes written | Tue Jan 31 20:03:07 2023

---
Object write complete, status: 200

>>> 16 bytes read | Chunk 1
Chunk 2

>>> 16 bytes read | Tue Jan 31 20:03
>>> 16 bytes read | :07 2023
Tue Jan
>>> 16 bytes read |  31 20:03:07 202
>>>  2 bytes read | 3

---
Object read complete, status: 200
```

