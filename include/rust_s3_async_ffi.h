#ifndef RUST_S3_ASYNC_FFI_H
#define RUST_S3_ASYNC_FFI_H

#include <sys/types.h>

#define RUST_S3_ASYNC_TASK_ERROR (-1)
#define RUST_S3_ASYNC_TASK_NOT_READY (-2)


#ifdef __cplusplus
extern "C"
{
#endif

    struct BucketDescr
    {
        const char* name;
        const char* region;
        const char* access_key;
        const char* secret_key;
        const char* security_token;
        const char* session_token;
        const char* expiration;
    };

    struct StreamHandle
    {
        void* rt_handle;
        void* join_handle;
        void* client;
        int   fd;
    };

    void* rust_s3_init_bucket(const BucketDescr* bucket);
    void rust_s3_close_bucket(void* bucket_handle);
    void* rust_s3_init_tokio_runtime();
    void rust_s3_close_tokio_runtime(void* rt_handle);
    StreamHandle* rust_s3_write_object_stream(
        void* rt_handle, void* bucket_handle, const char* path);
    StreamHandle* rust_s3_read_object_stream(
        void* rt_handle, void* bucket_handle, const char* path);
    void* rust_s3_close_object_stream(void* stream_handle);
    ssize_t rust_s3_write_object_chunk(
        void* stream_handle, void* chunk, size_t size, int* errno);
    ssize_t rust_s3_read_object_chunk(
        void* stream_handle, void* chunk, size_t size, int* errno);
    int rust_s3_get_task_status(void* join_handle);
    void rust_s3_close_task(void* join_handle);

#ifdef __cplusplus
}
#endif

#endif  /* RUST_S3_ASYNC_FFI_H */

