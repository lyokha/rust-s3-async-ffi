#ifndef RUST_S3_ASYNC_FFI_H
#define RUST_S3_ASYNC_FFI_H

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

    void* c_init_bucket(const BucketDescr* bucket);
    void c_close_bucket(void* bucket);
    void* c_init_tokio_runtime();
    void c_close_tokio_runtime(void* rt_handle);
    StreamHandle* c_init_object_stream(
        void* rt_handle, void* bucket_handle, int write, const char* path);
    void* c_close_stream(void* handle);
    void* c_get_task_status(void* handle, int* status);
#ifdef __cplusplus
}
#endif

#endif  /* RUST_S3_ASYNC_FFI_H */

