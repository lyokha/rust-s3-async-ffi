/* Build: g++ -o s3async-test s3async.cpp \
 *           -lboost_program_options -L../../target/debug -lrusts3asyncffi
 *
 * Run: LD_LIBRARY_PATH=../../target/debug ./s3_async_test -p path
 */

#include <boost/asio/io_context.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/placeholders.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/streambuf.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/system/error_code.hpp>
#include <boost/bind/bind.hpp>
#include <boost/program_options.hpp>
#include <utility>
#include <vector>
#include <string>
#include <chrono>
#include <memory>
#include <exception>
#include <stdexcept>
#include <cassert>
#include <cstring>
#include <cstdlib>
#include <istream>
#include <fstream>
#include <iostream>
#include <iomanip>

#include "../../include/rust_s3_async_ffi.h"


namespace local = boost::asio::local;

using Buffers = std::vector<boost::asio::const_buffer>;
using StreamBuffer = boost::asio::streambuf;

using AsyncS3ReadPtr = std::shared_ptr<class AsyncS3Read>;
using AsyncS3WritePtr = std::shared_ptr<class AsyncS3Write>;


// Prevent improper usage of rust_s3_close_object_stream()
void* close_object_stream(void* stream_handle)
{
    assert(stream_handle != nullptr);
    void* res = rust_s3_close_object_stream(stream_handle);
    stream_handle = nullptr;
    return res;
}


// Prevent improper usage of rust_s3_close_task()
void close_task(void* join_handle)
{
    assert(join_handle != nullptr);
    rust_s3_close_task(join_handle);
    join_handle = nullptr;
}


class AsyncS3Read : public std::enable_shared_from_this<AsyncS3Read>
{
    friend void async_s3_read(
                    boost::asio::io_context& io_context, void* tokio_rt,
                    void* bucket_handle, const std::string& path,
                    AsyncS3WritePtr writer);

    private:
        AsyncS3Read(boost::asio::io_context& io_context, void* tokio_rt,
                    void* bucket_handle, const std::string& path,
                    AsyncS3WritePtr writer = nullptr) :
            io_context_(io_context), tokio_rt_(tokio_rt),
            bucket_handle_(bucket_handle), path_(path), buf_(16),
            stream_handle_(rust_s3_read_object_stream(
                tokio_rt_, bucket_handle, path_.c_str())),
            fd_(io_context_, local::stream_protocol(),
                stream_handle_ == nullptr ? -1 : stream_handle_->fd),
            timer_(io_context_), join_handle_(nullptr), writer_(writer)
        {}

    private:
        void async_read()
        {
            boost::asio::async_read(fd_, buf_,
                boost::bind(&AsyncS3Read::handle_read, shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
        }

    private:
        void handle_read(const boost::system::error_code& error, size_t size)
        {
            bool eof = false;

            if (error)
            {
                if (error != boost::asio::error::eof)
                {
                    std::cerr << "Read error: " << error.message() << std::endl;
                    join_handle_ = close_object_stream(stream_handle_);
                    close_task(join_handle_);
                    return;
                }

                eof = true;
            }

            buf_.commit(size);
            std::istream stream(&buf_);
            char buf[size];
            stream.read(buf, size);
            std::string chunk(buf, size);

            std::cout << ">>> " << std::setw(2) << size << " bytes read | " <<
                    chunk << std::endl;

            if (!eof)
            {
                boost::asio::async_read(fd_, buf_,
                    boost::bind(&AsyncS3Read::handle_read, shared_from_this(),
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
                return;
            }

            join_handle_ = close_object_stream(stream_handle_);

            handle_status(boost::system::error_code());
        }

        void handle_status(const boost::system::error_code& error)
        {
            if (error)
            {
                std::cerr << "Status error: " << error.message() << std::endl;
                close_task(join_handle_);
                return;
            }

            char* errmsg = nullptr;

            int status = rust_s3_get_task_status(join_handle_, &errmsg);

            if (status == RUST_S3_ASYNC_TASK_NOT_READY)
            {
                std::cerr << "Read status is not ready " <<
                        "(supposedly rare case)" << std::endl;

                timer_.expires_after(std::chrono::milliseconds(10));
                timer_.async_wait(
                    boost::bind(&AsyncS3Read::handle_status,
                                shared_from_this(),
                                boost::asio::placeholders::error));
            } else
            {
                std::cout << "---\nObject read complete, status: " <<
                        status << std::endl;
                if (errmsg != nullptr)
                {
                    std::cout << "Error while reading object: " <<
                            errmsg << std::endl;
                    free(errmsg);
                }
                std::cout << std::endl;

                close_task(join_handle_);
            }
        }

    private:
        boost::asio::io_context& io_context_;
        void* tokio_rt_;
        void* bucket_handle_;
        std::string path_;
        StreamBuffer buf_;
        StreamHandle* stream_handle_;
        local::stream_protocol::socket fd_;
        boost::asio::steady_timer timer_;
        void* join_handle_;
        AsyncS3WritePtr writer_;
};


void async_s3_read(boost::asio::io_context& io_context, void* tokio_rt,
                   void* bucket_handle, const std::string& path,
                   AsyncS3WritePtr writer);


class AsyncS3Write : public std::enable_shared_from_this<AsyncS3Write>
{
    friend void async_s3_write(
                     boost::asio::io_context& io_context, void* tokio_rt,
                     void* bucket_handle, const std::string& path,
                     const Buffers&& bufs, bool read_back);

    private:
        AsyncS3Write(boost::asio::io_context& io_context, void* tokio_rt,
                     void* bucket_handle, const std::string& path,
                     const Buffers&& bufs, bool read_back = false) :
            io_context_(io_context), tokio_rt_(tokio_rt),
            bucket_handle_(bucket_handle), path_(path), bufs_(bufs),
            read_back_(read_back), notify_buf_(1),
            stream_handle_(rust_s3_write_object_stream(
                tokio_rt, bucket_handle_, path_.c_str())),
            fd_(io_context_, local::stream_protocol(),
                stream_handle_ == nullptr ? -1 : stream_handle_->fd),
            timer_(io_context_), join_handle_(nullptr)
        {}

    private:
        void async_write()
        {
            boost::asio::async_write(fd_, bufs_[0],
                boost::bind(&AsyncS3Write::handle_write, shared_from_this(),
                            0, boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
        }

    private:
        void handle_write(size_t n, const boost::system::error_code& error,
                          size_t size)
        {
            if (error)
            {
                std::cerr << "Write error: " << error.message() << std::endl;
                join_handle_ = close_object_stream(stream_handle_);
                close_task(join_handle_);
                return;
            }

            std::string chunk(static_cast<const char*>(bufs_[n].data()), size);

            std::cout << ">>> " << std::setw(2) << size <<
                    " bytes written | " << chunk << std::endl;

            if (n++ < bufs_.size() - 1)
            {
                boost::asio::async_write(fd_, bufs_[n],
                    boost::bind(&AsyncS3Write::handle_write, shared_from_this(),
                                n, boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred));
                return;
            }

            int err = 0;

            if (rust_s3_write_object_stream_done(stream_handle_, &err) == -1)
            {
                static const size_t buf_size = 128;
                char buf[buf_size];

                std::cerr << "Failed to finalize writing into the stream: " <<
                        strerror_r(err, buf, buf_size) << std::endl;
                join_handle_ = close_object_stream(stream_handle_);
                close_task(join_handle_);
                return;
            }

            boost::asio::async_read(fd_, notify_buf_,
                boost::bind(&AsyncS3Write::handle_notify, shared_from_this(),
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
        }

        void handle_notify(const boost::system::error_code& error, size_t size)
        {
            if (error)
            {
                std::cerr << "Notify error: " << error.message() << std::endl;
                join_handle_ = close_object_stream(stream_handle_);
                close_task(join_handle_);
                return;
            }

            join_handle_ = close_object_stream(stream_handle_);

            handle_status(boost::system::error_code());
        }

        void handle_status(const boost::system::error_code& error)
        {
            if (error)
            {
                std::cerr << "Status error: " << error.message() << std::endl;
                close_task(join_handle_);
                return;
            }

            char* errmsg = nullptr;

            int status = rust_s3_get_task_status(join_handle_, &errmsg);

            if (status == RUST_S3_ASYNC_TASK_NOT_READY)
            {
                std::cerr << "Write status is not ready " <<
                        "(supposedly rare case)" << std::endl;

                timer_.expires_after(std::chrono::milliseconds(10));
                timer_.async_wait(
                    boost::bind(&AsyncS3Write::handle_status,
                                shared_from_this(),
                                boost::asio::placeholders::error));
            } else
            {
                std::cout << "---\nObject write complete, status: " <<
                        status << std::endl;
                if (errmsg != nullptr)
                {
                    std::cout << "Error while reading object: " <<
                            errmsg << std::endl;
                    free(errmsg);
                }
                std::cout << std::endl;

                close_task(join_handle_);

                if (read_back_)
                {
                    // Wait a moment, otherwise S3 is not so fast and may
                    // return nothing or an older object
                    timer_.expires_after(std::chrono::seconds(1));
                    timer_.async_wait(
                        boost::bind(&AsyncS3Write::read_written_file,
                                    shared_from_this(),
                                    boost::asio::placeholders::error));
                }
            }
        }

        void read_written_file(const boost::system::error_code& error)
        {
            if (error)
            {
                std::cerr << "Timer error: " << error.message() << std::endl;
                return;
            }

            // Note that passing a reference to this is not that important
            // if the reader does not aim to compare against written bytes
            async_s3_read(io_context_, tokio_rt_, bucket_handle_, path_,
                          shared_from_this());
        }

    private:
        boost::asio::io_context& io_context_;
        void* tokio_rt_;
        void* bucket_handle_;
        std::string path_;
        const Buffers bufs_;
        bool read_back_;
        StreamBuffer notify_buf_;
        StreamHandle* stream_handle_;
        local::stream_protocol::socket fd_;
        boost::asio::steady_timer timer_;
        void* join_handle_;
};


void async_s3_read(boost::asio::io_context& io_context, void* tokio_rt,
                   void* bucket_handle, const std::string& path,
                   AsyncS3WritePtr writer = nullptr)
{
    AsyncS3ReadPtr s3_read(new AsyncS3Read(
        io_context, tokio_rt, bucket_handle, path, writer));

    s3_read->async_read();
}


void async_s3_write(boost::asio::io_context& io_context, void* tokio_rt,
                    void* bucket_handle, const std::string& path,
                    const Buffers&& bufs, bool read_back = false)
{
    AsyncS3WritePtr s3_write(new AsyncS3Write(
        io_context, tokio_rt, bucket_handle, path, std::move(bufs), read_back));

    s3_write->async_write();
}


namespace po = boost::program_options;

int main(int argc, char** argv)
{
    std::string config_file;

    po::options_description cmdline_options("Command line options");
    cmdline_options.add_options()
            ("config,c",
             po::value<std::string>(&config_file)->default_value("bucket.ini"),
             "bucket config file")
            ("path,p",
             po::value<std::string>(&config_file)->required(),
             "file path in the bucket")
            ("help,h", "display this help message");

    std::string name, region, endpoint, access_key, secret_key,
            security_token, session_token, expiration;
    float request_timeout;

    po::options_description bucket_options("Bucket options");
    bucket_options.add_options()
            ("name",
             po::value<std::string>(&name)->required(),
             "bucket name")
            ("region",
             po::value<std::string>(&region)->required(),
             "bucket region")
            ("endpoint",
             po::value<std::string>(&endpoint)->default_value(""),
             "bucket endpoint")
            ("access_key",
             po::value<std::string>(&access_key)->default_value(""),
             "bucket access key")
            ("secret_key",
             po::value<std::string>(&secret_key)->default_value(""),
             "bucket secret key")
            ("security_token",
             po::value<std::string>(&security_token)->default_value(""),
             "bucket access key")
            ("session_token",
             po::value<std::string>(&session_token)->default_value(""),
             "bucket session key")
            ("expiration",
             po::value<std::string>(&expiration)->default_value(""),
             "bucket expiration")
            ("request_timeout",
             po::value<float>(&request_timeout)->default_value(30.0),
             "request timeout in seconds");

    try
    {
        po::variables_map vm_cmdline;

        po::store(po::parse_command_line(argc, argv, cmdline_options),
                  vm_cmdline);

        if (vm_cmdline.count("help") || !vm_cmdline.count("path"))
        {
            std::cout << cmdline_options;
            return 0;
        }

        std::ifstream ifs(vm_cmdline.at("config").as<std::string>());

        if (!ifs)
        {
            throw std::runtime_error("Failed to read config file");
        }

        po::variables_map vm_config;

        po::store(po::parse_config_file(ifs, bucket_options), vm_config);
        po::notify(vm_config);

        BucketDescr bucket
        {
            vm_config.at("name").as<std::string>().c_str(),
            vm_config.at("region").as<std::string>().c_str(),
            vm_config.at("endpoint").empty() ? nullptr :
                    vm_config.at("endpoint").as<std::string>().c_str(),
            vm_config.at("access_key").empty() ? nullptr :
                    vm_config.at("access_key").as<std::string>().c_str(),
            vm_config.at("secret_key").empty() ? nullptr :
                    vm_config.at("secret_key").as<std::string>().c_str(),
            vm_config.at("security_token").empty() ? nullptr :
                    vm_config.at("security_token").as<std::string>().c_str(),
            vm_config.at("session_token").empty() ? nullptr :
                    vm_config.at("session_token").as<std::string>().c_str(),
            vm_config.at("expiration").empty() ? nullptr :
                    vm_config.at("expiration").as<std::string>().c_str(),
            vm_config.at("request_timeout").as<float>()
        };

        // Create and own S3 bucket
        void* bucket_handle = rust_s3_init_bucket(&bucket);

        if (bucket_handle == nullptr)
        {
            throw std::runtime_error("Failed to initialize s3 bucket");
        }

        boost::asio::io_context io_context;
        time_t now = time(0);
        const char* stime = ctime(&now);
        size_t stime_len = strlen(stime);

        Buffers bufs =
        {
            boost::asio::const_buffer("Chunk 1\n", 8),
            boost::asio::const_buffer("Chunk 2\n", 8),
            boost::asio::const_buffer(stime, stime_len),
            boost::asio::const_buffer(stime, stime_len)
        };

        // Create and own tokio runtime
        void* rt = rust_s3_init_tokio_runtime();

        if (rt == nullptr)
        {
            throw std::runtime_error("Failed to initialize tokio runtime");
        }

        // Writer will also read the written object back
        async_s3_write(io_context, rt, bucket_handle,
                       vm_cmdline.at("path").as<std::string>(),
                       std::move(bufs), true);

        io_context.run();

        // Close tokio runtime and S3 bucket
        rust_s3_close_tokio_runtime(rt);
        rust_s3_close_bucket(bucket_handle);
    }
    catch (const std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return 0;
}

