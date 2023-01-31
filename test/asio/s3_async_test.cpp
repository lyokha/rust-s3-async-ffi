/* Build: g++ -o s3async-test s3async.cpp \
 *           -lboost_program_options -L../../target/debug -lrusts3asyncffi
 *
 * Run: LD_LIBRARY_PATH=../../target/debug ./s3async-test -p path
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
#include <istream>
#include <fstream>
#include <iostream>
#include <iomanip>

#include "../../include/rust_s3_async_ffi.h"


namespace local = boost::asio::local;

using Buffers = std::vector<boost::asio::const_buffer>;
using StreamBuffer = boost::asio::streambuf;


class AsyncS3Read : public std::enable_shared_from_this<AsyncS3Read>
{
    public:
        AsyncS3Read(boost::asio::io_context& io_context, void* tokio_rt,
                   void* bucket_handle, const std::string& path) :
            io_context_(io_context), tokio_rt_(tokio_rt),
            bucket_handle_(bucket_handle), path_(path), buf_(16),
            handle_(c_init_object_stream(tokio_rt_, bucket_handle, 0,
                                         path_.c_str())),
            fd_(io_context_, local::stream_protocol(),
                handle_ == nullptr ? -1 : handle_->fd),
            timer_(io_context_), join_handle_(nullptr)
        {}

    public:
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
                    std::cout << "Read error: " << error.message() << std::endl;
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

            join_handle_ = c_close_stream(handle_);

            int status = 0;
            join_handle_ = c_get_task_status(join_handle_, &status);

            if (join_handle_ == nullptr)
            {
                std::cout << "---\nObject read complete, status: " <<
                        status << std::endl << std::endl;
            } else
            {
                timer_.expires_after(std::chrono::milliseconds(10));
                timer_.async_wait(
                    boost::bind(&AsyncS3Read::handle_status,
                                shared_from_this(),
                                boost::asio::placeholders::error));
            }
        }

        void handle_status(const boost::system::error_code& error)
        {
            if (error)
            {
                std::cout << "Status error: " << error.message() << std::endl;
                return;
            }

            int status = 0;
            join_handle_ = c_get_task_status(join_handle_, &status);

            if (join_handle_ == nullptr)
            {
                std::cout << "---\nObject read complete, status: " <<
                        status << std::endl << std::endl;
            } else
            {
                timer_.expires_after(std::chrono::milliseconds(10));
                timer_.async_wait(
                    boost::bind(&AsyncS3Read::handle_status,
                                shared_from_this(),
                                boost::asio::placeholders::error));
            }
        }

    private:
        boost::asio::io_context& io_context_;
        void* tokio_rt_;
        void* bucket_handle_;
        std::string path_;
        StreamBuffer buf_;
        StreamHandle* handle_;
        local::stream_protocol::socket fd_;
        boost::asio::steady_timer timer_;
        void* join_handle_;
};


class AsyncS3Write : public std::enable_shared_from_this<AsyncS3Write>
{
    public:
        AsyncS3Write(boost::asio::io_context& io_context, void* tokio_rt,
                     void* bucket_handle, const std::string& path,
                     const Buffers&& bufs, bool read_back = false) :
            io_context_(io_context), tokio_rt_(tokio_rt),
            bucket_handle_(bucket_handle), path_(path), bufs_(bufs),
            read_back_(read_back),
            handle_(c_init_object_stream(tokio_rt, bucket_handle_, 1,
                                         path_.c_str())),
            fd_(io_context_, local::stream_protocol(),
                handle_ == nullptr ? -1 : handle_->fd),
            timer_(io_context_), join_handle_(nullptr)
        {}

    public:
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
                std::cout << "Write error: " << error.message() << std::endl;
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

            join_handle_ = c_close_stream(handle_);

            int status = 0;
            join_handle_ = c_get_task_status(join_handle_, &status);

            if (join_handle_ == nullptr)
            {
                std::cout << "---\nObject write complete, status: " <<
                        status << std::endl << std::endl;
            } else
            {
                timer_.expires_after(std::chrono::milliseconds(10));
                timer_.async_wait(
                    boost::bind(&AsyncS3Write::handle_status,
                                shared_from_this(),
                                boost::asio::placeholders::error));
            }
        }

        void handle_status(const boost::system::error_code& error)
        {
            if (error)
            {
                std::cout << "Status error: " << error.message() << std::endl;
                return;
            }

            int status = 0;
            join_handle_ = c_get_task_status(join_handle_, &status);

            if (join_handle_ == nullptr)
            {
                std::cout << "---\nObject write complete, status: " <<
                        status << std::endl << std::endl;

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
            } else
            {
                timer_.expires_after(std::chrono::milliseconds(10));
                timer_.async_wait(
                    boost::bind(&AsyncS3Write::handle_status,
                                shared_from_this(),
                                boost::asio::placeholders::error));
            }
        }

        void read_written_file(const boost::system::error_code& error)
        {
            if (error)
            {
                std::cout << "Timer error: " << error.message() << std::endl;
                return;
            }

            std::shared_ptr<AsyncS3Read> s3_read =
                std::make_shared<AsyncS3Read>(io_context_, tokio_rt_,
                                              bucket_handle_, path_);

            s3_read->async_read();
        }

    private:
        boost::asio::io_context& io_context_;
        void* tokio_rt_;
        void* bucket_handle_;
        std::string path_;
        const Buffers bufs_;
        bool read_back_;
        StreamHandle* handle_;
        local::stream_protocol::socket fd_;
        boost::asio::steady_timer timer_;
        void* join_handle_;
};


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

    std::string name, region, access_key, secret_key,
            security_token, session_token, expiration;

    po::options_description bucket_options("Bucket options");
    bucket_options.add_options()
            ("name",
              po::value<std::string>(&name)->required(),
              "bucket name")
            ("region",
             po::value<std::string>(&region)->required(),
             "bucket region")
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
             "bucket expiration");

    try
    {
        po::variables_map vm_cmdline;

        po::store(po::parse_command_line(argc, argv, cmdline_options),
                  vm_cmdline);

        if(vm_cmdline.count("help") || !vm_cmdline.count("path"))
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

        store(parse_config_file(ifs, bucket_options), vm_config);
        notify(vm_config);

        BucketDescr bucket
        {
            vm_config.at("name").as<std::string>().c_str(),
            vm_config.at("region").as<std::string>().c_str(),
            vm_config.at("access_key").empty() ? nullptr :
                    vm_config.at("access_key").as<std::string>().c_str(),
            vm_config.at("secret_key").empty() ? nullptr :
                    vm_config.at("secret_key").as<std::string>().c_str(),
            vm_config.at("security_token").empty() ? nullptr :
                    vm_config.at("security_token").as<std::string>().c_str(),
            vm_config.at("session_token").empty() ? nullptr :
                    vm_config.at("session_token").as<std::string>().c_str(),
            vm_config.at("expiration").empty() ? nullptr :
                    vm_config.at("expiration").as<std::string>().c_str()
        };

        void* bucket_handle = c_init_bucket(&bucket);

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
            boost::asio::const_buffer( "Chunk 1\n", 8 ),
            boost::asio::const_buffer( "Chunk 2\n", 8 ),
            boost::asio::const_buffer( stime, stime_len ),
            boost::asio::const_buffer( stime, stime_len )
        };

        void* rt = c_init_tokio_runtime();

        if (rt == nullptr)
        {
            throw std::runtime_error("Failed to initialize tokio runtime");
        }

        std::shared_ptr<AsyncS3Write> s3_write =
                std::make_shared<AsyncS3Write>(io_context, rt, bucket_handle,
                        vm_cmdline.at("path").as<std::string>(),
                        std::move(bufs), true);

        s3_write->async_write();

        io_context.run();

        c_close_tokio_runtime(rt);
        c_close_bucket(bucket_handle);
    }
    catch (std::exception& e)
    {
        std::cerr << "Exception: " << e.what() << std::endl;
    }

    return 0;
}
