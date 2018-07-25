
#include "CommandProcessor.h"
#include "async.h"
#include <map>
#include <set>
#include <memory>
#include <iostream>
#include <boost/asio.hpp>

using boost::asio::ip::tcp;

class AsyncSession:public std::enable_shared_from_this<AsyncSession> {
public:
	AsyncSession(tcp::socket socket)
		: socket_(std::move(socket))
	{
	}
	void start() {
		//todo transmit bulk size
		handle = async::connect(5);
		doReadCommand();
	}
	void doReadCommand() {
		auto self(shared_from_this());
		boost::asio::async_read(socket_,
			boost::asio::buffer(data_, maxLength),
			[this, self](boost::system::error_code ec, std::size_t length)
		{
			if (!ec)
			{
				async::receive(handle,data_, length);
				doReadCommand();
			} else{
				std::cout << ec.message()<<std::endl;
			}
		});
	}
private:
	tcp::socket socket_;
	async::handle_t handle;
	enum { maxLength =1024};
	char data_[maxLength];
};

class AsyncServer {
public:
	AsyncServer(boost::asio::io_service& io_service, tcp::endpoint const& endpoint):
		acceptor_(io_service,endpoint),socket_(io_service) {
		do_accept();
	}
private:
	void do_accept() {
		acceptor_.async_accept(socket_, [this](boost::system::error_code ec) {
			if (!ec) {
				std::make_shared<AsyncSession>(std::move(socket_))->start();
			}
			do_accept();
		});
	}
	tcp::acceptor acceptor_;
	tcp::socket socket_;
};

int main(int argc, char * argv[]) {
	try {
		if (argc < 3) {
			std::cerr << "Usage bulk_server <port> <bulk_size>" << std::endl;
			return 1;
		}
		int port = atoi(argv[1]);
		int bulk = atoi(argv[2]);
		async::init(bulk);

		boost::asio::io_service io_service;
		tcp::endpoint endpoint(tcp::v4(), port);
		AsyncServer serv(io_service, endpoint);
		io_service.run();
	}
	catch (std::exception& e) {
		std::cerr << "Exepition: " << e.what() << std::endl;
	}
	return 0;
}
