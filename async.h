#pragma once

#include <cstddef>

namespace async {
	using handle_t = void*;
	handle_t connect(std::size_t bulk);
	void receive(handle_t handle, const char *data, std::size_t size);
	void receive(handle_t handle, std::string &command);
	void disconnect(handle_t handle);
	handle_t init(int bulk);
};
