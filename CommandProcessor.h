#pragma once


#include <iostream>
#include <string>
#include <vector>
#include <queue>
#include <ctime>
#include <fstream>
#include <memory>
#include <thread>
#include <queue>
#include <mutex>
#include <chrono>
#include <atomic>
#include <tuple>


#include "async.h"

enum class state { inputStatcSizeBlock, inputDynamicSizeBlock, outputBlock, terminate };

using CommandBlockT = std::vector<std::string>;
using CommandQueueT = std::queue<CommandBlockT>;
using FileMutexT = std::mutex;
using QueueMutexT = std::mutex;

class OFThread {
public:
	OFThread(std::shared_ptr<CommandQueueT> const& commandsQueue, std::shared_ptr<QueueMutexT>& queueMutex,
		std::shared_ptr<FileMutexT>& fileCreateMutex,
		std::shared_ptr<std::atomic_bool>& isDisconnectCalled) :commandsQueue(commandsQueue), queueMutex(queueMutex),
		fileCreateMutex(fileCreateMutex), isDisconnectCalled(isDisconnectCalled) {};
	OFThread(OFThread&& ofthread) = default;
	OFThread(OFThread& ofthread) = default;
	bool fileExist(std::string& fileName) {
		std::ifstream fileExistsCheck;
		fileExistsCheck.open(fileName);
		auto rez = fileExistsCheck.good();
		fileExistsCheck.close();
		return rez;
	}
	void writeBlockToFile(CommandBlockT& commandsBlock) {
		countOfBlocks++;
		bool isOFileCreated = false;
		std::ofstream outputFile;
		size_t countOfTries = 0;
		std::string fileName = std::string("bulk") + commandsBlock[0] + "_";
		while (!isOFileCreated) {
			std::string fName = fileName + std::to_string(countOfTries);
			fileCreateMutex->lock();
			bool isFileExist = fileExist(fName);
			if (!isFileExist) {
				outputFile.open(fileName + std::to_string(countOfTries));
				if (outputFile.is_open()) {
					isOFileCreated = true;
				}
			}
			fileCreateMutex->unlock();
			countOfTries++;
		}
		outputFile << "bulk: ";
		for (size_t i = 1; i < commandsBlock.size(); i++) {
			countOfCommands++;
			outputFile << commandsBlock[i];
			if (i != commandsBlock.size() - 1) {
				outputFile << ", ";
			}
		}
		outputFile << std::endl;
		outputFile.close();
	}

	void outputToFile() {
		bool shouldTerminate = false;
		while (true) {
			bool isLockMutexSucsesseful = queueMutex->try_lock();
			bool isQueueEmpty = false;
			if (isLockMutexSucsesseful)
				isQueueEmpty = commandsQueue->empty();
			if (isLockMutexSucsesseful && !isQueueEmpty) {
				if (commandsQueue->empty() && *isDisconnectCalled) {
					shouldTerminate = true;
				}
				auto commandsBlock = std::move(commandsQueue->front());
				commandsQueue->pop();
				queueMutex->unlock();
				if (commandsBlock.size() > 1) {
					writeBlockToFile(commandsBlock);
				}
			}
			else {
				if (isLockMutexSucsesseful) {
					queueMutex->unlock();
				}
				if (*isDisconnectCalled && isQueueEmpty) {
					shouldTerminate = true;
				}
				else {
					std::this_thread::sleep_for(std::chrono::microseconds(1));
					totalSleepTime += 1;
				}
			}
			if (shouldTerminate) {
				return;
			}
		}
	}
	auto getCountOfBlocksAndCommands() {
		return std::make_tuple(countOfBlocks, countOfCommands, totalSleepTime);
	}
private:
	size_t countOfBlocks = 0;
	size_t countOfCommands = 0;
	size_t totalSleepTime = 0;
	std::shared_ptr<QueueMutexT> queueMutex;
	std::shared_ptr<FileMutexT> fileCreateMutex;
	std::shared_ptr<std::atomic_bool> isDisconnectCalled;
	std::shared_ptr<CommandQueueT> commandsQueue;
};

class CommandsProcessor {
public:
	CommandsProcessor() = default;
	CommandsProcessor(size_t countsOfThreads,size_t countOfCommandsInBlock,std::shared_ptr<std::mutex> coutMutex):
		countOfCommandsInBlock(countOfCommandsInBlock),coutMutex(coutMutex), handle(reinterpret_cast<void*>(this)){
		processCommandsThread = std::thread( &CommandsProcessor::processCommands, this );
		commandsQueue = std::make_shared<CommandQueueT>();
		queueMutex = std::make_shared<QueueMutexT>();
		fileCreateMutex = std::make_shared<FileMutexT>();
		isDisconnectCalled = std::make_shared<std::atomic_bool>();
		*isDisconnectCalled = false;
		workers.reserve(countsOfThreads);
		IOFstreams.reserve(countsOfThreads);
		for (size_t i = 0; i < countsOfThreads; i++) {
			IOFstreams.emplace_back(commandsQueue, queueMutex, fileCreateMutex, isDisconnectCalled);
			workers.emplace_back(&OFThread::outputToFile, &IOFstreams[i]);
		}
	}
	CommandsProcessor(size_t countsOfThreads, size_t countOfCommandsInBlock, std::shared_ptr<std::mutex> coutMutex,bool isGeneralProcessor):
		CommandsProcessor(countsOfThreads,countOfCommandsInBlock, coutMutex)  {
		this->isGeneralProcessor = isGeneralProcessor;
	}
	auto processNewCommand() {
		std::string newCommand;
		bool stringRecived = false;
		while (newCommand.size()==0)
		{
			rawCommandsMutex.lock();
			if (rawCommands.size() > 0) {
				newCommand = std::move(rawCommands.front());
				rawCommands.pop();
				rawCommandsMutex.unlock();
				stringRecived = true;
				break;
			}
			else if (disconnectCalled){
				rawCommandsMutex.unlock();
				break;
			}
			else{
				rawCommandsMutex.unlock();
				std::this_thread::sleep_for(std::chrono::microseconds(1));
			}
		}
		if (disconnectCalled&&rawCommands.empty()) {
			if (currentState == state::inputStatcSizeBlock) {
				stateQueue.push(state::outputBlock);
			}
			stateQueue.push(state::terminate);
		}
		countOfLines++;
		return std::make_tuple(newCommand,stringRecived);
	}

	void inputNewCommand(std::string &command) {
		if (disconnectCalled==false) {
			rawCommandsMutex.lock();
			rawCommands.emplace(command);
			rawCommandsMutex.unlock();
		}
	}

	void inputSaticSizeBlock(CommandBlockT &commandsInBlock) {
		commandsInBlock.reserve(countOfCommandsInBlock);
		for (int i = 0; i < countOfCommandsInBlock; i++) {
			std::string currentCommand;
			bool stringRecived;
			std::tie(currentCommand, stringRecived)= processNewCommand();
			if (i == 0) {
				auto firstCommandTime = std::time(NULL);
				commandsInBlock.emplace_back(std::to_string(firstCommandTime));
			}
			if (currentCommand == startDynamicBlock) {
				stateQueue.push(state::outputBlock);
				stateQueue.push(state::inputDynamicSizeBlock);
				return;
			}
			if (stringRecived)
				commandsInBlock.emplace_back(currentCommand);
			else
				break;
		}
		stateQueue.push(state::outputBlock);
		stateQueue.push(state::inputStatcSizeBlock);
	}

	void inputDynamicSizeBlock(CommandBlockT &commandsInBlock) {
		int endOfDynamicBlockCounter = 1;
		bool firstLineRecived = false;
		while (endOfDynamicBlockCounter)
		{
			std::string currentCommand;
			bool stringRecived;
			std::tie(currentCommand, stringRecived) = processNewCommand();
			if (!firstLineRecived) {
				auto firstCommandTime = std::time(NULL);
				commandsInBlock.emplace_back(std::to_string(firstCommandTime));
				firstLineRecived = true;
			}
			if (currentCommand == startDynamicBlock) {
				endOfDynamicBlockCounter++;
			}
			else if (currentCommand == endDynamicBlock) {
				endOfDynamicBlockCounter--;
			}
			else {
				if (stringRecived)
					commandsInBlock.emplace_back(currentCommand);
				else
					break;
			}
		}
		if (endOfDynamicBlockCounter == 0) {
			stateQueue.push(state::outputBlock);
			stateQueue.push(state::inputStatcSizeBlock);
		}
	}
	void outputBlock(CommandBlockT const &commandsInBlock) {
		queueMutex->lock();
		commandsQueue->push(commandsInBlock);
		queueMutex->unlock();
		outputToConsole(commandsInBlock);
	}

	void outputToConsole(CommandBlockT const &commandsInBlock) {
		if (commandsInBlock.size()>1) {
			coutMutex->lock();
			std::cout << "Current handle = " << handle<<std::endl;
			std::cout << "bulk: ";
			countOfBlocks++;
			for (size_t i = 1; i < commandsInBlock.size(); i++) {
				countOfCommands++;
				std::cout << commandsInBlock[i];
				if (i != commandsInBlock.size() - 1) {
					std::cout << ", ";
				}
			}
			std::cout << std::endl;
			coutMutex->unlock();
		}
	}

	void processCommands() {
		stateQueue.push(state::inputStatcSizeBlock);
		CommandBlockT commandsInBlock;
		while (!stateQueue.empty()) {
			currentState = stateQueue.front();
			stateQueue.pop();
			switch (currentState)
			{
			case state::inputStatcSizeBlock:
				inputSaticSizeBlock(commandsInBlock);
				break;
			case state::inputDynamicSizeBlock:
				inputDynamicSizeBlock(commandsInBlock);
				break;
			case state::outputBlock:
				outputBlock(commandsInBlock);
				commandsInBlock.clear();
				break;
			case state::terminate:
				stateQueue = std::queue<state>{};
				*isDisconnectCalled = true;
				break;
			default:
				break;
			}
		}
		for (auto& i : workers) {
			i.join();
		}
		printMetrics();
	}
	void printMetrics() {
		coutMutex->lock();
		std::cout << "Handle = " <<handle<< std::endl;
		std::cout << "Count of blocks = " << countOfBlocks
			<< " Count of commands = " << countOfCommands << " Count of lines = " << countOfLines - 1 << std::endl;
		size_t threadCounter = 0;
		for (auto &i : IOFstreams) {
			size_t countOfBlocks = 0, countOfCommands = 0, totalSleepTime = 0;
			std::tie(countOfBlocks, countOfCommands, totalSleepTime) = i.getCountOfBlocksAndCommands();
			std::cout << "Thread " + std::to_string(threadCounter) << std::endl;
			std::cout << "Count of blocks = " << countOfBlocks << " Count of commands = " << countOfCommands << " Total sleep time =" << totalSleepTime << std::endl;
			threadCounter++;
		}
		coutMutex->unlock();
	}
	void diconnect() {
		disconnectCalled = true;
		processCommandsThread.join();
	}
	~CommandsProcessor() {

	}
private:
	size_t countOfCommandsInBlock = 0;
	size_t countOfBlocks = 0;
	size_t countOfCommands = 0;
	size_t countOfLines = 0;
	async::handle_t handle;
	std::thread processCommandsThread;
	std::queue<state> stateQueue;
	std::vector<std::thread> workers;
	std::vector<OFThread> IOFstreams;
	std::shared_ptr<QueueMutexT> queueMutex;
	std::shared_ptr<FileMutexT> fileCreateMutex;
	std::shared_ptr<std::atomic_bool> isDisconnectCalled;
	std::shared_ptr<CommandQueueT> commandsQueue;
	std::queue<std::string> rawCommands;
	std::mutex rawCommandsMutex;
	std::shared_ptr<std::mutex> coutMutex;
	bool disconnectCalled = false;
	bool isGeneralProcessor = false;
	state currentState;
	const std::string startDynamicBlock = std::string({ "{" });
	const std::string endDynamicBlock = std::string({ "}" });
};
