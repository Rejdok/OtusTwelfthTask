#include "CommandProcessor.h"

#include <map>
#include <queue>
#include <string>
#include <sstream>
#include <memory>
#include <mutex>

#include "async.h"
namespace async{
	class asyncHandler {
	public:
		asyncHandler() {
			coutMutex = std::make_shared<std::mutex>();
		}

		handle_t registerNewHandler(std::size_t bulk, bool isGeneralProcessor = false) {
			if (isGeneralProcessor&&defaultHandler!=nullptr) {
				return nullptr;
			}
			size_t countOfOutputThreads = 2;
			CommandsProcessor* p= new CommandsProcessor(countOfOutputThreads, bulk, coutMutex,isGeneralProcessor);
			registredHandles.emplace(reinterpret_cast<handle_t>(p),p);
			if (isGeneralProcessor) {
				 defaultHandler = reinterpret_cast<handle_t>(p);
			}
			return reinterpret_cast<handle_t>(p);
		}
		void inputNewCommand(handle_t handle,std::string& rawCommand) {
			bool shoudProcess = false;
			if (registredHandles.find(handle) != registredHandles.end()) {
				if (rawCommand[0] == '{'&&rawCommand.size() == 1){
					dynBlockProcessing[handle] += 1;
					shoudProcess = true;
				}else if (rawCommand[0] == '}'&&rawCommand.size() == 1) {
					dynBlockProcessing[handle] -= 1;
					shoudProcess = true;
				}
				if (dynBlockProcessing[handle] != 0 || shoudProcess) {
					registredHandles[handle]->inputNewCommand(rawCommand);
				} else{
					registredHandles[defaultHandler]->inputNewCommand(rawCommand);
				}

			}
		}
		void disconnectHandler(handle_t handle) {
			if (registredHandles.find(handle) != registredHandles.end()) {
				registredHandles[handle]->diconnect();
				delete registredHandles[handle];
				registredHandles.erase(handle);
			}
		}
		~asyncHandler() {
			for (auto &i : registredHandles) {
				registredHandles[i.second]->diconnect();
				delete registredHandles[i.second];
				registredHandles.erase(i.first);
			}
		}
		void processInput(handle_t handle, const char *data, std::size_t size){
			std::string inputString(data, size);
			inputBuf[handle] += inputString;
			bool allPossibleCommandsProcessed = false;
			while (!allPossibleCommandsProcessed)
			{
				allPossibleCommandsProcessed = true;
				auto endOfCommand = inputBuf[handle].find('\n');
				if (endOfCommand != std::string::npos){
					auto newCommand = std::string(inputBuf[handle], 0, endOfCommand);
					inputNewCommand(handle, newCommand);
					inputBuf[handle].erase(0, endOfCommand+1);
					allPossibleCommandsProcessed = false;
				}
			}
		}
	private:
		size_t maxConnections = maxConnections;
		std::shared_ptr<std::mutex> coutMutex;
		//Возможно стоит сделать отдельной структурой
		std::map<handle_t, CommandsProcessor*> registredHandles;
		std::map<handle_t, std::queue<std::string>> inputMessagesQueue;
		std::map<handle_t, std::string> inputBuf;
		std::map<handle_t, int> dynBlockProcessing;
		//
		handle_t defaultHandler;
		std::thread queueHandle;
	};
	
	asyncHandler OneObjectToRuleThemAll;

	handle_t connect(std::size_t bulk) {
		return OneObjectToRuleThemAll.registerNewHandler(bulk);
	}

	void receive(handle_t handle, const char *data, std::size_t size) {
		OneObjectToRuleThemAll.processInput(handle,data,size);
	}

	void receive(handle_t handle, std::string &command) {
		OneObjectToRuleThemAll.inputNewCommand(handle, command);
	}

	void disconnect(handle_t handle) {
		OneObjectToRuleThemAll.disconnectHandler(handle);
	}

	handle_t init(int bulk)
	{
		return	OneObjectToRuleThemAll.registerNewHandler(bulk, true);
	}

}
