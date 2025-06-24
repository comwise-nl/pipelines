"""
title: FlowiseAI Integration
author: Eric Zavesky
author_url: https://github.com/ezavesky
git_url: https://github.com/open-webui/pipelines/
description: Access FlowiseAI endpoints via chat integration
required_open_webui_version: 0.4.3
requirements: requests,flowise>=1.0.4
version: 0.4.3.8
licence: MIT
"""

from typing import List, Union, Generator, Iterator, Dict, Optional
from pydantic import BaseModel, Field
import requests
import os
import json
from datetime import datetime
import time
from flowise import Flowise, PredictionData
from logging import getLogger

logger = getLogger(__name__)
logger.setLevel("DEBUG")

class Pipeline:
    class Valves(BaseModel):
        FLOWISE_API_KEY: str = Field(default="changeme", description="FlowiseAI API key")
        FLOWISE_BASE_URL: str = Field(default="changeme", description="FlowiseAI base URL")
        RATE_LIMIT: int = Field(default=15, description="Rate limit for the pipeline (ops/minute)")
        FLOW_ENABLED: Optional[bool] = Field(default=False, description="Flow Enabled")
        FLOW_ID: Optional[str] = Field(default=None, description="Flow ID")
        FLOW_NAME: Optional[str] = Field(default=None, description="Flow Name")
        DISPLAY_AGENT_FLOW_EVENT: Optional[bool] = Field(default=True, description="Display agentFlowEvent metadata")
        DISPLAY_NEXT_AGENT_FLOW: Optional[bool] = Field(default=True, description="Display nextAgentFlow metadata")
        DISPLAY_AGENT_FLOW_EXECUTED_DATA: Optional[bool] = Field(default=True, description="Display agentFlowExecutedData metadata")
        DISPLAY_CALLED_TOOLS: Optional[bool] = Field(default=True, description="Display usedTools name")
        DISPLAY_CALLED_TOOLS_INPUT: Optional[bool] = Field(default=True, description="Display usedTools input")
        DISPLAY_CALLED_TOOLS_OUTPUT: Optional[bool] = Field(default=True, description="Display usedTools output")
        DISPLAY_USAGE_METADATA: Optional[bool] = Field(default=True, description="Display usageMetadata")
        DISPLAY_AGENT_REASONING: Optional[bool] = Field(default=True, description="Display agentReasoning metadata")
        DISPLAY_METADATA: Optional[bool] = Field(default=True, description="Display general metadata")
        DISPLAY_UPDATE_EVENT: Optional[bool] = Field(default=True, description="Display update event data")
        DISPLAY_START_EVENT: Optional[bool] = Field(default=True, description="Display start event data")
        DISPLAY_END_EVENT: Optional[bool] = Field(default=True, description="Display end event data")
        DISPLAY_OTHER_EVENTS: Optional[bool] = Field(default=True, description="Display other unhandled events")

    def __init__(self):
        self.name = "FlowiseAI Pipeline"
        logger.info(f"Initializing {self.name}")
        self.valves = self.Valves(
            **{k: os.getenv(k, v.default) for k, v in self.Valves.model_fields.items()}
        )
        self.flow_id = None
        self.flow_name = None
        self.api_flow_name = None
        self.update_flow_details()

    def get_flow_details(self, flow_id: str) -> Optional[dict]:
        logger.info(f"Fetching flow details for Flow ID: {flow_id}")
        try:
            api_url = f"{self.valves.FLOWISE_BASE_URL.rstrip('/')}/api/v1/chatflows/{flow_id}"
            headers = {"Authorization": f"Bearer {self.valves.FLOWISE_API_KEY}"}
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to fetch flow details. Status code: {response.status_code}")
        except Exception as e:
            logger.error(f"Exception fetching flow details: {str(e)}")
        return None

    def update_flow_details(self):
        logger.info("Updating flow configuration...")
        self.flow_id = None
        self.flow_name = None
        self.api_flow_name = None

        if self.valves.FLOW_ENABLED and self.valves.FLOW_ID and self.valves.FLOW_NAME:
            flow_details = self.get_flow_details(self.valves.FLOW_ID)
            self.flow_id = self.valves.FLOW_ID
            self.flow_name = self.valves.FLOW_NAME.lower()
            self.api_flow_name = flow_details.get('name', 'Unknown') if flow_details else 'Unknown'
            logger.info(f"Flow configured: {self.flow_name} (API Name: {self.api_flow_name}, ID: {self.flow_id})")
        else:
            logger.warning("FlowiseAI flow is not fully configured or not enabled.")

    async def on_startup(self):
        logger.info(f"Starting up {self.name}...")
        self.update_flow_details()

    async def on_shutdown(self):
        logger.info(f"Shutting down {self.name}...")

    async def on_valves_updated(self) -> None:
        logger.info("Valves updated. Refreshing flow details...")
        self.update_flow_details()

    def rate_check(self, dt_start: datetime) -> bool:
        dt_end = datetime.now()
        time_diff = (dt_end - dt_start).total_seconds()
        time_buffer = (1 / self.valves.RATE_LIMIT)
        if time_diff < time_buffer:
            sleep_time = time_buffer - time_diff
            logger.info(f"Rate limit active. Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            return True
        return False

    def parse_user_input(self, user_message: str) -> str:
        date_now = datetime.now().strftime("%Y-%m-%d")
        time_now = datetime.now().strftime("%H:%M:%S")
        query = f"{user_message.strip()}"
        logger.info(f"Parsed user input: {query}")
        return query

    def safe_get(self, d, *keys):
        """Safely gets a nested value from a dictionary, returning None if any key in the path is missing or not a dictionary."""
        current = d
        for key in keys:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current
    
    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        logger.info(f"Pipeline triggered for message: {user_message}")

        # CRITICAL FIX: The 'body' parameter is sometimes received as a JSON string instead of a dictionary.
        # This block ensures 'body' is always a dictionary before any dictionary methods like .get() are called on it.
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError:
                error_msg = "Error: The 'body' parameter received was an invalid JSON string. It must be a valid JSON object or a dictionary."
                logger.error(error_msg)
                return error_msg

        # Get starting time for total analysis time at the end of an LLM turn
        dt_start = datetime.now()
        streaming = body.get("stream", False)
        session_id = self.chat_id

        system_message = None
        # Attempt to extract system message from body.model.info.params.system
        # This path is used when 'model' is a dictionary containing detailed info.
        system_message = self.safe_get(body, "model", "info", "params", "system")

        # If system_message is not found in the first path, check the 'messages' array.
        # This path is used when the system message is part of the chat history,
        # and 'model' might be a string at the top level.
        if system_message is None and isinstance(body.get("messages"), list):
            for msg in body["messages"]:
                if isinstance(msg, dict) and msg.get("role") == "system" and isinstance(msg.get("content"), str):
                    system_message = msg["content"]
                    break

        logger.debug(f"Extracted system_message: {system_message}")

        if not self.valves.FLOWISE_API_KEY or not self.valves.FLOWISE_BASE_URL:
            error_msg = "Missing FlowiseAI configuration."
            logger.error(error_msg)
            return error_msg if not streaming else iter([error_msg])

        query = self.parse_user_input(user_message)

        if not self.valves.FLOW_ENABLED or not self.flow_id:
            error_msg = "FlowiseAI flow is not configured or enabled."
            logger.warning(error_msg)
            return error_msg if not streaming else iter([error_msg])

        if streaming:
            return self.stream_retrieve(self.flow_id, self.flow_name, query, dt_start, session_id, system_message)
        else:
            return self.static_retrieve(self.flow_id, self.flow_name, query, dt_start, session_id, system_message)

    async def inlet(self, body: dict, user: Optional[dict] = None) -> dict:
        logger.info(f"inlet: {__name__}")
        logger.debug(f"body: {json.dumps(body, indent=2)}")
        logger.debug(f"user: {json.dumps(user, indent=2)}")
        
        self.user_id = user.get("id") if user else None
        self.user_name = user.get("name") if user else None
        self.user_email = user.get("email") if user else None
        
        metadata = body.get("metadata", {})
        self.chat_id = metadata.get("chat_id")
        self.message_id = metadata.get("message_id")

        logger.debug(f"Extracted chat_id: {self.chat_id}")
        logger.debug(f"Extracted message_id: {self.message_id}")

        return body
    
    import json

    def unwrap_json(self, value):
        import json

        def try_parse_json(val):
            if isinstance(val, str):
                val = val.strip()
                if (val.startswith('{') and val.endswith('}')) or (val.startswith('[') and val.endswith(']')):
                    try:
                        return json.loads(val)
                    except json.JSONDecodeError:
                        pass
            return val

        def recurse(val):
            val = try_parse_json(val)

            if isinstance(val, dict):
                return {k: recurse(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [recurse(v) for v in val]
            else:
                return val

        return recurse(value)

    def stream_retrieve(self, flow_id: str, flow_name: str, query: str, dt_start: datetime, session_id: Optional[str], system_message: Optional[str]) -> Generator:
        if not query:
            yield "Query is empty."
            return

        try:
            logger.info(f"Streaming query to FlowiseAI: {query}")
            self.rate_check(dt_start)

            client = Flowise(
                base_url=self.valves.FLOWISE_BASE_URL.rstrip('/'),
                api_key=self.valves.FLOWISE_API_KEY
            )

            prediction_data = PredictionData(chatflowId=flow_id, question=query, streaming=True)
            override_config = {}
            if session_id:
                override_config["sessionId"] = session_id
            # Add the extracted system message to the overrideConfig for FlowiseAI.
            # FlowiseAI chatflows can then access this via `req.body.overrideConfig.systemMessage`.
            # Only include systemMessage if a value is provided, to avoid FlowiseAI errors
            # when an empty string is passed for a variable expected to be populated.
            if system_message is not None:
                if "vars" not in override_config:
                    override_config["vars"] = {}
                override_config["vars"]["systemMessageOpenWebUI"] = system_message
            if override_config:
                prediction_data.overrideConfig = override_config
            logger.debug(f"overrideConfig = {override_config}")

            completion = client.create_prediction(prediction_data)
        except requests.exceptions.RequestException as e:
            error_msg = f"❌ Network error during streaming:\n```\n{str(e)}\n```"
            logger.error(error_msg)
            yield error_msg
            return
        except Exception as e:
            error_msg = f"❌ Exception during streaming:\n```\n{str(e)}\n```"
            logger.error(error_msg)
            yield error_msg
            return

        # Update Open WebUI status line
        yield {
            "event": {
                "type": "status",
                "data": {
                    "description": f"Analysis started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
                    "done": False
                }
            }
        }

        for chunk in completion:
            logger.debug(f"Raw chunk: {chunk}")
            try:
                if isinstance(chunk, str):
                    try:
                        chunk = json.loads(chunk)
                    except json.JSONDecodeError:
                        logger.warning(f"Non-JSON chunk: {chunk}")
                        yield f"⚠️ Invalid JSON chunk:\n```\n{chunk}\n```"
                        continue

                # process normal chunk chunk
                if "error" in chunk:
                    yield f"🚨 Error during streaming: {chunk['error']}"
                    continue

                event = chunk.get("event")
                data = chunk.get("data")

                if event == "token":
                    yield data

                elif event == "start":
                    if self.valves.DISPLAY_START_EVENT:
                        if isinstance(data, str):
                            yield f"_Analysis started... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}:_\n{data}\n"
                        elif isinstance(data, dict) or isinstance(data, list):
                            yield f"__Start Data__:\n```json\n{json.dumps(data, indent=2)}\n```"
                        else:
                            yield f"[Start] Unexpected data format: {str(data)}\n"

                elif event == "update":
                    if self.valves.DISPLAY_UPDATE_EVENT:
                        yield f"[Update] {json.dumps(data)}\n"

                elif event == "agentReasoning":
                    yield {
                        "event": {
                            "type": "status",
                            "data": {
                                "description": f"Thinking...\n",
                                "done": False
                            }
                        }
                    }
                    if self.valves.DISPLAY_AGENT_REASONING:
                        if isinstance(data, list):
                            for step in data:
                                yield f"[Reasoning Step] {json.dumps(step, indent=2)}\n"
                        else:
                            yield f"[Reasoning] {json.dumps(data, indent=2)}\n"

                elif event == "metadata":
                    if self.valves.DISPLAY_METADATA:
                        yield f"[Metadata] {json.dumps(data, indent=2)}\n"

                elif "error" in chunk:
                    yield f"Error from FlowiseAI: {chunk['error']}"

                # Handle specific "Other Event" types based on the user's example
                elif event == "agentFlowEvent":
                    if self.valves.DISPLAY_AGENT_FLOW_EVENT:
                        yield f"[Other Event: {event}] {json.dumps(data)}\n"
                elif event == "nextAgentFlow":
                    if self.valves.DISPLAY_NEXT_AGENT_FLOW:
                        yield f"[Other Event: {event}] {json.dumps(data)}\n"
                elif event == "agentFlowExecutedData":
                    if self.valves.DISPLAY_AGENT_FLOW_EXECUTED_DATA:
                        yield f"[Other Event: {event}] {json.dumps(data)}\n"
                elif event == "usedTools":
                    yield {
                        "event": {
                            "type": "status",
                            "data": {
                                "description": f"Tool calling...\n",
                                "done": False
                            }
                        }
                    }
                    if self.valves.DISPLAY_CALLED_TOOLS:
                        formatted_tools = []
                        if isinstance(data, list):
                            for tool_call in data:
                                tool_name = tool_call.get("tool", "Unknown Tool")
                                tool_input = tool_call.get("toolInput", {})
                                tool_output = tool_call.get("toolOutput", {})
                                logger.debug(f"toolOutput: {json.dumps(tool_output, indent=2)}")

                                # Attempt to parse toolInput if it's a JSON string                                
                                if isinstance(tool_input, str):
                                    try:
                                        tool_input = json.loads(tool_input)
                                    except json.JSONDecodeError:
                                        pass # Keep as string if not valid JSON

                                # Attempt to parse toolOutput if it's a JSON string
                                # if isinstance(tool_output, str):
                                #     try:
                                #         tool_output = json.loads(tool_output)
                                #     except json.JSONDecodeError:
                                #         pass # Keep as string if not valid JSON
                                tool_output = self.unwrap_json(tool_output)

                                formatted_tools.append(f"  - {tool_name}\n")
                                if self.valves.DISPLAY_CALLED_TOOLS_INPUT:
                                    formatted_tools.append(f"```json\nInput:\n\n{json.dumps(tool_input, indent=2)}\n```\n")
                                if self.valves.DISPLAY_CALLED_TOOLS_OUTPUT:
                                    formatted_tools.append(f"```json\nOutput:\n\n{json.dumps(tool_output, indent=2)}\n```")
                            yield f"\n\n__Called Tools__:\n" + "\n".join(formatted_tools) + "\n"
                        else:
                            yield f"\n\n__Called Tools__:\n```json\n{json.dumps(data, indent=2)}\n```\n"
                elif event == "usageMetadata":
                    if self.valves.DISPLAY_USAGE_METADATA:
                        yield f"[Other Event: {event}] {json.dumps(data)}\n"
                elif event == "end":
                    if self.valves.DISPLAY_END_EVENT:
                        yield f"Analysis complete... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                elif event == "agent_trace":
                    agent_step = data.get("step")
                    if agent_step == "agent_action":
                        # Step 1: Parse the "action" field, which is a JSON string
                        action_str = data.get("action")
                        action_dict = json.loads(action_str)

                        # Step 2: Get the tool name
                        tool_name = action_dict.get("tool", "Unknown tool")
                        yield {
                            "event": {
                                "type": "status",
                                "data": {
                                    "description": f"Tool calling {tool_name}...\n",
                                    "done": False
                                }
                            }
                        }
                else:
                    if self.valves.DISPLAY_OTHER_EVENTS:
                        yield f"[Other Event: {event}] {json.dumps(data)}\n"

            except Exception as e:
                logger.exception("Error processing stream chunk")
                yield f"\nError handling chunk: {str(e)}"

        dt_end = datetime.now()
        total_analysis_time = (dt_end - dt_start).total_seconds()
        # Update status line
        yield {
            "event": {
                "type": "status",
                "data": {
                    "description": f"Analysis completed at {dt_end.strftime('%Y-%m-%d %H:%M:%S')}, total analysis time: {total_analysis_time:.2f} seconds\n",
                    "done": True
                }
            }
        }

    def static_retrieve(self, flow_id: str, flow_name: str, query: str, dt_start: datetime, session_id: Optional[str], system_message: Optional[str]) -> Generator:
        if not query:
            yield "Query is empty."
            return

        api_url = f"{self.valves.FLOWISE_BASE_URL.rstrip('/')}/api/v1/prediction/{flow_id}"
        headers = {"Authorization": f"Bearer {self.valves.FLOWISE_API_KEY}"}
        payload = {"question": query}
        override_config = {}
        if session_id:
            override_config["sessionId"] = session_id
        # Add the extracted system message to the overrideConfig for FlowiseAI.
        # FlowiseAI chatflows can then access this via `req.body.overrideConfig.systemMessage`.
        # Only include systemMessage if a value is provided, to avoid FlowiseAI errors
        # when an empty string is passed for a variable expected to be populated.
        if system_message is not None:
            if "vars" not in override_config:
                override_config["vars"] = {}
            override_config["vars"]["systemMessageOpenWebUI"] = system_message
        if override_config:
            payload["overrideConfig"] = override_config

        try:
            logger.info(f"Sending static query to FlowiseAI: {query}")
            self.rate_check(dt_start)
            response = requests.post(api_url, headers=headers, json=payload)

            if response.status_code != 200:
                yield f"Error from FlowiseAI: Status {response.status_code}"
                return

            result = response.json()
            logger.info("Received static response from FlowiseAI")

            if isinstance(result, dict):
                for key in ["text", "answer", "response", "result"]:
                    if key in result:
                        yield result[key]
                        return
                yield f"```json\n{json.dumps(result, indent=2)}\n```"
            elif isinstance(result, str):
                yield result
            else:
                yield f"```json\n{json.dumps(result, indent=2)}\n```"
        except Exception as e:
            yield f"Error calling FlowiseAI: {str(e)}"
