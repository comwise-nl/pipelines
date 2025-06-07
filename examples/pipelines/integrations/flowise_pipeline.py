"""
title: FlowiseAI Integration
author: Eric Zavesky
author_url: https://github.com/ezavesky
git_url: https://github.com/open-webui/pipelines/
description: Access FlowiseAI endpoints via chat integration
required_open_webui_version: 0.4.3
requirements: requests,flowise>=1.0.4
version: 0.4.3.1
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
        FLOWISE_API_KEY: str = Field(default="", description="FlowiseAI API key")
        FLOWISE_BASE_URL: str = Field(default="", description="FlowiseAI base URL")
        RATE_LIMIT: int = Field(default=5, description="Rate limit for the pipeline (ops/minute)")
        FLOW_ENABLED: Optional[bool] = Field(default=False, description="Flow Enabled")
        FLOW_ID: Optional[str] = Field(default=None, description="Flow ID")
        FLOW_NAME: Optional[str] = Field(default=None, description="Flow Name")
        DISPLAY_AGENT_FLOW_EVENT: Optional[bool] = Field(default=True, description="Display agentFlowEvent metadata")
        DISPLAY_NEXT_AGENT_FLOW: Optional[bool] = Field(default=True, description="Display nextAgentFlow metadata")
        DISPLAY_AGENT_FLOW_EXECUTED_DATA: Optional[bool] = Field(default=True, description="Display agentFlowExecutedData metadata")
        DISPLAY_CALLED_TOOLS: Optional[bool] = Field(default=True, description="Display calledTools metadata")
        DISPLAY_USAGE_METADATA: Optional[bool] = Field(default=True, description="Display usageMetadata")
        DISPLAY_AGENT_REASONING: Optional[bool] = Field(default=True, description="Display agentReasoning metadata")
        DISPLAY_METADATA: Optional[bool] = Field(default=True, description="Display general metadata")
        DISPLAY_START_EVENT: Optional[bool] = Field(default=True, description="Display start event data")
        DISPLAY_UPDATE_EVENT: Optional[bool] = Field(default=True, description="Display update event data")
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
        query = f"{user_message.strip()}; today's date is {date_now} and the current time is {time_now}"
        logger.info(f"Parsed user input: {query}")
        return query

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        logger.info(f"Pipeline triggered for message: {user_message}")
        dt_start = datetime.now()
        streaming = body.get("stream", False)

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
            return self.stream_retrieve(self.flow_id, self.flow_name, query, dt_start)
        else:
            return self.static_retrieve(self.flow_id, self.flow_name, query, dt_start)

    def stream_retrieve(self, flow_id: str, flow_name: str, query: str, dt_start: datetime) -> Generator:
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

            completion = client.create_prediction(
                PredictionData(chatflowId=flow_id, question=query, streaming=True)
            )
        except Exception as e:
            error_msg = f"Exception during streaming: {str(e)}"
            logger.error(error_msg)
            yield error_msg
            return

        yield f"Analysis started... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

        for chunk in completion:
            logger.debug(f"Raw chunk: {chunk}")
            try:
                if isinstance(chunk, str):
                    try:
                        chunk = json.loads(chunk)
                    except json.JSONDecodeError:
                        logger.warning(f"Non-JSON chunk: {chunk}")
                        yield chunk
                        continue

                event = chunk.get("event")
                data = chunk.get("data")

                if event == "token":
                    yield data

                elif event == "start":
                    if self.valves.DISPLAY_START_EVENT:
                        if isinstance(data, str):
                            yield f"\n__Start__:\n{data}"
                        elif isinstance(data, dict) or isinstance(data, list):
                            yield f"\n__Start Data__:\n```json\n{json.dumps(data, indent=2)}\n```"
                        else:
                            yield f"\n[Start] Unexpected data format: {str(data)}"

                elif event == "update":
                    if self.valves.DISPLAY_UPDATE_EVENT:
                        yield f"\n[Update] {json.dumps(data)}"

                elif event == "agentReasoning":
                    if self.valves.DISPLAY_AGENT_REASONING:
                        if isinstance(data, list):
                            for step in data:
                                yield f"\n[Reasoning Step] {json.dumps(step, indent=2)}"
                        else:
                            yield f"\n[Reasoning] {json.dumps(data, indent=2)}"

                elif event == "metadata":
                    if self.valves.DISPLAY_METADATA:
                        yield f"\n[Metadata] {json.dumps(data, indent=2)}"

                elif event == "end":
                    if self.valves.DISPLAY_END_EVENT:
                        yield "\nAnalysis complete... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

                elif "error" in chunk:
                    yield f"Error from FlowiseAI: {chunk['error']}"

                # Handle specific "Other Event" types based on the user's example
                elif event == "agentFlowEvent":
                    if self.valves.DISPLAY_AGENT_FLOW_EVENT:
                        yield f"\n[Other Event: {event}] {json.dumps(data)}"
                elif event == "nextAgentFlow":
                    if self.valves.DISPLAY_NEXT_AGENT_FLOW:
                        yield f"\n[Other Event: {event}] {json.dumps(data)}"
                elif event == "agentFlowExecutedData":
                    if self.valves.DISPLAY_AGENT_FLOW_EXECUTED_DATA:
                        yield f"\n[Other Event: {event}] {json.dumps(data)}"
                elif event == "calledTools":
                    if self.valves.DISPLAY_CALLED_TOOLS:
                        yield f"\n[Other Event: {event}] {json.dumps(data)}"
                elif event == "usageMetadata":
                    if self.valves.DISPLAY_USAGE_METADATA:
                        yield f"\n[Other Event: {event}] {json.dumps(data)}"
                else:
                    if self.valves.DISPLAY_OTHER_EVENTS:
                        yield f"\n[Other Event: {event}] {json.dumps(data)}"

            except Exception as e:
                logger.exception("Error processing stream chunk")
                yield f"\nError handling chunk: {str(e)}"

    def static_retrieve(self, flow_id: str, flow_name: str, query: str, dt_start: datetime) -> Generator:
        if not query:
            yield "Query is empty."
            return

        api_url = f"{self.valves.FLOWISE_BASE_URL.rstrip('/')}/api/v1/prediction/{flow_id}"
        headers = {"Authorization": f"Bearer {self.valves.FLOWISE_API_KEY}"}
        payload = {"question": query}

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
