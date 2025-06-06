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
import re
import json
from datetime import datetime
import time
from flowise import Flowise, PredictionData

from logging import getLogger
logger = getLogger(__name__)
logger.setLevel("DEBUG")


class Pipeline:
    class Valves(BaseModel):
        FLOWISE_API_KEY: str = Field(default="", description="FlowiseAI API key (from Bearer key, e.g. QMknVTFTB40Pk23n6KIVRgdB7va2o-Xlx73zEfpeOu0)")
        FLOWISE_BASE_URL: str = Field(default="", description="FlowiseAI base URL (e.g. http://localhost:3000 (URL before '/api/v1/prediction'))")
        RATE_LIMIT: int = Field(default=5, description="Rate limit for the pipeline (ops/minute)")
 
        FLOW_ENABLED: Optional[bool] = Field(default=False, description="Flow Enabled (make this flow available for use)")
        FLOW_ID: Optional[str] = Field(default=None, description="Flow ID (the flow GUID, e.g. b06d97f5-da14-4d29-81bd-8533261b6c88)")
        FLOW_NAME: Optional[str] = Field(default=None, description="Flow Name (human-readable flow name, no special characters, e.g. news or stock-reader)")

    def __init__(self):
        self.name = "FlowiseAI Pipeline"

        # Initialize valve parameters from environment variables
        self.valves = self.Valves(
            **{k: os.getenv(k, v.default) for k, v in self.Valves.model_fields.items()}
        )
        
        # Build flow mapping for faster lookup
        self.flow_id = None
        self.flow_name = None
        self.api_flow_name = None
        self.update_flow_details()

    def get_flow_details(self, flow_id: str) -> Optional[dict]:
        """
        Fetch flow details from the FlowiseAI API
        
        Args:
            flow_id (str): The ID of the flow to fetch
            
        Returns:
            Optional[dict]: Flow details if successful, None if failed
        """
        try:
            api_url = f"{self.valves.FLOWISE_BASE_URL.rstrip('/')}/api/v1/chatflows/{flow_id}"
            headers = {"Authorization": f"Bearer {self.valves.FLOWISE_API_KEY}"}
            
            response = requests.get(api_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                logger.error(f"Error fetching flow details: Status {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching flow details: {str(e)}")
            return None

    def update_flow_details(self):
        """Update the single flow details based on the current valve settings"""
        self.flow_id = None
        self.flow_name = None
        self.api_flow_name = None

        if self.valves.FLOW_ENABLED and self.valves.FLOW_ID and self.valves.FLOW_NAME:
            flow_details = self.get_flow_details(self.valves.FLOW_ID)
            self.flow_id = self.valves.FLOW_ID
            self.flow_name = self.valves.FLOW_NAME.lower()
            self.api_flow_name = flow_details.get('name', 'Unknown') if flow_details else 'Unknown'
            logger.info(f"Configured Flow: {self.flow_name} (API Name: {self.api_flow_name}, ID: {self.flow_id})")
        else:
            logger.info("No FlowiseAI flow configured or enabled.")

    async def on_startup(self):
        """Called when the server is started"""
        logger.debug(f"on_startup:{self.name}")
        self.update_flow_details()

    async def on_shutdown(self):
        """Called when the server is stopped"""
        logger.debug(f"on_shutdown:{self.name}")

    async def on_valves_updated(self) -> None:
        """Called when valves are updated"""
        logger.debug(f"on_valves_updated:{self.name}")
        self.update_flow_details()

    def rate_check(self, dt_start: datetime) -> bool:
        """
        Check time, sleep if not enough time has passed for rate
        
        Args:
            dt_start (datetime): Start time of the operation
        Returns:
            bool: True if sleep was done
        """
        dt_end = datetime.now()
        time_diff = (dt_end - dt_start).total_seconds()
        time_buffer = (1 / self.valves.RATE_LIMIT)
        if time_diff >= time_buffer:  # no need to sleep
            return False
        time.sleep(time_buffer - time_diff)
        return True

    def parse_user_input(self, user_message: str) -> str:
        """
        Prepare the user message as the query for the single flow.
        
        Args:
            user_message (str): User's input message
            
        Returns:
            str: The prepared query
        """
        date_now = datetime.now().strftime("%Y-%m-%d")
        time_now = datetime.now().strftime("%H:%M:%S")
        query = f"{user_message.strip()}; today's date is {date_now} and the current time is {time_now}"
        
        return query

    def pipe(
        self, 
        user_message: str, 
        model_id: str, 
        messages: List[dict], 
        body: dict
    ) -> Union[str, Generator, Iterator]:
        """
        Main pipeline function. Calls the configured FlowiseAI flow with the provided query.
        """
        logger.debug(f"pipe:{self.name}")
        
        dt_start = datetime.now()
        streaming = body.get("stream", False)
        logger.warning(f"Stream: {streaming}")
        context = ""
        
        # Check if we have valid API configuration
        if not self.valves.FLOWISE_API_KEY or not self.valves.FLOWISE_BASE_URL:
            error_msg = "FlowiseAI configuration missing. Please set FLOWISE_API_KEY and FLOWISE_BASE_URL valves."
            if streaming:
                yield error_msg
            else:
                return error_msg
        
        # Prepare the query
        query = self.parse_user_input(user_message)
        
        # Check if the single flow is configured and enabled
        if not self.valves.FLOW_ENABLED or not self.flow_id:
            error_msg = "FlowiseAI flow is not configured or enabled. Please set FLOW_ENABLED, FLOW_ID, and FLOW_NAME valves."
            if streaming:
                yield error_msg
            else:
                return error_msg
        
        if streaming:
            yield from self.stream_retrieve(self.flow_id, self.flow_name, query, dt_start)
        else:
            for chunk in self.static_retrieve(self.flow_id, self.flow_name, query, dt_start):
                context += chunk
            return context if context else "No response from FlowiseAI"

    def stream_retrieve(
            self, flow_id: str, flow_name: str, query: str, dt_start: datetime
        ) -> Generator:
        """
        Stream responses from FlowiseAI using the official client library.
        
        Args:
            flow_id (str): The ID of the flow to call
            flow_name (str): The name of the flow (for logging)
            query (str): The user's query
            dt_start (datetime): Start time for rate limiting
            
        Returns:
            Generator: Response chunks for streaming
        """
        if not query:
            yield "Query is empty. Please provide a question or prompt for the flow."
            return

        try:
            logger.info(f"Streaming from FlowiseAI flow '{flow_name}' with query: {query}")
            
            # Rate limiting check
            self.rate_check(dt_start)
            
            # Initialize Flowise client with API configuration
            client = Flowise(
                base_url=self.valves.FLOWISE_BASE_URL.rstrip('/'),
                api_key=self.valves.FLOWISE_API_KEY
            )
            
            # Create streaming prediction request
            completion = client.create_prediction(
                PredictionData(
                    chatflowId=flow_id,
                    question=query,
                    streaming=True
                )
            )

        except Exception as e:
            error_msg = f"Error streaming from FlowiseAI: {str(e)}"
            logger.error(error_msg)
            yield error_msg
            
        idx_last_update = 0
        yield f"Analysis started... {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

        # Process each streamed chunk
        for chunk in completion:
            try:
                if isinstance(chunk, str):
                    chunk = json.loads(chunk)
            except Exception as e:
                # If chunk is not a string, it's already a dictionary
                pass

            try:
                if isinstance(chunk, dict):
                    # Expected format: {event: "token", data: "content"}
                    if "event" in chunk:
                        if ((chunk["event"] in ["start", "update", "agentReasoning"]) and 
                                ("data" in chunk) and (isinstance(chunk["data"], list))):
                            for data_update in chunk["data"][idx_last_update:]:
                                # e.g. {"event":"start","data":[{"agentName":"Perspective Explorer","messages":["...
                                idx_last_update += 1
                                yield "\n---\n"
                                yield f"\n__Reasoning: {data_update['agentName']} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})__\n\n"
                                for message in data_update["messages"]:
                                    yield message  # yield message for each agent update
                        elif chunk["event"] == "end":
                            # {"event":"end","data":"[DONE]"}
                            yield "\n---\n"
                            yield f"\nAnalysis complete. ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n\n"
                        elif chunk["event"] == "token":
                            # This is the actual output content
                            if "data" in chunk:
                                yield chunk["data"]
                    elif "error" in chunk:
                        error_msg = f"Error from FlowiseAI: {chunk['error']}"
                        logger.error(error_msg)
                        yield error_msg
                else:
                    # If chunk format is unexpected, yield as is
                    yield str(chunk)
            except Exception as e:
                logger.error(f"Error processing chunk: {str(e)}")
                yield f"\nUnusual Response Chunk: ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})\n{str(e)}\n"
                yield f"\n---\n"
                yield str(chunk)
                    
        return

    def static_retrieve(
            self, flow_id: str, flow_name: str, query: str, dt_start: datetime
        ) -> Generator:
        """
        Call the FlowiseAI endpoint with the specified flow ID and query using REST API.
        
        Args:
            flow_id (str): The ID of the flow to call
            flow_name (str): The name of the flow (for logging)
            query (str): The user's query
            dt_start (datetime): Start time for rate limiting
            
        Returns:
            Generator: Response chunks for non-streaming requests
        """
        if not query:
            yield "Query is empty. Please provide a question or prompt for the flow."
            return
            
        api_url = f"{self.valves.FLOWISE_BASE_URL.rstrip('/')}/api/v1/prediction/{flow_id}"
        headers = {"Authorization": f"Bearer {self.valves.FLOWISE_API_KEY}"}
        
        payload = {
            "question": query,
        }
        
        try:
            logger.info(f"Calling FlowiseAI flow '{flow_name}' with query: {query}")
            
            # Rate limiting check
            self.rate_check(dt_start)
            
            response = requests.post(api_url, headers=headers, json=payload)
            
            if response.status_code != 200:
                error_msg = f"Error from FlowiseAI: Status {response.status_code}"
                logger.error(f"{error_msg} - {response.text}")
                yield error_msg
                return
                
            try:
                result = response.json()
                
                # Format might vary based on flow configuration
                # Try common response formats
                if isinstance(result, dict):
                    if "text" in result:
                        yield result["text"]
                    elif "answer" in result:
                        yield result["answer"]
                    elif "response" in result:
                        yield result["response"]
                    elif "result" in result:
                        yield result["result"]
                    else:
                        # If no standard field found, return full JSON as string
                        yield f"```json\n{json.dumps(result, indent=2)}\n```"
                elif isinstance(result, str):
                    yield result
                else:
                    yield f"```json\n{json.dumps(result, indent=2)}\n```"
                    
            except json.JSONDecodeError:
                # If not JSON, return the raw text
                yield response.text
                
        except Exception as e:
            error_msg = f"Error calling FlowiseAI: {str(e)}"
            logger.error(error_msg)
            yield error_msg
            
        return
